import json, os
import logging
import urllib.request
import azure.functions as func
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentProperties
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.mgmt.resource.resources.models import Deployment
from azure.mgmt.web import WebSiteManagementClient
from azure.mgmt.web.models import AppServicePlan
from azure.identity import DefaultAzureCredential
import traceback

# This script will extract the payload from appranix recovery webhook and deploys as Function App and Web App to the
# destination resource group.

resource_ids = []
rg_template = {}
src_info = {}
resource_mapping = {}
recovery_arm = {}
src_rg_name = ''
recovery_region = ''
recovery_rg = ''
recovery_prefix = ''
server_farm_resource_type = ""

app = func.FunctionApp()

@app.function_name(name="func-app-deployer")
@app.route(route="deploy", methods= ['POST']) # HTTP Trigger
def main(req: func.HttpRequest) -> func.HttpResponse:
    global src_info, resource_mapping, recovery_region, subscription_id, recovery_prefix, recovery_rg, resource_ids
    try:
        credential = DefaultAzureCredential()
        destination_info = {}
        logging.info("Function app processed a request.....")

        if req and req.get_body():
            body = json.loads(req.get_body())
            logging.info(body)

            # Get source_recovery_resource_mapping file url to download.
            # TODO: Validate  body['resourceMapping']['sourceRecoveryMappingPath'] if keys exist , else throw error
            if "resourceMapping" in body.keys():
                resource_mapping = get_source_and_recovery_resource_mapping(body)
            else:
                logging.error("Webhook payload body does not have resourceMapping key")

            # Get resource groups and regions info from the resource mapping
            # TODO: get_resource_group_info()
            recovery_prefix, destination_info, src_info, subscription_id = get_resource_group_info(body, resource_mapping)
        logging.info("Resource group info")
        logging.info(destination_info)
        logging.info(src_info)
        logging.info('Calling resource Id block.....')

        resource_client = ResourceManagementClient(credential, subscription_id)

        for source_resource_group_name in src_info.keys():
            logging.info(source_resource_group_name)
            # Get Web App/Function from the source resource group
            get_resource_ids(source_resource_group_name, resource_client, "Microsoft.Web/sites")

            resource_group_template = export_template(resource_client, source_resource_group_name)
            modify_arm(resource_group_template, recovery_prefix, destination_info[source_resource_group_name]['region'], destination_info[source_resource_group_name]['groupIdentifier'], resource_client)
        logging.info("DEPLOYMENT SUCCESSFUL")
        return func.HttpResponse(
            status_code=200
        )
    except Exception as e:

        traceback.print_exc()

        logging.error("Exception occurred while deploying apps: " + str(e))

        return func.HttpResponse(
            'Exception occurred while deploying apps:' + str(e),
            status_code=500
        )


def export_template(resource_client, source_resource_group_name):
    BODY = {
        'resources': resource_ids,
        'options': 'IncludeParameterDefaultValue'
    }
    export_request = resource_client.resource_groups.begin_export_template(
        str(source_resource_group_name),
        BODY
    )
    logging.info('Exporting template for the resources.....')
    template = export_request.result()
    resource_group_template = template.template
    logging.info("Manipulating the exported ARM template....")
    logging.info(resource_group_template)
    return resource_group_template


def get_resource_group_info(body, resource_mapper):
    global subscription_id, src_info, recovery_prefix
    destination_info = {}
    resource_id = ''
    src_info = {}
    # TODO improve logic here.
    for mapping in resource_mapper:
        for key in mapping.keys():
            if key == 'VIRTUAL_MACHINE':
                for vm in mapping['VIRTUAL_MACHINE']:
                    for vm_key in vm.keys():
                        resource_id = (str(vm_key).split('/'))
                        destination_info[vm[vm_key]['source']['groupIdentifier']] = vm[vm_key]['destination']
                        src_info[vm[vm_key]['source']['groupIdentifier']] = vm[vm_key]['source']['region']
    # Source values
    logging.info('Fetching source region details......')
    # TODO validate if value exist.
    if resource_id is not None:
        subscription_id = resource_id[2]
    else:
        logging.error("Subscription ID not found..")
    logging.info(src_info)

    # Destination values
    recovery_prefix = body['recoveryName'] + '-'
    logging.info('Fetching destination region details......')

    return recovery_prefix, destination_info, src_info, subscription_id


def get_source_and_recovery_resource_mapping(body):
    global resource_mapping
    source_recovery_resource_mapping = body['resourceMapping']['sourceRecoveryMappingPath']
    with urllib.request.urlopen(source_recovery_resource_mapping) as url:
        data = json.loads(url.read().decode())
    resource_mapping = list(data)
    return resource_mapping


def get_resource_ids(source_resource_group_name, resource_client, resource_type):
    logging.info("fetching function app resource Ids.....")
    result = []
    azure_resources = resource_client.resources.list_by_resource_group(source_resource_group_name,
                                                                       filter="resourceType eq '{0}'".format(
                                                                           resource_type))
    # TODO : make this as separate method.
    get_service_plan_ids(azure_resources, resource_client, result, source_resource_group_name)
    for resource in result:
        resource_ids.append("{0}".format(resource.id))
    logging.info("resource Id generated.....")
    logging.info(resource_ids)


def get_service_plan_ids(azure_resources, resource_client, result, source_resource_group_name):
    service_app = "Microsoft.Web/serverfarms"
    result.extend(list(azure_resources))
    logging.info("fetching ASP Ids.....")
    result.extend(list(resource_client.resources.list_by_resource_group(source_resource_group_name,
                                                                        filter="resourceType eq '{0}'".format(
                                                                            service_app))))

def modify_arm(resource_group_template, recovery_prefix, recovery_region, recovery_rg, resource_client):
    global recovery_params
    template = resource_group_template
    resources = template['resources']

    exclude_resources_types_from_template(resources, template)
    exclude_params_type_from_template(template)

    update_params(recovery_prefix, template)

    update_recovery_location(recovery_region, template)

    recovery_arm = template
    logging.info("Generated ARM template to deploy resources in recovery region........")
    logging.info(recovery_arm)

    parameter_dict = template['parameters']
    result_dict = dict()
    for key, value in parameter_dict.items():
        result_dict[key] = {"value": value["defaultValue"]}
    recovery_params = result_dict
    logging.info("Created the parameter block to deploy resources.....")
    logging.info(recovery_params)
    deploy_resources(recovery_arm, recovery_params, recovery_rg, resource_client)


def update_recovery_location(recovery_region, template):
    key = "location"
    for item in template['resources']:
        if key in item.keys():
            item["location"] = recovery_region


def update_params(recovery_prefix, template):
    logging.info("Removed backup properties in parameter blocks...")
    for parameterValue in template['parameters'].keys():
        if not str(parameterValue).endswith('id'):
            for keys in template['parameters'][parameterValue].keys():
                if "value" in str(keys).lower():
                    template['parameters'][parameterValue][keys] = (recovery_prefix +
                                                                    template['parameters'][parameterValue][keys])[:55]


def exclude_params_type_from_template(template):
    parameters = template['parameters'].copy()
    logging.info("Removed Snapshots/backup properties in resource block...")
    for parameter in parameters.keys():
        if not parameter.startswith('sites') and not parameter.startswith('serverfarms'):
            template['parameters'].pop(parameter, None)

# In case of slot resources to be included, add "Microsoft.Web/sites/slots" in the required_types and add slot plan in get_service_plan_ids method
def exclude_resources_types_from_template(resources, template):
    exclude_types = ["Microsoft.Web/sites/snapshots", "Microsoft.Web/sites/backups",
                     "Microsoft.Web/sites/slots/snapshots", "Microsoft.Web/sites/functions"]
    required_types = ["Microsoft.Web/serverfarms", "Microsoft.Web/sites"]
    for resource in resources.copy():
        if resource['type'] in exclude_types:
            resources.remove(resource)
        elif resource['type'] in required_types:
            if "hostNameSslStates" in resource['properties'].keys():
                del resource['properties']['hostNameSslStates']
            if "virtualNetworkSubnetId" in resource['properties'].keys():
                del resource['properties']['virtualNetworkSubnetId']
                resource['dependsOn'] = [item for item in resource['dependsOn'] if
                                         'Microsoft.Network/virtualNetworks/subnets' not in item]
        else:
            resources.remove(resource)
    template['resources'] = resources


def deploy_resources(recovery_arm, recovery_params, recovery_rg, resource_client):
    logging.info("Loading parameters for deploying resources.......")
    logging.info("Deployment resource grp " + recovery_rg)

    template_file_path = recovery_arm
    parameter_file_path = recovery_params
    # resource_client = ResourceManagementClient(credential, subscription_id)
    deployment_properties = DeploymentProperties(mode=DeploymentMode.incremental, template=template_file_path,
                                                 parameters=parameter_file_path)
    logging.info("Deploying resources.......")
    deployment_async_operation = resource_client.deployments.begin_create_or_update(
        recovery_rg,
        "functionappdeployment",
        Deployment(properties=deployment_properties)
    )
    # Wait for deployment to complete
    logging.info("Waiting for deployment.........")
    deployment_async_operation.wait()
    logging.info("Resource deployment successful in " + recovery_rg)
