import json, os
import logging
import urllib.request
from azure.identity import DefaultAzureCredential
from azure.mgmt.rdbms import postgresql
from azure.mgmt.rdbms.postgresql import PostgreSQLManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.rdbms.postgresql.models import ServerUpdateParameters
import traceback
import azure.functions as func

# This script will extract the payload from appranix recovery webhook and promotes the Postgres single server replica as master

credential = DefaultAzureCredential()
src_info = {}
resource_mapping = {}
recovery_region = ''
subscription_id = ''
recovery_prefix = ''


def get_resource_group_info(body, resource_mapper):
    global subscription_id, src_info, recovery_prefix
    destination_info = {}
    resource_id = ''
    src_info = {}
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
    if resource_id is not None:
        subscription_id = resource_id[2]
    else:
        logging.error("Subscription ID not found..")
    logging.info(src_info)

    # Destination values
    recovery_prefix = body['recoveryName'] + '-'
    return recovery_prefix, destination_info, src_info, subscription_id


def get_source_and_recovery_resource_mapping(body):
    global resource_mapping
    source_recovery_resource_mapping = body['resourceMapping']['sourceRecoveryMappingPath']
    with urllib.request.urlopen(source_recovery_resource_mapping) as url:
        data = json.loads(url.read().decode())
    resource_mapping = list(data)
    return resource_mapping


def get_master_postgres_single_server(resource_group, resource_client):
    global server_name, postgres_servers, master_servers
    # Get the list of resources in the resource group
    resources = resource_client.resources.list_by_resource_group(resource_group)
    # Filter resources to get only PostgresSQL single servers
    postgres_servers = [
        r for r in resources if r.type == 'Microsoft.DBforPostgreSQL/servers'
    ]
    master_servers = []
    for s in postgres_servers:
        logging.info(s.name)
        if is_master(resource_group, s.name):
            master_servers.append(s.name)
    return master_servers


def is_master(resource_group, server_name):
    server = postgresql_client.servers.get(resource_group, server_name)
    # Check if the server is already a master
    if server.replication_role == 'Master':
        logging.info('Server ' + server_name + ' is a master\n')
        return True
    return False
        

def find_recovery_region_replica(replicas, recovery_region):
    for replica in replicas:
        replica_resource = resource_client.resources.get_by_id(replica.id,
                                                               api_version='2017-12-01')  # api_version='2017-12-01'
        replica_region = replica_resource.location
        if replica_region == recovery_region:
            logging.info(f"Replica name: {replica.name}, Region: {replica_region}")
            return replica.name
        else:
            return None

def get_replicas(resource_group, server_name, recovery_region):
    global replicas, replica_to_promote
    # Get the properties of the server to be promoted
    server = postgresql_client.servers.get(resource_group, server_name)
    # Check if the server is already a master
    if server.replication_role == 'Master':
        logging.info('Server ' + server_name + ' is a master\n')
    logging.info("getting replicas of " + server_name)
    # Get the list of replicas for the server
    replicas = list(postgresql_client.replicas.list_by_server(resource_group, server_name))
    # Check if there are any replicas
    if not replicas:
        logging.info('No replicas found for ' + str(server_name))
        return None
        
    replica_to_promote = find_recovery_region_replica(replicas, recovery_region)
    if replica_to_promote is not None:
        return replica_to_promote
    else:
        return None


def promote_replica_to_master(replica_to_promote, resource_group, client):
    # Get the properties of the server to be promoted
    server = postgresql_client.servers.get(resource_group, replica_to_promote)
    # Check if the server is already a master
    if server.replication_role == 'Master':
        logging.info('Server ' + replica_to_promote + ' already a master')
    else:
        logging.info("Promoting " + replica_to_promote + " to Master")
        server_update_params = ServerUpdateParameters(replication_role='None')
        poller = client.servers.begin_update(resource_group_name=resource_group, server_name=replica_to_promote,
                                             parameters=server_update_params)  # On average of 5m
        # wait for the operation to complete
        # result = poller.result()  # On average of 5m
        # logging.info("Replica " + replica_to_promote + " has been promoted to Master...")
        status = poller.status()
        logging.info("Master Promotion for the replica " + replica_to_promote + " is " + str(status))
        return func.HttpResponse(
            "Master promotion for the replica " + replica_to_promote + " has been triggered.....",
            status_code=200
        )


app = func.FunctionApp()

@app.function_name(name="app-deployer")
@app.route(route="promote", methods=['POST'])  # HTTP Trigger
def main(req: func.HttpRequest) -> func.HttpResponse:
    global src_info, resource_mapping, recovery_region, subscription_id, resource_client, client, postgresql_client, server_name
    try:
        credentials = DefaultAzureCredential()
        logging.info("Function app processed a request.....")

        if req and req.get_body():
            body = json.loads(req.get_body())
            logging.info(body)
            if "resourceMapping" in body.keys():
                resource_mapping = get_source_and_recovery_resource_mapping(body)
            else:
                logging.error("Webhook payload body does not have resourceMapping key")
            recovery_prefix, destination_info, src_info, subscription_id = get_resource_group_info(body,
                                                                                                   resource_mapping)
            
            client = PostgreSQLManagementClient(credentials, subscription_id)
            resource_client = ResourceManagementClient(credentials, subscription_id)
            postgresql_client = postgresql.PostgreSQLManagementClient(credentials, subscription_id)

        for resource_group_name in src_info.keys():
            logging.info("Fetching the master servers in the resource group " + resource_group_name)
            recovery_region = destination_info[resource_group_name]['region']
            logging.info("Recovery region : " + str(recovery_region))
            master_servers = get_master_postgres_single_server(resource_group_name, resource_client)
            
            logging.info("Available master servers in the resource group : " )
            logging.info(master_servers)
            if master_servers is not None:
                for server in master_servers:
                    logging.info("Fetching replicas of server " + server)
                    replica_to_promote = get_replicas(resource_group_name, server, recovery_region)
                    if replica_to_promote is not None:
                        promote_replica_to_master(replica_to_promote, resource_group_name, client)
                    else:
                        logging.info("No replicas found in the resource group to promote..........")
                        return func.HttpResponse(
                            "No replicas found in the resource group to promote..........",
                            status_code=500
                        )
            else:
                logging.info("No master servers found in the resource group " )
                return func.HttpResponse(
                        "No master servers found in the resource group.........",
                        status_code=200
                    )
        return func.HttpResponse(status_code=200)
    except Exception as e:

        traceback.print_exc()

        logging.error("Exception occurred while promoting replica: " + str(e))

        return func.HttpResponse(
            'Exception occurred while promoting replica: ' + str(e),
            status_code=500
        )

