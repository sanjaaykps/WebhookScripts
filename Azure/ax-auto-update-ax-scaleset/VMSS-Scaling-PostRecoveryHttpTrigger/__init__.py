import logging
import requests


import azure.functions as func

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import Sku


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    logging.debug(req.get_json())
    logging.debug("______________________________________________________")
    # Acquire a credential object
    token_credential = DefaultAzureCredential()
    logging.info(token_credential)

    try:
        resource_mapping = req.get_json().get('resourceMapping')
        logging.debug(f"RESOURCE MAPPING = f{resource_mapping}")
        recovered_metadata_path_url = resource_mapping['recoveredMetadataPath']
        logging.debug(f"recovered_metadata_path_url = f{recovered_metadata_path_url}")
        recovered_metadata = requests.get(url = recovered_metadata_path_url)
        logging.debug(f"recovered_metadata = {recovered_metadata}")
        recovered_metadata_json = recovered_metadata.json()
        logging.info(f"recovered_metadata_json = {recovered_metadata_json}")

        for metadata in recovered_metadata_json:
            for k, v in metadata.items():
                if k == 'recoveredScalesetInitalCapacityMap':
                    for scaleset_id, scaleset_capacity in v.items():
                        logging.info("---------------------------------------------")
                        subscriptionstringfind = scaleset_id.split("/")
                        subscription_id = subscriptionstringfind[2]
                        rg_name = subscriptionstringfind[4]
                        scaleset_name = subscriptionstringfind[-1]
                        logging.info(f"scaleset_id = {scaleset_id}")
                        logging.info(f"scaleset_name = {scaleset_name}")
                        logging.info(f"resource group = {rg_name}")
                        logging.info(f"Instance Capacity = {scaleset_capacity}")

                        compute_vmss_client = ComputeManagementClient(token_credential, subscription_id)
                        logging.info(f"VMSS Client = {compute_vmss_client}")

                        scalesetdetails = compute_vmss_client.virtual_machine_scale_sets.get(rg_name, scaleset_name)
                        scalesetdetails.sku = Sku(name =scalesetdetails.sku.name, capacity = scaleset_capacity)
                        poller = compute_vmss_client.virtual_machine_scale_sets.begin_create_or_update(rg_name,\
                            scalesetdetails.name, scalesetdetails)
                        logging.info(poller.status())
                        logging.info("===========================================================")

        return func.HttpResponse(
             "This HTTP triggered function executed successfully.",
             status_code=200
        )
    except Exception as e:
        return func.HttpResponse(e.message())

