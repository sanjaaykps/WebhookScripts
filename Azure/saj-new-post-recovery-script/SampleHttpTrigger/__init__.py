import logging
import requests
import time

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
                        subscriptionstringfind = scaleset_id.split("/")
                        subscription_id = subscriptionstringfind[2]
                        rg_name = subscriptionstringfind[4]
                        compute_vmss_client = ComputeManagementClient(token_credential, subscription_id)
                        time.sleep(60)
                        list_of_scaleset = compute_vmss_client.virtual_machine_scale_sets.list(rg_name)
                        logging.info(f"compute vms client = {compute_vmss_client}")
                        logging.info(f"subscription_id = {subscription_id}")
                        logging.info(f"rg_name = {rg_name}")
                        logging.info(f"list of scaleset VMs = {list_of_scaleset}")
                        logging.info(f"Scaleset ID = {scaleset_id}")
                        logging.info(f"list of scaleset Count  = {len(list(list_of_scaleset))}")
                        logging.info(f"list of list of scaleset VMs= {list(list_of_scaleset)}")
                        logging.info(f"type of list of scaleset VMs= {type(list_of_scaleset)}")
                        logging.info("-----------------------------------------------------------")
                        for each_scaleset in list_of_scaleset:
                            logging.info(f"each scaleset = {each_scaleset}")
                            logging.info(f"scaleset_id = {scaleset_id}")
                            if each_scaleset.id == scaleset_id:
                                each_scaleset.sku = Sku(name =each_scaleset.sku.name, capacity = scaleset_capacity)
        
                                poller = compute_vmss_client.virtual_machine_scale_sets.begin_create_or_update(rg_name,\
                                    each_scaleset.name, each_scaleset)                                
                                logging.info(poller.status())
                        logging.info("===========================================================")

        return func.HttpResponse(
             "This HTTP triggered function executed successfully.",
             status_code=200
        )
    except Exception as e:
        return func.HttpResponse(e)

