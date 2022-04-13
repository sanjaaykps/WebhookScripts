import json
import boto3
import logging

from urllib.request import urlopen
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
message = "Something went wrong !!!"

ec2 = boto3.client('ec2')

def poweroff(instanceid):
    logger.info("calling Poweron Instnace")
    try:
        response = ec2.start_instances(InstanceIds=[instanceid], DryRun=False)
        logger.info(response)
    except ClientError as e:
        logger.error(e)


def lambda_handler(event, context):
    try:
        logger.info(f"EVENT =   ============ {str(event)}")        
        data = json.dumps(event).replace('null', '"user"')
        logger.info(data)
        data_dictionary = eval(data)
        url = data_dictionary["resourceMapping"]["primaryResourceMetadataPath"]
        response = urlopen(url)
        primary_resource_mapping_json = json.loads(response.read())
        logger.info(f"primary_resource_mapping_json =  {primary_resource_mapping_json}")
        for primary_resource_map in primary_resource_mapping_json:
            logger.info(f"source_recovery_map.keys() = {primary_resource_map.keys()}")
            if "COMPUTE" in primary_resource_map.keys():
                compute_details_list = list(primary_resource_map.values())
                for compute_details in compute_details_list:
                    for compute in compute_details:
                        instance_id = compute["cloudResourceReferenceId"]
                        region = compute["region"]
                        logger.info(f"Instance ID = {instance_id} and Region is {region}")
                        poweroff(instance_id)                      

    except Exception as e:
        logger.error(f"FAILED with Exception {e}")
        return {
            'statusCode': 505,
            'body': {"Status": "Failed", "Message": "Failed" }
        }

