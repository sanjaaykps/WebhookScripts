import json
import boto3
import base64
from urllib.request import urlopen
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
message = "Something went wrong !!!"

resouce_list = ["COMPUTE"]  #[COMPUTE, APPLICATION_LOADBALANCER, CLASSIC_LOADBALANCER] 

client = boto3.client('route53')

class DnsUpdateException(Exception):

    def __init__(self, message="Some thing went wrong!!!"):
        self.message = message
        logger.error(message)
        super().__init__(self.message)


def updateRecordSetwithNewValue(hosted_id, record_name, record_type, new_ip):
    response = client.change_resource_record_sets(
        HostedZoneId=hosted_id,
        ChangeBatch={
            "Comment": "DNS Updated Programatically",
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": record_name,
                        "Type": record_type,
                        "TTL": 180,
                        "ResourceRecords": [
                            {
                                "Value": new_ip
                            },
                        ],
                    }
                },
            ]
        }
    )
    logger.info(response)
    

def find_and_replace_a_records(findsourceip, replacetargetip):
    try:
        list_all_hostedzone_response = client.list_hosted_zones()
    except Exception as e:
        logger.error(f"Failed to get the list of all hosted zones {e}")
        raise e
        
    if list_all_hostedzone_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        hosted_zones = list_all_hostedzone_response['HostedZones']
        if len(hosted_zones) == 0:
            message = "There are no Hosted Zones available"
            raise DnsUpdateException(message)                           
        for zone in hosted_zones:
            logger.debug(f"Hosted Zone Detail = {zone}")
            #List all the record set in the Hostedzone
            resource_record_sets = client.list_resource_record_sets(HostedZoneId=zone['Id'])
            if len(resource_record_sets) == 0:
                message = "There are no resource record sets found"
                raise DnsUpdateException(message)  
            for recordset in resource_record_sets['ResourceRecordSets']:
                if recordset['Type'] == 'A':
                    logger.debug("Found a A Record set")
                    for resourcerecord in recordset['ResourceRecords']:
                        if resourcerecord['Value'] == findsourceip:
                            logger.debug(recordset)
                            updateRecordSetwithNewValue(zone['Id'], recordset['Name'], 'A', replacetargetip)
    else:
        message = "Got an upexpcted response when trying to list hostedzone \
            repsonse" + str(list_all_hostedzone_response['ResponseMetadata']['HTTPStatusCode'])
        raise DnsUpdateException(message)                           


def update_a_record(listofdictofips):
    for each_item in listofdictofips:
        source_ip = each_item['sourceip']
        target_ip = each_item['targetip']
        logger.info(f"Source IP  = {source_ip} and Target IP = {target_ip}")
        find_and_replace_a_records(source_ip, target_ip)
    

def lambda_handler(event, context):
    try:
        data = base64.b64decode(event["body-json"]).decode('utf-8').replace('null', '"user"')
        logger.info(data)
        data_dictionary = eval(data)
        process_dict = []
        if data_dictionary['recoveryStatus'] == "RECOVERY_COMPLETED":
            url = data_dictionary["resourceMapping"]["sourceRecoveryMappingPath"]
            response = urlopen(url)
            source_recovery_resource_mapping_json = json.loads(response.read())
            for source_recovery_map in source_recovery_resource_mapping_json:
                for resource_type in resouce_list:
                    if resource_type in source_recovery_map.keys():
                        logger.debug(f"Resource Type {resource_type}")
                        dat_values = list(source_recovery_map.values())
                        data_dict = dat_values[0][0]
                        for k, v in data_dict.items():
                            sourceip = v['source']['publicIpAddress']
                            destip = v['destination']['publicIpAddress']
                            ip_dict = {"sourceip" : sourceip, "targetip" : destip}
                            process_dict.append(ip_dict)
                            logger.info(process_dict)
                        
            if len(process_dict) != 0:
                update_a_record(process_dict)
                return {
                    'statusCode': 200,
                    'body': {"Status": "Success"}
                }
            else:
                logger.error(f"Failed as there are no source and target ip map found")
                return {
                    'statusCode': 505,
                    'body': {"Status": "Failed"}
                }
        else:
            message = "The Recovery did not complete successfully so skipping DNS Update"
            raise DnsUpdateException(message)
    except Exception as e:
            logger.error(f"FAILED with Exception {e}")
            return {
                'statusCode': 505,
                'body': {"Status": "Failed", "Message": "Failed" }
            }
