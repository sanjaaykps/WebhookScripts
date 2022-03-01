import json
import boto3
import base64
from urllib.request import urlopen
import logging

logger = logging.getLogger()
#TODO Logging Level to be picked from the environment variable
logger.setLevel(logging.DEBUG)
message = "Something went wrong !!!"

#Appranix Resource Types that are supported in this script
resouce_list = ["COMPUTE", "APPLICATION_LOAD_BALANCER", "CLASSIC_LOAD_BALANCER"]  #[COMPUTE, APPLICATION_LOADBALANCER, CLASSIC_LOADBALANCER] 
#Appranix Resource Property that are supported in this script to be replaced with that of equivalend recovered resource
resource_properties_list = ["publicIpAddress", "privateIpAddress", "privateDnsName", "publicDnsName", "dnsName"]
#Record type that are supported in Route53, currently testing only A record and CName
record_type_list = ["A", "AAAA", "CNAME", "MX", "TXT", "PTR", "SRV", "SPF", "NAPTR", "CAA", "NS", "DS"]
#Empty dict that will be filled inside
list_of_dict_to_process = []

client = boto3.client('route53')

class DnsUpdateException(Exception):

    def __init__(self, message="Some thing went wrong!!!"):
        self.message = message
        logger.error(message)
        super().__init__(self.message)


#Update with new records for the hosted_id mapping to the record_name
def updateRecordSetwithNewValue(hosted_id, record_name, record_type, new_value):
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
                                "Value": new_value
                            },
                        ],
                    }
                },
            ]
        }
    )
    logger.info(response)
    

#Find and replace all the records where source matches
def find_and_replace_all_records(findsource, replacetarget):
    try:
        logger.debug("List all hosted Zones")
        list_all_hostedzone_response = client.list_hosted_zones()
    except Exception as e:
        logger.error(f"Failed to get the list of all hosted zones {e}")
        raise e
        
    if list_all_hostedzone_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.debug("Making sure the list_hosted_zone call returned successfully")
        hosted_zones = list_all_hostedzone_response['HostedZones']
        if len(hosted_zones) == 0:
            message = "There are no Hosted Zones available"
            raise DnsUpdateException(message)                           
        for zone in hosted_zones:
            logger.info(f"Hosted Zone = {zone}")
            #List all the record set in the Hostedzone
            resource_record_sets = client.list_resource_record_sets(HostedZoneId=zone['Id'])
            if len(resource_record_sets) == 0:
                message = "There are no resource record sets found"
                raise DnsUpdateException(message)  
            for recordset in resource_record_sets['ResourceRecordSets']:
                for recordtype in record_type_list:
                    if recordset['Type'] == recordtype:
                        logger.debug(f"Found a {recordtype} Record set")
                        for resourcerecord in recordset['ResourceRecords']:
                            if resourcerecord['Value'] == findsource:
                                logger.info(f"Updating the record for {recordset}")
                                updateRecordSetwithNewValue(zone['Id'], recordset['Name'], recordtype, replacetarget)
    else:
        message = "Got an upexpcted response when trying to list hostedzone \
            repsonse" + str(list_all_hostedzone_response['ResponseMetadata']['HTTPStatusCode'])
        raise DnsUpdateException(message)                           


def update_records(list_of_dict_of_source_and_target_records):
    for each_item in list_of_dict_of_source_and_target_records:
        source = each_item['source_entry']
        dest = each_item['target_entry']
        logger.info(f"Source  = {source} and Target  = {dest}")
        find_and_replace_all_records(source, dest)


def add_source_target_to_process_dict(data_dict, value_type):
    for k, v in data_dict.items():
        if value_type in v['source'].keys():
            source = v['source'][value_type]
            dest = v['destination'][value_type]
            temp_dict = {"source_entry" : source, "target_entry" : dest}
            list_of_dict_to_process.append(temp_dict)
            logger.info(list_of_dict_to_process)

    

def lambda_handler(event, context):
    try:
        data = base64.b64decode(event["body-json"]).decode('utf-8').replace('null', '"user"')
        logger.info(data)
        data_dictionary = eval(data)
        if data_dictionary['recoveryStatus'] == "RECOVERY_COMPLETED":
            url = data_dictionary["resourceMapping"]["sourceRecoveryMappingPath"]
            response = urlopen(url)
            source_recovery_resource_mapping_json = json.loads(response.read())
            logger.info(f"source_recovery_resource_mapping_json =  {source_recovery_resource_mapping_json}")
            for source_recovery_map in source_recovery_resource_mapping_json:
                logger.info(f"source_recovery_map.keys() = {source_recovery_map.keys()}")
                for resource_type in resouce_list:
                    if resource_type in source_recovery_map.keys():
                        logger.info(f"Resource Type {resource_type}")
                        dat_values = list(source_recovery_map.values())
                        data_dict = dat_values[0][0]
                        logger.info(f"DATA VALUES {data_dict}")
                        for resource_property in resource_properties_list:
                            logger.debug(f"Fill all {resource_property} in the process dict")
                            add_source_target_to_process_dict(data_dict, resource_property)
                        
            if len(list_of_dict_to_process) != 0:
                logger.debug("Processing the data to update records based on source and destination values")
                update_records(list_of_dict_to_process)
                return {
                    'statusCode': 200,
                    'body': {"Status": "Success"}
                }
            else:
                logger.error(f"Failed as there are no source and target map found")
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
