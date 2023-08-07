import json
import boto3
from urllib.request import urlopen
import logging
import os
logger = logging.getLogger()
#TODO Logging Level to be picked from the environment variable
LOGLEVEL = os.getenv('LOGLEVEL').upper()
logging.basicConfig(level=LOGLEVEL)

message = "Something went wrong !!!"

# Appranix Resource Types that are supported in this script
# RESOURCE_LIST=COMPUTE,APPLICATION_LOAD_BALANCER,CLASSIC_LOAD_BALANCER,RDS_INSTANCE
aws_resource_list = os.environ.get('RESOURCE_LIST')
resouce_list = aws_resource_list.split(',')

# Appranix Resource Property that are supported in this script to be replaced with that of equivalend recovered resource 
# RESOURCE_PROPERTIES_LIST=publicIpAddress,privateIpAddress,privateDnsName,publicDnsName,dnsName,endpoint
aws_resource_properties_list = os.environ.get('RESOURCE_PROPERTIES_LIST')
resource_properties_list = aws_resource_properties_list.split(',')

# Record type that are supported in Route53, currently testing only A record and CName 
# RECORD_TYPE_LIST=A,AAAA,CNAME,MX,TXT,PTR,SRV,SPF,NAPTR,CAA,NS,DS
aws_record_type_list = os.environ.get('RECORD_TYPE_LIST')
record_type_list = aws_record_type_list.split(',')

list_of_dict_to_process = []

# TODO change this
# HOSTED_ZONE_ID=zone_id_1,zone_id_2
hosted_zones = os.environ.get('HOSTED_ZONE_ID')
hosted_zones_to_update = hosted_zones.split(',')

client = boto3.client('route53')

class DnsUpdateException(Exception):

    def __init__(self, message="Some thing went wrong!!!"):
        self.message = message
        logger.error(message)
        super().__init__(self.message)


def update_alias_records(hosted_id, record_name, record_type, new_value, targetZoneId):
    response = client.change_resource_record_sets(
        HostedZoneId=hosted_id,
        ChangeBatch={
            "Comment": "DNS Alias Updated Programatically",
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": record_name,
                        "Type": record_type,
                        "AliasTarget":{
                            'HostedZoneId': targetZoneId,
                            'DNSName': "dualstack." + new_value + ".",
                            'EvaluateTargetHealth': True
                        },
                    }
                },
            ]
        }
    )
    logger.info("Updated ALIAS RECORDSET with NEW VALUES")

def trimRecvDNSName(recoveredDNSName):
    recoveredDNSName = recoveredDNSName.split('-')
    recoveredDNSName = recoveredDNSName[:len(recoveredDNSName)-1]
    recoveredDNSName = '-'.join(recoveredDNSName)
    return recoveredDNSName


def findRecoveredResourceAlias(recoveredDNSNameToCheckAlias):
    recoveredDNSName = recoveredDNSNameToCheckAlias.split('.',4)[0]
    recoveredDNSName = recoveredDNSName.split('-')
    recoveredDNSName = recoveredDNSName[:len(recoveredDNSName)-1]
    recoveredDNSName = '-'.join(recoveredDNSName)
    
    recoveryRegion = recoveredDNSNameToCheckAlias.split('.')[1]
    elb_client = boto3.client('elbv2',region_name=recoveryRegion)
    response = elb_client.describe_load_balancers(Names=[recoveredDNSName])
    recvCanonicalId = response['LoadBalancers'][0]['CanonicalHostedZoneId']
    return recvCanonicalId

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
    hosted_zones = hosted_zones_to_update
    for zone in hosted_zones:
        logger.info(f"Hosted Zone = {zone}")
        #List all the record set in the Hostedzone
        resource_record_sets = client.list_resource_record_sets(HostedZoneId = zone)
        if len(resource_record_sets) == 0:
            logger.info("There are no resource record sets found")
            continue
        for recordset in resource_record_sets['ResourceRecordSets']:
            for recordtype in record_type_list:
                if recordset['Type'] == recordtype:
                    logger.debug(f"Found a {recordtype} Record set")
                    if 'ResourceRecords' in recordset:
                        for resourcerecord in recordset['ResourceRecords']:
                            if resourcerecord['Value'] == findsource:
                                logger.info(f"Updating the record for {recordset}")
                                updateRecordSetwithNewValue(zone, recordset['Name'], recordtype, replacetarget)
                    if 'AliasTarget' in recordset:
                        for resourcerecord in recordset['AliasTarget']:
                            if recordset['AliasTarget']["DNSName"] == str("dualstack." + findsource + "."):
                                recoveredZoneId = findRecoveredResourceAlias(replacetarget)
                                update_alias_records(zone, recordset['Name'], recordtype, replacetarget,recoveredZoneId)


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
        logger.info(str(event))
        data_dictionary = event
        data_dictionary = json.loads(event['body'])
        # logger.info(str(data_dictionary))
        if data_dictionary["recoveryStatus"] == "RECOVERY_COMPLETED":
            url = data_dictionary["resourceMapping"]["sourceRecoveryMappingPath"]
            logger.info(url)
            response = urlopen(url)
            source_recovery_resource_mapping_json = json.loads(response.read())
            logger.info(f"source_recovery_resource_mapping_json =  {source_recovery_resource_mapping_json}")
            for source_recovery_map in source_recovery_resource_mapping_json:
                logger.info(f"source_recovery_map.keys() = {source_recovery_map.keys()}")
                for resource_type in resouce_list:
                    if resource_type in source_recovery_map.keys():
                        logger.info(f"Resource Type {resource_type}")
                        all_resource_details = list(source_recovery_map.values())
                        for each_resoucetypes in all_resource_details:
                            for each_resource in each_resoucetypes:
                                logger.info(f"Each Resources {each_resource}")
                                for resource_property in resource_properties_list:
                                    logger.debug(f"Fill all {resource_property} in the process dict")
                                    add_source_target_to_process_dict(each_resource, resource_property)
            
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
