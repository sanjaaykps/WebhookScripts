import json
import boto3
import base64
from urllib.request import urlopen

client = boto3.client('route53')


def updateRecordSetwithNewValue(hostedID, record_name, recordType, oldValue, NewValue):
    response = client.change_resource_record_sets(
        HostedZoneId=hostedID,
        ChangeBatch={
            "Comment": "DNS Updated Programatically",
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": record_name,
                        "Type": recordType,
                        "TTL": 180,
                        "ResourceRecords": [
                            {
                                "Value": NewValue
                            },
                        ],
                    }
                },
            ]
        }
    )
    print(response)

def findandreplaceArecord(sourceIP, targetIP):
    # hostedZones = list_all_hostedzone_response = 
    list_all_hostedzone_response = client.list_hosted_zones()
    if list_all_hostedzone_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        hosted_zones = list_all_hostedzone_response['HostedZones']
        for zone in hosted_zones:
            print(zone)
            #find if source IP is the value of the A record in any of the hosted zones
            hostedZoneDetails = client.list_resource_record_sets(HostedZoneId=zone['Id'])
            if hostedZoneDetails != "":
                for recordset in hostedZoneDetails['ResourceRecordSets']:
                    if recordset['Type'] == 'A':
                        for resourcerecord in recordset['ResourceRecords']:
                            if resourcerecord['Value'] == sourceIP:
                                print(recordset)
                                updateRecordSetwithNewValue(zone['Id'], recordset['Name'], 'A', sourceIP, targetIP)


def updateARecord(listofdictofips):
    for each_item in listofdictofips:
        sourceIP = each_item['sourceip']
        targetIP = each_item['targetip']
        findandreplaceArecord(sourceIP, targetIP)
    
process_dict = []
url = "https://storage.googleapis.com/appranixsra-devorg/recovery/53:devorg:6d81b34312da:97660c9559ad:1644860938670:6427d46ba5fb:c45d40470484/resource-mapping/cross6-metadata-changes.json?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=axproduction-53%40appranixsra.iam.gserviceaccount.com%2F20220215%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20220215T152349Z&X-Goog-Expires=14400&X-Goog-SignedHeaders=host&X-Goog-Signature=9f5ff48a03fe6b8a8dd374ad54fc6f8a5933aabfd3ed7c1dbf34f85f6ce9da9edd842c84bcf2de89326901356fba191a378c03566da805120c5553a7a239474e9fd3fd64edefeed7bf340bf7d4e4aa6e794f24d98755ceac9ff13f32543d8d7af2980ce3349120bf1d16577d40a981fc64795aa876fb8536437e39a96cdf8236d5faec7f12ad88498c976de53341338de19bd55c1719ffd4974b9ad159fc511d2f4e41faf1b724203b87f966f9b25fba709c4fe45382b71fb8e37dbede953d71b6214a4d0b4df6bea44a3b0f4f88e510fb4943ff6e704184454eae759552801af0164152ac5556136332b6903ea64dcecb2ffa81527c3c1ecc528e03926fc08f"
response = urlopen(url)
data_json = json.loads(response.read())
for dat in data_json:
    if "COMPUTE" in dat.keys():
        dat_values = list(dat.values())
        data_dict = dat_values[0][0]
        
        for k, v in data_dict.items():
            sourceip = v['source']['publicIpAddress']
            destip = v['destination']['publicIpAddress']
            ip_dict = {"sourceip" : sourceip, "targetip" : destip}
            process_dict.append(ip_dict)
            print(process_dict)
            
if len(process_dict) != 0:
    updateARecord(process_dict)
else:
    print("FAILED")

def lambda_handler(event, context):
    data = base64.b64decode(event["body-json"]).decode('utf-8').replace('null', '"user"')
    print(data)
    data_dictionary = eval(data)
    process_dict = []
    if data_dictionary['recoveryStatus'] == "RECOVERY_COMPLETED":
        url = data_dictionary["resourceMapping"]["sourceRecoveryMappingPath"]

        response = urlopen(url)
        data_json = json.loads(response.read())
        for dat in data_json:
            if "COMPUTE" in dat.keys():
                dat_values = list(dat.values())
                data_dict = dat_values[0][0]
                for k, v in data_dict.items():
                    sourceip = v['source']['publicIpAddress']
                    destip = v['destination']['publicIpAddress']
                    ip_dict = {"sourceip" : sourceip, "targetip" : destip}
                    process_dict.append(ip_dict)
                    print(process_dict)
                    
        if len(process_dict) != 0:
            updateARecord(process_dict)
            return {
                'statusCode': 200,
                'body': {"Status": "Success"}
            }
        else:
            print("FAILED")
            return {
                'statusCode': 505,
                'body': {"Status": "Success"}
            }
            

