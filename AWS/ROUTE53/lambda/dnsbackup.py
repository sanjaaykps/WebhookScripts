import json
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
message = "Something went wrong !!!"

bucket_name = 'appranix-bucket-for-dnsbackup'
region = 'us-east-2'
s3 = boto3.client('s3')
route53 = boto3.client('route53')


def create_bucket_if_not_exist(region):
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info('Bucket "{}" already exists'.format(bucket_name))
    except ClientError as e:
        try:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == '403':
                if region == 'us-east-1':
                    s3.create_bucket(ACL='private', Bucket=bucket_name)
                else:
                    s3.create_bucket(ACL='private', Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})

                s3.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'})
                s3.put_bucket_encryption(
                    Bucket=bucket_name,
                    ServerSideEncryptionConfiguration={'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]}
                )

                retention_period = 365
                s3.put_bucket_lifecycle_configuration(
                    Bucket=bucket_name,
                    LifecycleConfiguration={
                        "Rules": [
                            {
                                "Expiration": {"Days": retention_period},
                                "ID": "S3 Deletion Rule",
                                "Filter": {"Prefix": ""},
                                "Status": "Enabled",
                                "NoncurrentVersionExpiration": {"NoncurrentDays": retention_period}
                            }
                        ]
                    }
                )

                s3.put_bucket_policy(
                    Bucket=bucket_name,
                    Policy="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"Stmt1566916793194\",\"Action\":\"s3:*\",\"Effect\":\"Deny\","
                        "\"Resource\":\"arn:aws:s3:::" + bucket_name + "/*\",\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"}},"
                                                                        "\"Principal\":\"*\"}]} "
                )

                logger.info('Created the bucket "{}" with SSL verification, versioning and SSE enabled'.format(bucket_name))
            else:
                logger.info(f"Different Error Code {e.response['Error']['Code']}")
        except Exception as e:
            logger.info(f"Entirely Different Error Mesage {e.message}")


def get_route53_hosted_zones(next_dns_name=None, next_hosted_zone_id=None):
    if next_dns_name and next_hosted_zone_id:
        response = route53.list_hosted_zones_by_name(DNSName=next_dns_name, HostedZoneId=next_hosted_zone_id)
    else:
        response = route53.list_hosted_zones_by_name()
    zones = response['HostedZones']
    if response['IsTruncated']:
        zones += get_route53_hosted_zones(response['NextDNSName'], response['NextHostedZoneId'])

    private_hosted_zones = list(filter(lambda x: x['Config']['PrivateZone'], zones))
    for zone in private_hosted_zones:
        zone['VPCs'] = route53.get_hosted_zone(Id=zone['Id'])['VPCs']
    return zones


def get_route53_zone_records(zone_id, start_record_name=None, start_record_type=None):
    if start_record_name and start_record_type:
        response = route53.list_resource_record_sets(HostedZoneId=zone_id, StartRecordName=start_record_name, StartRecordType=start_record_type)
    else:
        response = route53.list_resource_record_sets(HostedZoneId=zone_id)
    zone_records = response['ResourceRecordSets']

    if response['IsTruncated']:
        zone_records += get_route53_zone_records(zone_id, response['NextRecordName'], response['NextRecordType'])

    return zone_records


def get_route53_health_checks(marker=None):
    if marker:
        response = route53.list_health_checks(Marker=marker)
    else:
        response = route53.list_health_checks()
    health_checks = response['HealthChecks']
    if response['IsTruncated']:
        health_checks += get_route53_health_checks(response['NextMarker'])

    return health_checks


def lambda_handler(event, context):
    logger.info(str(event))
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", datetime.utcnow().utctimetuple())

    create_bucket_if_not_exist(region)

    hosted_zones = get_route53_hosted_zones()
    s3.put_object(Body=json.dumps(hosted_zones).encode(), Bucket=bucket_name, Key='{}/zones.json'.format(timestamp))

    for zone in hosted_zones:
        zone_records = get_route53_zone_records(zone['Id'])
        s3.put_object(Body=json.dumps(zone_records).encode(), Bucket=bucket_name, Key="{}/{}.json".format(timestamp, zone['Name']))

    health_checks = get_route53_health_checks()
    for health_check in health_checks:
        tags = route53.list_tags_for_resource(ResourceType='healthcheck', ResourceId=health_check['Id'])['ResourceTagSet']['Tags']
        health_check['Tags'] = tags

    s3.put_object(Body=json.dumps(health_checks).encode(), Bucket=bucket_name, Key="{}/Health checks.json".format(timestamp))

    s3.put_object(Body=timestamp.encode(), Bucket=bucket_name, Key="latest_backup_timestamp")

    message = "Success: {} zones backed up and {} health checks backed up at {}".format(len(hosted_zones), len(health_checks), timestamp)
    logger.info(message)

    return {
        "message" : message
    }
