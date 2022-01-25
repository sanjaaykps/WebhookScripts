#!/bin/bash

#########PRE-REQUSITE################################################
##CONFIGURE AWS CLI and Authenticate for this Script to be executed##
#####################################################################
#Variable Declaration - Change this based on your requirement
HOSTED_ZONE_ID="XXXXXXXX"
NAME="example.com."
TYPE="A"
TTL=60

#get current IP address ( Replace with the Source IP address you want to monitor)
IP=$(curl http://checkip.amazonaws.com/)

#validate IP address (makes sure Route 53 doesn't get updated with a malformed payload)
if [[ ! $IP =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
	echo "Source IP Address is not valid. Exit."
    exit 1
fi

#get current
aws2 route53 list-resource-record-sets --hosted-zone-id $HOSTED_ZONE_ID | \
jq -r '.ResourceRecordSets[] | select (.Name == "'"$NAME"'") | select (.Type == "'"$TYPE"'") | .ResourceRecords[0].Value' > /tmp/current_route53_value

cat /tmp/current_route53_value

#check if IP is different from Route 53
if grep -Fxq "$IP" /tmp/current_route53_value; then
	echo "IP Has Not Changed, Exiting"
	exit 1
fi


echo "IP Changed, Updating Records"

#prepare route 53 payload
cat > /tmp/route53_changes.json << EOF
    {
      "Comment":"Updated From DDNS Shell Script",
      "Changes":[
        {
          "Action":"UPSERT",
          "ResourceRecordSet":{
            "ResourceRecords":[
              {
                "Value":"$IP"
              }
            ],
            "Name":"$NAME",
            "Type":"$TYPE",
            "TTL":$TTL
          }
        }
      ]
    }
EOF

#update records
aws2 route53 change-resource-record-sets --hosted-zone-id $HOSTED_ZONE_ID --change-batch file:///tmp/route53_changes.json >> /dev/null