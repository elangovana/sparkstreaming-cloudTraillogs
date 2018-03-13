import boto3
import uuid
import time
import base64
import json


def lambda_handler(event, context):
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"])
        print("Decoded payload: " + str(payload))
        write_to_dynamodb(json.loads((payload.decode("utf-8"))))


def write_to_dynamodb(json_payload):
    sourceIPAddress = json_payload["SOURCEIPADDRESS"]
    hits = json_payload["OCCURRENCE"]
    detectedOnTimestamp = json_payload["EVENTTIMESTAMP"]
    id = json_payload["ID"]
    anomalyScore = json_payload["SCORE"]
    item = {'id': {'S': id}
        , 'timestamp': {'N': str(int(time.time()))}
        , 'sourceIPAddress': {'S': sourceIPAddress}
        , 'count': {'N': str(hits)}
        , 'detectedOnTimestamp': {'N': str(detectedOnTimestamp)}
        , 'anomalyScore': {'N': str(anomalyScore)}
            }

    print(item)
    client = boto3.client('dynamodb', region_name='us-east-1', api_version='2012-08-10')
    client.put_item(TableName='CloudTrailAnomaly', Item=item)
    print("Wrote to dynamodbmo : " + str(item))