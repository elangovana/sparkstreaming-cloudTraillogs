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
    sourceIPAddress = json_payload["sourceIPAddress"]
    hits = json_payload["count"]
    detectedOnTimestamp = json_payload["detectedOnTimestamp"]
    id = json_payload["id"]
    anomalyScore = json_payload["anomalyScore"]
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