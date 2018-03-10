import boto3
import uuid
import time
import base64

def lambda_handler(event, context):
    for record in event['Records']:
       #Kinesis data is base64 encoded so decode here
       payload=base64.b64decode(record["kinesis"]["data"])
       print("Decoded payload: " + str(payload))
    #write_to_lambda('1.0.0.1', '5')
    return 'Hello from Lambda'


def write_to_lambda(ip, hits):

    client = boto3.client('dynamodb',  region_name='us-east-1', api_version='2012-08-10')
    client.put_item(TableName='CloudTrailAnomaly', Item={'id': {'S': str(uuid.uuid4())}
        , 'timestamp': {'N': str(int(time.time()))}
        , 'sourceIPAddress': {'S': ip}
        , 'count': {'N': str(hits)}})