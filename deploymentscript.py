import boto3

#Setup dynamodb
region = 'us-east-1'
dynamodb = boto3.resource('dynamodb', region_name= region)
table = dynamodb.create_table(
    TableName='CloudTrailAnomaly',
    KeySchema=[
        {
            'AttributeName': 'id',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[

        {
            'AttributeName': 'id',
            'AttributeType': 'S'
        },

        {
            'AttributeName': 'timestamp',
            'AttributeType': 'N'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

#Create kinese stream to receive cloud* logs
client = boto3.client('kinesis')
client.create_stream(
    StreamName='CloudTrailEventStream'
    , ShardCount= 2)

client = boto3.client('kinesis')
client.create_stream(
    StreamName='ReproducedCloudTrailEventStream'
    , ShardCount= 2)

#create stream to write to dd
client.create_stream(
    StreamName='AnomalyEventStream',
    ShardCount = 1)



