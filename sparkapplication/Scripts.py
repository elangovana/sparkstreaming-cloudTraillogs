import boto3

#Setup dynamodb
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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
