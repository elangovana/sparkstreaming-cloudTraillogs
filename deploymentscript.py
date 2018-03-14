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

# client = boto3.client('kinesis')
# client.create_stream(
#     StreamName='ReproducedCloudTrailEventStream'
#     , ShardCount= 2)

#create stream to write to dd
client.create_stream(
    StreamName='AnomalyEventStream',
    ShardCount = 1)


# start up scripts
#sudo pip-3.4 install boto3
#aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin --ec2-attributes '{"KeyName":"SparkNViginia","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-dcaff0f3","EmrManagedSlaveSecurityGroup":"sg-de9226a8","EmrManagedMasterSecurityGroup":"sg-ec9c289a"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.12.0 --log-uri 's3n://aws-logs-606965926739-us-east-1/elasticmapreduce/' --name 'My cluster new' --instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1