spark-submit  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.1.2 main.py myStream myStream https://kinesis.us-east-1.amazonaws.com us-east-1


#spark-submit  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.1.2 --py-files buildartifacts/dist/jobs.zip,buildartifacts/dist/libs.zip  buildartifacts/dist/main.py cloudtrailmockAppNew myStream https://kinesis.us-east-1.amazonaws.com us-east-1



# Kick off locut to data inot kinese
locust -f buildartifacts/dist/MockKinesisProducerLocust.py --no-web -c 1 -r 1

import boto.dynamodb, uuid, time

ip = '10.1.1.2'
hits = 10
print("ip {} hit {}", ip, hits)
conn = boto.dynamodb.connect_to_region(
                'us-east-1')

table = conn.get_table('CloudTrailAnomaly')
item_data = {
    'sourceIPAddress': ip,
    'count': hits
}
i=table.new_item(
    # Our hash key is 'forum'
    hash_key=str(uuid.uuid4()),
    # Our range key
    range_key=(int(time.time())),
    # This has the
    attrs=item_data
)