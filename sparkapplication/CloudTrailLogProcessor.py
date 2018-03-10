#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
  Consumes messages from a Amazon Kinesis streams and does wordcount.

  This example spins up 1 Kinesis Receiver per shard for the given stream.
  It then starts pulling from the last checkpointed sequence number of the given stream.

  Usage: kinesis_wordcount_asl.py <app-name> <stream-name> <endpoint-url> <region-name>
    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
    <stream-name> name of the Kinesis stream (ie. mySparkStream)
    <endpoint-url> endpoint of the Kinesis service
      (e.g. https://kinesis.us-east-1.amazonaws.com)


  Example:
      # export AWS keys if necessary
      $ export AWS_ACCESS_KEY_ID=<your-access-key>
      $ export AWS_SECRET_KEY=<your-secret-key>

      # run the example
      $ bin/spark-submit -jar external/kinesis-asl/target/scala-*/\
        spark-streaming-kinesis-asl-assembly_*.jar \
        external/kinesis-asl/src/main/python/examples/streaming/kinesis_wordcount_asl.py \
        myAppName mySparkStream https://kinesis.us-east-1.amazonaws.com

  There is a companion helper class called KinesisWordProducerASL which puts dummy data
  onto the Kinesis stream.

  This code uses the DefaultAWSCredentialsProviderChain to find credentials
  in the following order:
      Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
      Java System Properties - aws.accessKeyId and aws.secretKey
      Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
      Instance profile credentials - delivered through the Amazon EC2 metadata service
  For more information, see
      http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html

  See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more details on
  the Kinesis Spark Streaming integration.
"""
from __future__ import print_function
import json
import uuid
import boto3.session

from boto import dynamodb
import boto3
import time
from pyspark import HiveContext, SQLContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
from pyspark.streaming.kinesis import KinesisUtils


class CloudTrailLogProcessor:

    def write_to_dynamodb(self, item):
        # TODO hardcode region name to fix
        # Bug:  boto3.client('dynamodb' requires api version & region name, else I get the error
        #     File
        #     "/tmp/pip-build-KYLsEh/botocore/botocore/loaders.py", line
        #     424, in load_data
        #     raise DataNotFoundError(data_path=name)
        #
        # DataNotFoundError: Unable to load for endpoints

        item.pprint()
        # ip = item[0]
        # hits = item[1]
        ip = "1.0.0.0"
        hits = 0

        print("ip {} hit {}", ip, hits)

        # client = boto3.client('dynamodb',  region_name='us-east-1', api_version='2012-08-10')
        # client.put_item(TableName='CloudTrailAnomaly', Item={'id': {'S': str(uuid.uuid4())}
        #     , 'timestamp': {'N': str(int(time.time()))}
        #     , 'sourceIPAddress': {'S': ip}
        #     , 'count': {'N': str(hits)}
        #                                                      })

        conn = dynamodb.connect_to_region(
            'us-east-1')

        table = conn.get_table('CloudTrailAnomaly')
        item_data = {
            'sourceIPAddress': ip,
            'count': hits
        }
        dynmodb_item = table.new_item(
            # Our hash key is 'forum'
            hash_key=str(uuid.uuid4()),
            # Our range key
            range_key=str(int(time.time())),
            # This has the
            attrs=item_data
        )
        dynmodb_item.put()

    def write_to_dynamodb_boto2(self, item):
        # TODO hardcode region name to fix
        # Bug:  boto3.client('dynamodb' requires api version & region name, else I get the error
        #     File
        #     "/tmp/pip-build-KYLsEh/botocore/botocore/loaders.py", line
        #     424, in load_data
        #     raise DataNotFoundError(data_path=name)
        #
        # DataNotFoundError: Unable to load for endpoints

        # ip = item[0]
        # hits = item[1]
        ip = item[0]
        hits = item[1]

        print("ip {} hit {}", ip, hits)

        conn = dynamodb.connect_to_region(
            'us-east-1')

        table = conn.get_table('CloudTrailAnomaly')
        item_data = {
            'sourceIPAddress': ip,
            'count': hits
        }
        dynmodb_item = table.new_item(
            # Our hash key is 'forum'
            hash_key=str(uuid.uuid4()),
            # Our range key
            range_key=str(int(time.time())),
            # This has the
            attrs=item_data
        )
        dynmodb_item.put()

    def write_to_kineses(self, anomaly_tuple):
        ip = anomaly_tuple[0]
        hits = anomaly_tuple[1]
        hash_key = str(uuid.uuid4())
        detectOnTimeStamp = str(int(time.time()))
        # TODO Hardcode names for stream
        stream_name = "AnomalyEventStream"

        client = self.get_kinesis_client()

        item = {'id': {'S': hash_key}
            , 'detectedOnTimestamp': {'N': detectOnTimeStamp}
            , 'sourceIPAddress': {'S': ip}
            , 'count': {'N': str(hits)}}

        client.put_record(
            StreamName=stream_name,
            Data=json.dumps(item),
            PartitionKey=str(uuid.uuid4()),
            SequenceNumberForOrdering=detectOnTimeStamp
        )

    def get_kinesis_client(self):
        session = boto3.session.Session(region_name='us-east-1')
        client = session.client('kinesis', region_name='us-east-1',
                                endpoint_url="https://kinesis.us-east-1.amazonaws.com")
        return client

    def write_to_kineses_raw(self, raw):

        # TODO Hardcode names for stream
        stream_name = "ReproducedCloudTrailEventStream"

        client = self.get_kinesis_client()


        client.put_record(
            StreamName=stream_name,
            Data=raw,
            PartitionKey=str(uuid.uuid4()),
            SequenceNumberForOrdering=str(int(time.time()))
        )

    def process(self, sc, ssc, dstreamRecords):
        #writ to originalStream
        dstreamRecords.foreachRDD(lambda rdd: rdd.foreach(lambda x: self.write_to_kineses_raw(x)))

        # TODO filter for count > threshold
        json_dstream = dstreamRecords.map(lambda v: json.loads(v)). \
            map(lambda ct: (ct['sourceIPAddress'], 1)). \
            reduceByKeyAndWindow(lambda a, b: a + b, invFunc=None, windowDuration=30, slideDuration=30)

        json_dstream.pprint()

        # Write anomalies to dynamodb
        json_dstream.foreachRDD(lambda rdd: rdd.foreach(lambda x: self.write_to_kineses(x)))

        # counts = dstreamRecords.map(lambda word: (str(uuid.uuid4()), 1)) \
        #     .reduceByKey(lambda a, b: a + b)
        # counts.pprint()

        # pythonSchema = StructType() \
        #     .add("awsRegion", StringType()) \
        #     .add("sourceIPAddress", StringType())
        #
        #
        # def process_rdd(rdd):
        #     print(rdd)
        #     rdd.map(lambda a: str(a)).map(lambda a: from_json(a, pythonSchema).registerAsTable("ctrail"))
        #     teenagers = sqc.sql("SELECT name FROM ctrail ")
        #     print(teenagers)
        #
        #
        # #rdd.map(_.split(",")).map(p= > Persons(p(0), p(1).trim.toInt)).registerAsTable("data")
        #
        # sqc = SQLContext(sc);
        #
        #
        # dstreamRecords.foreachRDD(process_rdd)

# \
#        # .add("timestamp", TimestampType())
#
#        context = HiveContext(sc)
#
#        dataDevicesDF = dstreamRecords \
#            .selectExpr("cast (data as STRING) jsonData") \
#            .select(from_json("jsonData", pythonSchema).alias("ctrail")) \
#            .select("ctrail.*") \
#            .groupby("sourceIPAddress")
#
#        dataDevicesDF.pprint()
