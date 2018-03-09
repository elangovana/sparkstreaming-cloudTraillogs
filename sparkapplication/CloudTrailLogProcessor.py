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
from pyspark import HiveContext, SQLContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType


class CloudTrailLogProcessor:

    def process(self, sc, ssc, dstreamRecords):
        print("process......")
        json_dstream = dstreamRecords.map(lambda v: json.loads(v))
        json_dstream.pprint()
        text_counts = json_dstream.map(lambda ct: (ct['awsRegion'], 1)). \
            reduceByKey(lambda x, y: x + y)

        text_counts.pprint()

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


