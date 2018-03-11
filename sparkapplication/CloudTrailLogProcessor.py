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
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from boto import dynamodb
import boto3
import time
from pyspark import HiveContext, SQLContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
from pyspark.streaming.kinesis import KinesisUtils


class CloudTrailLogProcessor:

    def write_anomaly_kineses(self, anomaly_tuple):
        ip = anomaly_tuple[0]
        hits = anomaly_tuple[1]
        hash_key = str(uuid.uuid4())
        detectOnTimeStamp = str(int(time.time()))
        # TODO Hardcode names for stream
        stream_name = "AnomalyEventStream"

        item = {'id': {'S': hash_key}
            , 'detectedOnTimestamp': {'N': detectOnTimeStamp}
            , 'sourceIPAddress': {'S': ip}
            , 'count': {'N': str(hits)}}



        # client = self.get_kinesis_client()
        # client.put_record(
        #     StreamName=stream_name,
        #     Data=json.dumps(item),
        #     PartitionKey=str(uuid.uuid4()),
        #     SequenceNumberForOrdering=detectOnTimeStamp
        # )

    def write_orginial_data_kineses(self, raw):
        # TODO Hardcode names for stream
        stream_name = "ReproducedCloudTrailEventStream"

        print(raw)

        # client = self.get_kinesis_client()
        #
        #
        # client.put_record(
        #     StreamName=stream_name,
        #     Data=raw,
        #     PartitionKey=str(uuid.uuid4()),
        #     SequenceNumberForOrdering=str(int(time.time()))
        # )

#TODO, this doesnot work,
    def detect_anomaly_withsql(self, sc, ssc, dstream):
        # Apply windows
        dstream_window = dstream.window(windowDuration=30, slideDuration=30)

        # Get just the source ip address from the json
        accumulated_dstream = dstream_window\
            .map(lambda v: json.loads(v)) \
            .map(lambda j: Row(j['sourceIPAddress'], j['awsRegion'])).filter()

        accumulated_dstream.pprint()

        #http: // spark.apache.org / docs / latest / structured - streaming - programming - guide.html
        # Get the singleton instance of SparkSession
        #spark = self._getSparkSessionInstance(row_rdd.context().getconf())
        spark = self._getSparkSessionInstance(sc._conf)

        # create dataframe from rdd
        df_ip = spark.createDataFrame(accumulated_dstream)
        df_ip.createOrReplaceTempView("events")

        # Anomaly when more then N (10) hits per source IP
        anomalies = spark.sql(
            "select sourceIPAddress, count(*) as total from events group by sourceIPAddress having count(*) > 10 ")

        # send anomalies to kineses
        anomalies.foreach(lambda a: self.write_anomaly_kineses(a))

    def detect_anomaly(self, sc, ssc, dstream):
        # Apply windows
        dstream_window = dstream.window(windowDuration=30, slideDuration=30)

        # Group by by IP & count
        dstream_window = dstream\
            .map(lambda v: json.loads(v)) \
            .map(lambda ct: (ct['sourceIPAddress'], 1)) \
            .reduceByKeyAndWindow(lambda a, b: a + b, invFunc=None, windowDuration=30, slideDuration=30)

        dstream_window.pprint()

        #Anomalythreshold 3
        anomalies = dstream.filter(lambda t : t[1] > 1)
        anomalies.pprint()

        # send anomalies to kineses
        anomalies.foreachRDD(lambda rdd: rdd.foreach(lambda x: self.write_anomaly_kineses(x)))



    def process(self, sc, ssc, dstreamRecords):
        # write to original data back to a different stream
        dstreamRecords.foreachRDD(lambda rdd: rdd.foreach(lambda x: self.write_orginial_data_kineses(x)))

        # detect anomalies
        self.detect_anomaly(sc, ssc, dstreamRecords)

    def _get_kinesis_client(self):
        session = boto3.session.Session(region_name='us-east-1')
        client = session.client('kinesis', region_name='us-east-1',
                                endpoint_url="https://kinesis.us-east-1.amazonaws.com")
        return client

    def _getSparkSessionInstance(self, sparkConf):
        if "sparkSessionSingletonInstance" not in globals():
            globals()["sparkSessionSingletonInstance"] = SparkSession \
                .builder \
                .config(conf=sparkConf) \
                .getOrCreate()
        return globals()["sparkSessionSingletonInstance"]
