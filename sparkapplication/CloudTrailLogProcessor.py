#

"""
  Detects anomalies and writes them back to a kinesis streams

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
        is_anomaly = str(anomaly_tuple[2])
        hash_key = str(uuid.uuid4())
        detectOnTimeStamp = str(int(time.time()))
        # TODO Hardcode names for stream
        stream_name = "AnomalyEventStream"

        item = {'id': hash_key
            , 'detectedOnTimestamp': detectOnTimeStamp
            , 'sourceIPAddress':  ip
            , 'count':  hits
            , "isAnomaly":is_anomaly}

        client = self._get_kinesis_client()
        client.put_record(
            StreamName=stream_name,
            Data=json.dumps(item),
            PartitionKey=hash_key,
            SequenceNumberForOrdering=detectOnTimeStamp
        )

    #TODO might not need this code, check if kineses to kineses stream "copy" is possible
    def write_orginial_data_kineses(self, raw):
        # TODO Hardcode names for stream
        stream_name = "ReproducedCloudTrailEventStream"

        client = self._get_kinesis_client()

        client.put_record(
            StreamName=stream_name,
            Data=raw,
            PartitionKey=str(uuid.uuid4()),
            SequenceNumberForOrdering=str(int(time.time()))
        )

    # TODO remove this,as this does not work as kinese connector doesnt create df,
    def detect_anomaly_withsql(self, sc, ssc, dstream):
        # Apply windows
        dstream_window = dstream.window(windowDuration=30, slideDuration=30)

        # Get just the source ip address from the json
        accumulated_dstream = dstream_window \
            .map(lambda v: json.loads(v)) \
            .map(lambda j: Row(j['sourceIPAddress'], j['awsRegion'])).filter()

        accumulated_dstream.pprint()

        # http: // spark.apache.org / docs / latest / structured - streaming - programming - guide.html
        # Get the singleton instance of SparkSession
        # spark = self._getSparkSessionInstance(row_rdd.context().getconf())
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
        threshold_count = 50
        # Group by by IP & count and falg if anomaly
        dstream_anomalies = dstream \
            .map(lambda v: json.loads(v)) \
            .map(lambda ct: (ct["detail"]['sourceIPAddress'], 1)) \
            .reduceByKeyAndWindow(lambda a, b: a + b, invFunc=None, windowDuration=30, slideDuration=30)\
            .map(lambda r: (r[0], r[1],  r[1] > threshold_count ))

        dstream_anomalies.pprint()

        # send anomalies to kineses
        dstream_anomalies.foreachRDD(lambda rdd: rdd.foreach(lambda x: self.write_anomaly_kineses(x)))

    def process(self, sc, ssc, dstreamRecords):
        # write to original data back to a different stream
        dstreamRecords.foreachRDD(lambda rdd: rdd.foreach(lambda x: self.write_orginial_data_kineses(x)))

        # detect anomalies
        self.detect_anomaly(sc, ssc, dstreamRecords)

    def _get_kinesis_client(self):
        #TODO use the standapproach for boto3session
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
