from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import pyspark.sql.streaming
from sparkapplication.CloudTrailLogProcessor import CloudTrailLogProcessor


class KinesisConsumer:
    def run(self, appName, streamName, endpointUrl, region_name, rewrite_stream_name, anomaly_stream_name):
        sc = SparkContext(appName="PythonStreamingKinesisAnomalyDetection")
        print("Initialised SC")
        #TODO: log warn and above only
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
        ssc = StreamingContext(sc, 1)
        dstreamRecords = KinesisUtils.createStream(
            ssc, appName, streamName, endpointUrl, region_name, InitialPositionInStream.LATEST, 2)
        CloudTrailLogProcessor(rewrite_stream_name = rewrite_stream_name, anomaly_stream_name=anomaly_stream_name, region=region_name)\
            .process(sc, ssc, dstreamRecords)
        ssc.start()
        ssc.awaitTermination()