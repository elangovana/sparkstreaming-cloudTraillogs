from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import pyspark.sql.streaming
from sparkapplication.CloudTrailLogProcessor import CloudTrailLogProcessor


class KinesisConsumer:
    def run(self, appName, streamName, endpointUrl, regionName):
        sc = SparkContext(appName="PythonStreamingKinesisWordCountAsl")
        print("Initialised SC")
        #TODO: log warn and above only
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
        ssc = StreamingContext(sc, 1)
        dstreamRecords = KinesisUtils.createStream(
            ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)
        CloudTrailLogProcessor().process(sc, ssc, dstreamRecords)
        ssc.start()
        ssc.awaitTermination()