from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

from sparkapplication.CloudTrailLogProcessor import CloudTrailLogProcessor


class KinesisConsumer:
    def run(self, appName, streamName, endpointUrl, regionName):
        sc = SparkContext(appName="PythonStreamingKinesisWordCountAsl")
        ssc = StreamingContext(sc, 1)
        dstreamRecords = KinesisUtils.createStream(
            ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)
        CloudTrailLogProcessor().process(ssc, dstreamRecords)
        ssc.start()
        ssc.awaitTermination()