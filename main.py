from __future__ import print_function
import sys

from sparkapplication.KinesisConsumer import KinesisConsumer

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print(
            "Usage: main.py <app-name> <stream-name> <endpoint-url> <region-name> <anomaly-stream-name> <rewrite-stream-name>",
            file=sys.stderr)
        sys.exit(-1)
    appName, streamName, endpointUrl, regionName, anomaly_stream_name, rewrite_steam_name= sys.argv[1:]

    KinesisConsumer().run(appName, streamName, endpointUrl, regionName, anomaly_stream_name=anomaly_stream_name, rewrite_stream_name= rewrite_steam_name   )
