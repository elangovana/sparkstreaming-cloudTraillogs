from __future__ import print_function
import sys

from sparkapplication.KinesisConsumer import KinesisConsumer

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: main.py <app-name> <stream-name> <endpoint-url> <region-name>",
            file=sys.stderr)
        sys.exit(-1)
    appName, streamName, endpointUrl, regionName = sys.argv[1:]

    KinesisConsumer().run(appName, streamName, endpointUrl, regionName)