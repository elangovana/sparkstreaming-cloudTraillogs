import sys

import time
from locust import Locust

from datetime import datetime
import uuid
from locust import TaskSet, task
from boto import kinesis


class MockKinesisProducer(TaskSet):

    def get_stream_status(self, conn, stream_name):
        '''
        Query this provided connection object for the provided stream's status.
        :type conn: boto.kinesis.layer1.KinesisConnection
        :param conn: A connection to Amazon Kinesis
        :type stream_name: str
        :param stream_name: The name of a stream.
        :rtype: str
        :return: The stream's status
        '''
        r = conn.describe_stream(stream_name)
        description = r.get('StreamDescription')
        return description.get('StreamStatus')

    def wait_for_stream(self, conn, stream_name):
        '''
        Wait for the provided stream to become active.
        :type conn: boto.kinesis.layer1.KinesisConnection
        :param conn: A connection to Amazon Kinesis
        :type stream_name: str
        :param stream_name: The name of a stream.
        '''
        SLEEP_TIME_SECONDS = 3
        status = self.get_stream_status(conn, stream_name)
        while status != 'ACTIVE':
            print('{stream_name} has status: {status}, sleeping for {secs} seconds'.format(
                stream_name=stream_name,
                status=status,
                secs=SLEEP_TIME_SECONDS))
            time.sleep(SLEEP_TIME_SECONDS)  # sleep for 3 seconds
            status = self.get_stream_status(conn, stream_name)

    def put_record_in_stream(self, conn, stream_name, data):
        '''
        Put each word in the provided list of words into the stream.
        :type conn: boto.kinesis.layer1.KinesisConnection
        :param conn: A connection to Amazon Kinesis
        :type stream_name: str
        :param stream_name: The name of a stream.
        :type data: str
        :param data: Data to put
        '''
        try:
            partition_key = str(uuid.uuid4())
            conn.put_record(stream_name, data, partition_key)
            print("Put data: " + data + " into stream: " + stream_name)
        except Exception as e:
            sys.stderr.write("Encountered an exception while trying to put data: "
                             + data + " into stream: " + stream_name + " exception was: " + str(e))

    @task
    def put_data(self):
        # TODO: Fix hardcoded region
        region = "us-east-1"
        stream_name = "myStream"
        conn = kinesis.connect_to_region(region_name=region)
        try:
            # Check stream status
            status = self.get_stream_status(conn, stream_name)
            if 'DELETING' == status:
                print('The stream: {s} is being deleted, please rerun the script.'.format(s=stream_name))
                sys.exit(1)
            elif 'ACTIVE' != status:
                self.wait_for_stream(conn, stream_name)

            # put data into stream
            data = self.get_mock_data()
            self.put_record_in_stream(conn, stream_name, data)
        except Exception as error:
            print('{}'.format(error))
            sys.exit(1)

    def get_mock_data(self):
        data = "-------------------"
        max = 1000
        for i in range(1, max):
            data = "{}\n line {} of {}: {}".format( data, i, max,  "This is a dummy data")
        data = "{}\n-----End of record at {}-----".format(data,str(datetime.now()))
        return data


class MockKinesisProducerLocust(Locust):
    task_set = MockKinesisProducer
    min_wait = 5000
    max_wait = 15000
