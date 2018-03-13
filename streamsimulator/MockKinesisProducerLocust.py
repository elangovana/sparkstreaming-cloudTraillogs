import sys

import time
from locust import Locust
import json
from datetime import datetime
import uuid
from locust import TaskSet, task
from boto import kinesis
import random





class MockKinesisProducer(TaskSet):

    def __init__(self):
        self.config = self.get_config()


    def get_kinesis_stream_name(self):
        return self.config["kinesis_stream_name"]

    def get_region(self):
        return self.config["region"]


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
        region = self.get_region()
        stream_name = self.get_region()
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
            data = self.get_mock_data(israndom= False)
            self.put_record_in_stream(conn, stream_name, data)
            data = self.get_mock_data(israndom=True)
            self.put_record_in_stream(conn, stream_name, data)
        except Exception as error:
            print('{}'.format(error))
            sys.exit(1)

    def get_mock_data(self, israndom = True):
        ip = "205.251.233.182"
        if israndom:
            ip = "{}.251.233.{}".format(random.randrange(1,255), random.randrange(1,255))

        json_data = {
 "version": "0",
 "id": "708e34ca-87f9-61b4-ba45-fe562ee0902d",
 "detail-type": "AWS API Call via CloudTrail",
 "source": "aws.s3",
 "account": "157997839279",
 "time": "2018-03-13T04:00:26Z",
 "region": "us-east-1",
 "resources": [],
 "detail": {
   "eventVersion": "1.05",
   "userIdentity": {
     "type": "AssumedRole",
     "principalId": "AROAJW723CZNOTI6KYLXG:AWSFirehoseToS3",
     "arn": "arn:aws:sts::157997839279:assumed-role/firehose_delivery_to_S3_role/AWSFirehoseToS3",
     "accountId": "157997839279",
     "accessKeyId": "ASIAIXVLJFYEYFKQMVYQ",
     "sessionContext": {
       "sessionIssuer": {
         "type": "Role",
         "principalId": "AROAJW723CZNOTI6KYLXG",
         "arn": "arn:aws:iam::157997839279:role/firehose_delivery_to_S3_role",
         "accountId": "157997839279",
         "userName": "firehose_delivery_to_S3_role"
       },
       "attributes": {
         "creationDate": "2018-03-13T03:28:55Z",
         "mfaAuthenticated": "false"
       }
     },
     "invokedBy": "AWS Internal"
   },
   "eventTime": "2018-03-13T04:00:26Z",
   "eventSource": "s3.amazonaws.com",
   "eventName": "PutObject",
   "awsRegion": "us-east-1",
   "sourceIPAddress": ip,
   "userAgent": "[aws-internal/3]",
   "requestParameters": {
     "x-amz-acl": "bucket-owner-full-control",
     "bucketName": "ganesraj-structured-demo",
     "key": "rawdata/2018/03/13/03/StructredS3-1-2018-03-13-03-55-25-680033e1-eb0b-4325-a7fe-126fd0985b79"
   },
   "responseElements": "",
   "additionalEventData": {
     "x-amz-id-2": "hqnAk1J8Osh197y2aNBq7Q7KXuKY/aoxKEYvcRz8efayO8XLRV7wp1Zcl4hcsqm2gNEKxxsEifk="
   },
   "requestID": "D58027E774E8DB84",
   "eventID": "5ff8dee2-6578-4468-a0e0-1c651a1f2a09",
   "readOnly": False,
   "resources": [
     {
       "type": "AWS::S3::Object",
       "ARN": "arn:aws:s3:::ganesraj-structured-demo/rawdata/2018/03/13/03/StructredS3-1-2018-03-13-03-55-25-680033e1-eb0b-4325-a7fe-126fd0985b79"
     },
     {
       "accountId": "157997839279",
       "type": "AWS::S3::Bucket",
       "ARN": "arn:aws:s3:::ganesraj-structured-demo"
     }
   ],
   "eventType": "AwsApiCall",
   "recipientAccountId": "157997839279"
 }
}
        return json.dumps(json_data)


class MockKinesisProducerLocust(Locust):
    task_set = MockKinesisProducer
    min_wait = 5000
    max_wait = 15000
