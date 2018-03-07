from locust import Locust


from locust import TaskSet, task


class MockKinesisProducer(TaskSet):

    @task
    def put_data(self):
        # kinesis = kinesis.connect_to_region("eu-west-1")
        print("Hi.....")


class MockKinesisProducerLocust(Locust):
    task_set = MockKinesisProducer
    min_wait = 5000
    max_wait = 15000