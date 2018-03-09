spark-submit  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.1.2 main.py myStream myStream https://kinesis.us-east-1.amazonaws.com us-east-1

# Kick off locut to data inot kinese
locust -f MockKinesisProducerLocust.py --no-web -c 1 -r 1
