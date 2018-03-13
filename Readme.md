## Prerequisites
##### Spark 2.2
###### TODO..Dynamodb tables, Additional Streams, lambda functions to deploy

## Steps to run
#####List the latest builds
aws s3 ls s3://aegovan-spark/builds/ --recursive | sort | tail -1

#####Download the latest build

mkdir buildartifacts

aws s3 cp s3://aegovan-spark/builds/<path>/SparkApplicationBuildArtifacts.zip ~/buildartifacts

#####Set up files 

unzip ~/buildartifacts/SparkApplicationBuildArtifacts.zip -d deployfiles

unzip deployfiles/sparkapplication.zip -d deployfiles/sparkapplication

#####Set up mock stream simulator

pip install locust --user

######Do this in a separate session or run as background task

cd deployfiles/streamsimulator/
#########Edit config.json for the streamname to write to
locust -f MockKinesisProducerLocust.py --no-web -c 10 -r 1
nohup locust -f MockKinesisProducerLocust.py --no-web -c 10 -r 1 &

#####Start spark job
cd deployfiles
spark-submit  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0 --archives boto3.zip#boto3,botocore.zip#botocore,boto.zip#boto,sparkapplication.zip#sparkapplication  main.py mydemoapp20180302 CloudTrailEventStream https://kinesis.us-east-1.amazonaws.com us-east-1 AnomalyEventStream ReproducedCloudTrailEventStream 
