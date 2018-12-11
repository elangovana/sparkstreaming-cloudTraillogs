## Prerequisites
##### Spark 2.2
###### TODO..Dynamodb tables, Additional Streams, lambda functions to deploy
1. See [deploymentscript.py](deploymentscript.py)

## Steps to run the spark application
1. Setup codebuild to build using the [buildspec.yml](buildspec.yml)


2. Download the latest build. This example copies the 

    ```bash
    export s3_build_artifacts=s3://mubucket/builds/ 
    # Get the latest builds to find the one you want t
    aws s3 ls $s3_build_artifacts --recursive | sort | tail -1
 
    mkdir buildartifacts
    
    aws s3 cp $s3_build_artifacts/<path>/SparkApplicationBuildArtifacts.zip ~/buildartifacts

    ```


3. Extract the Set up files 

    ```bash
    unzip ~/buildartifacts/SparkApplicationBuildArtifacts.zip -d deployfiles
    
    unzip deployfiles/sparkapplication.zip -d deployfiles/sparkapplication
    ```

4. Submit spark job
    ```bash
    cd deployfiles
    spark-submit  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0 --archives boto3.zip#boto3,botocore.zip#botocore,sparkapplication.zip#sparkapplication  main.py mydemoapp20180302 CloudTrailEventStream https://kinesis.us-east-1.amazonaws.com us-east-1 AnomalyEventStream 
    ```
   

## Set up mock stream simulator

1. Install locust
    pip install locust --user

2. Do this in a separate session or run as background task
    ```bash
        cd deployfiles/streamsimulator/
     ```
     
3. *** Edit config.json for the streamname to write to.

4. Run locust
    ```bash
    locust -f MockKinesisProducerLocust.py --no-web -c 10 -r 1
    nohup locust -f MockKinesisProducerLocust.py --no-web -c 10 -r 1 &
    ```
