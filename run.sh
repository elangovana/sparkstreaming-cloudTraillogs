spark-submit  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.1.2 main.py myStream myStream https://kinesis.us-east-1.amazonaws.com us-east-1


#spark-submit  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0 --py-files buildartifacts/dist/jobs.zip,buildartifacts/dist/libs.zip  buildartifacts/dist/main.py cloudtrailmockAppNewSQlAB myStream https://kinesis.us-east-1.amazonaws.com us-east-1



# Kick off locut to data inot kinese
locust -f buildartifacts/dist/MockKinesisProducerLocust.py --no-web -c 1 -r 1

virtualenv demo_env
virtualenv --relocatable  demo_env
source demo_env/bin/activate
PYSPARK_PYTHON=demo_env/bin/python
spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=demo_env/bin/python --master yarn-cluster --archives boto3.zip#boto3,botocore.zip#botocore,boto.zip#boto --py-files buildartifacts/dist/jobs.zip,buildartifacts/dist/libs.zip  buildartifacts/dist/main.py


zip -r demo_env.zip demo_env
source activate demo
spark-submit --master yarn-client --conf spark.pyspark.virtualenv.enabled=true--conf spark.pyspark.virtualenv.type=conda--conf spark.pyspark.virtualenv.requirements=buildartifacts/dist/requirements.txt --conf spark.pyspark.virtualenv.bin.path