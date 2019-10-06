Basic sentiment analysis of realtime tweets using Apache Kafka - queuing service for data streams. 

Initialization Steps:

Download and extract the twitter data zip file: 

Download the 16M.txt.zip file from here : https://drive.google.com/file/d/1K1ub__1yKOMTSNAp7f_NGiCefT6KjxXM/view

unzip 16M.txt.zip 

Start zookeeper service:  

$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties  

Start kafka service:  

$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties  

Create a topic named twitterstream in kafka:  

$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 -partitions 1 --topic twitterstream  

Check what topics you have with:  

$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper localhost:2181  
 
Using the Streaming API:

In order to stream the tweets and push them to kafka queue, we have provided a python script twitter_to_kafka.py. 

To stream tweets, we will read tweets from a file and push them to the twitterstream topic in Kafka. Do this by running our program as follows:  

$ python twitter_to_kafka.py 
 
Note, this program must be running when you run your portion of the assignment, otherwise you will not get any tweets. 

To check if the data is landing in Kafka:  

$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic twitterstream --frombeginning  
 
Running the Stream Analysis Program (after finishing the project requirements): 

$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0  twitterStream.py  
 
