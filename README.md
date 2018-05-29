# GitHub Events Analytics 


## RUN
1. Run Kafka Satori Producer
`java -classpath kafka-satori-producer/kafka-satori-producer.jar tn.bigdata.project.KafkaSatoriProducer`

2. Run Spark Streaming App 
`spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 spark-streaming/app.py` 
