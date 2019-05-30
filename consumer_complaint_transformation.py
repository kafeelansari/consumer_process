from pyspark.streaming import StreamingContext
from pyspark import SparkConf,SparkContext
from pyspark.streaming.kafka import KafkaUtils

conf = SparkConf().setAppName("Consumer Complaint Streaming transformation").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("Error")
ssc = StreamingContext(sc,10)
kafkaStream = KafkaUtils.createDirectStream(ssc,['consumer_complaint'],{"metadata.broker.list": 'localhost:9092'})
kafkaStream.saveAsTextFiles("hdfs://localhost:54310/kafka_stream")
ssc.start()
ssc.awaitTermination()


