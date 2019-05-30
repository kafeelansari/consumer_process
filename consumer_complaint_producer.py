from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient,NewTopic
import sys
from time import sleep
import re


if __name__=='__main__':
    if len(sys.argv) !=4:
        print("Usage: consumer_complaint_producer.py <broker_list> <topic> <file_to_read>")
        exit(0)
    else:
        topic_response_re = "error_code=[^\s]"
        broker_list = sys.argv[1]
        topic = sys.argv[2]
        file_path = sys.argv[3]
        producer = KafkaProducer(bootstrap_servers=broker_list)
        file = open(file_path,'r')
        for line in file.readlines():
            producer.send(topic=topic,value=str.encode(line))
            sleep(2)
        producer.close()
