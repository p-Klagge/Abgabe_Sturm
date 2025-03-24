import time

from kafka import KafkaProducer

## Kafka broker has to be running on localhost:9092

file_path = "/home/student/test/anon_httpd_website4.it-2025-02-17.log"
kafka_broker_address = 'localhost:9092'
topic = 'abgabe-topic'

def start_server ():
    filetosend = open(file_path, "r")
    filetosend.readline() # skip header
    producer = KafkaProducer(bootstrap_servers=[kafka_broker_address])
    for line in filetosend:
       producer.send(topic, value=line.encode("utf-8"))
       print(line)
       time.sleep(0.1)
    producer.flush()
    filetosend.close()


if __name__ == "__main__":
    start_server()
