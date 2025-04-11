import time
from kafka import KafkaProducer
from pathlib import Path

path = Path().absolute()

file_path = path / 'data/Online_Retail.csv'
file_path_alt = path.parent / 'data/Online_Retail_2.csv'
## Kafka broker has to be running on localhost:9092

kafka_broker_address = 'localhost:9092'
topic = 'abgabe-topic'

#Sends data to the Kafka message queue
def start_server ():
    #I have had inconstenties with the file path, this should fix it
    try:
        filetosend = open(file_path, "r")
    except:
        filetosend = open(file_path_alt, "r")
    
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
