from kafka import KafkaProducer
import time, threading
from org.src.module.simulator import generate
from org.src.utility.data_loader import load_yaml
from definitions import ROOT_DIR, SERVER, TOPIC, CONFIG_FILE

file_path = ROOT_DIR+CONFIG_FILE
data = load_yaml(file_path)
rates = data.get('RATE')# messages per second

# creates kafka instance with server name mentioned in the definitions.py
# creates topic
# sets frequency of data ingestion
# gets the message from simulator
# sends to kafka
def send_at(rate):
    producer = KafkaProducer(bootstrap_servers=[SERVER])
    topic = TOPIC + str(rate)
    interval = 1 / rate
    while True:
        msg = generate()
        producer.send(topic, msg.encode('ascii'))
        time.sleep(interval)

#uses thread to produce data for each frequency mentioned in the config.yaml
if __name__ == "__main__":
    for rate in rates:
        server_thread = threading.Thread(target=send_at, args=(rate,))
        server_thread.setDaemon(True)
        server_thread.start()

    while 1:
        time.sleep(1)
