from kafka import KafkaProducer
import datetime, time, random, threading
import module as md


rates = [1, 10, 100]  # messages per second
sample_sigma = 500
fuzz_sigma = 10

def send_at(rate):
    producer = KafkaProducer(bootstrap_servers=['199.60.17.210:9092', '199.60.17.193:9092'])
    topic = 'cdr-' + str(rate)
    interval = 1 / rate
    while True:
        msg = md.generate()
        producer.send(topic, msg.encode('ascii'))
        time.sleep(interval)


if __name__ == "__main__":
    for rate in rates:
        server_thread = threading.Thread(target=send_at, args=(rate,))
        server_thread.setDaemon(True)
        server_thread.start()

    while 1:
        time.sleep(1)
