import threading,logging,time,json,sys
assert sys.version_info >= (3, 5)
from kafka import KafkaConsumer, KafkaProducer

json_file = [
   {
      "_id": "5be0a0504817b660eef107f2",
      "CustID": 0,
      "balance": "$11.53",
      "phone": "+1 (825) 454-2369",
      "Location": "Ontario",
      "Start": "2018-10-12T05:45:24",
      "End": "2018-10-12T05:56:42",
      "Event": {
         "Voice": True,
         "Data": False
      }
   },
   {
      "_id": "5be0a050faa7b8165c643095",
      "CustID": 1,
      "balance": "$3.38",
      "phone": "+1 (830) 572-3479",
      "Location": "Manitoba",
      "Start": "2018-06-06T12:05:25",
      "End": "2018-06-06T12:08:45",
      "Event": {
         "Voice": False,
         "Data": True
      }
   },
   {
      "_id": "5be0a050e4fb083e93386167",
      "CustID": 2,
      "balance": "$4.46",
      "phone": "+1 (929) 415-2591",
      "Location": "Manitoba",
      "Start": "2018-11-08T08:06:23",
      "End": "2018-11-08T08:10:23",
      "Event": {
         "Voice": True,
         "Data": False
      }
   },
   {
      "_id": "5be0a05083ec9d4ba18f9d2a",
      "CustID": 3,
      "balance": "$2.36",
      "phone": "+1 (854) 544-3144",
      "Location": "British Columbia",
      "Start": "2018-04-18T04:47:55",
      "End": "2018-04-18T04:49:55",
      "Event": {
         "Voice": True,
         "Data": False
      }
   },
   {
      "_id": "5be0a050bb367e94041f19b6",
      "CustID": 4,
      "balance": "$7.34",
      "phone": "+1 (971) 416-2964",
      "Location": "British Columbia",
      "Start": "2018-06-24T02:50:03",
      "End": "2018-06-24T02:57:57",
      "Event": {
         "Voice": True,
         "Data": False
      }
   },
   {
      "_id": "5be0a0505e872429b7b037ca",
      "CustID": 5,
      "balance": "$10.06",
      "phone": "+1 (993) 411-3848",
      "Location": "Alberta",
      "Start": "2018-12-04T02:49:03",
      "End": "2018-12-04T02:59:03",
      "Event": {
         "Voice": True,
         "Data": False
      }
   }
]


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers=['199.60.17.210:9092', '199.60.17.193:9092'],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            producer.send('cdr-10', json_file)
            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=['199.60.17.210:9092', '199.60.17.193:9092'],
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['cdr-10'])
        for message in consumer:
            print (message)


def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()