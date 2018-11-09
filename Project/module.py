from json import dumps
from faker import Faker
import random
import collections
import yaml_loader as yam
import json


def fake_person_generator(length, fake):

    for x in range(length):  # xrange in Python 2.7
        yield {'CustID': x,
               'last_name': fake.last_name(),
               'first_name': fake.first_name(),
               'phone_number': fake.phone_number(),
               'email': fake.email(),
               'balance': "$"+str(round(random.uniform(0,100),2)),
               'event': {"Voice": bool(random.getrandbits(1)), "Data": bool(random.getrandbits(1))}
               }

def main():
    filepath = "config.yaml"
    data = yam.loader(filepath)
    database = []
    length = data.get('RECORDS')
    filename = 'cdr' + str(length)
    fake = Faker()  # <--- Forgot this
    fpg = fake_person_generator(length, fake)
    with open('%s.json' % filename, 'w') as output:
        output.write('[')  # to made json file valid according to JSON format
        for person in fpg:
            json.dump(person, output)
        output.write(']')  # to made json file valid according to JSON format
    print("Done")

if __name__ == "__main__":
    main()