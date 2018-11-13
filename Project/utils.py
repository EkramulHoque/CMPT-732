import csv, random, string
import yaml, sys

assert sys.version_info >= (3, 5)


class Customer:
    'Common base class for all customer'
    customer_count = 0

    def __init__(self, id, caller, receiver, origin, destination):
        self.id = id
        self.caller = caller
        self.receiver = receiver
        self.origin = origin
        self.destination = destination
        Customer.customer_count += 1

    def displayCount(self):
        print("Total Customer" + str(Customer.customer_count))

    def displayCustomer(self):
        print("ID : " + str(self.id) + " Phone: " + str(self.caller))


def load_customer():
    customer_list = dict()
    with open('customer_list.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            alpa_id = ''.join(random.choice(string.ascii_uppercase) for _ in range(8))
            customer_list[line_count] = Customer(alpa_id + str("%02d" % (line_count,)), row["caller"], row["reciever"],
                                                 str(round(random.uniform(49.0, 50.0),4))+"|"+str(round(random.uniform(-122.0, -123.0),4)),
                                                 str(round(random.uniform(49.0, 50.0),4))+"|"+str(round(random.uniform(-122.0, -123.0),4)))
            line_count += 1
    return customer_list


def load_yaml(filepath):
    "Loads a yaml file"
    with open(filepath, "r") as file_descriptor:
        data = yaml.load(file_descriptor)
    return data


def dump_yaml(filepath, data):
    "Dumps data"
    with open(filepath, "w") as file_descriptor:
        yaml.dump(data, file_descriptor)
