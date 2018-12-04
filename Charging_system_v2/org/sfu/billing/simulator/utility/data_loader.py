import csv, random, string
import yaml, sys
from definitions import CELL_TOWER_LOCATIONS, CUSTOMER_LIST
assert sys.version_info >= (3, 5)

#Creates a customer instance with a constructor;
#Parameters: ID(alpha numeric), customer's phone number, receiver's phone number, location of the caller, location of the receiver
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

#loads cell tower locations from csv file; returns a list of latitude and longitude
def load_locations():
    locations_list = dict()
    with open(CELL_TOWER_LOCATIONS, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            locations_list[line_count] = row['nodeId']
            line_count += 1
    return locations_list,line_count

#1. Creates empty customer list
#2. Loads cell tower location and location count
#3. Loads customer list csv file
#4. Generates random index using the location count as range
#5. Selects source and destination location (latitude and longitude)
#6. Creates an alpha numeric value of length 8 and adds the linecount as the last two character ie. customer_id
#7. Creates a customer instance using the values created in above steps and stores it in a dictionary and returns it

def load_customer():
    customer_list = dict()
    locations,location_count = load_locations()
    with open(CUSTOMER_LIST, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            sindex = random.randint(0,location_count-1)
            source = locations[sindex]
            dindex = random.randint(0,location_count-1)
            destination = locations[dindex]
            alpa_id = ''.join(random.choice(string.ascii_uppercase) for _ in range(8))
            customer_list[line_count] = Customer(alpa_id + str("%02d" % (line_count,)), row["caller"], row["reciever"],
                                                 source,destination)
            line_count += 1
    return customer_list

#loads yaml configuration file
def load_yaml(filepath):
    "Loads a yaml file"
    with open(filepath, "r") as file_descriptor:
        data = yaml.load(file_descriptor)
    return data

#dumps yaml configuration file
def dump_yaml(filepath, data):
    "Dumps data"
    with open(filepath, "w") as file_descriptor:
        yaml.dump(data, file_descriptor)
