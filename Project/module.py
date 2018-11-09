
import random,time
import yaml_loader as yam
import csv


class Customer:
    'Common base class for all customer'
    customer_count = 0

    def __init__(self, id, phone, email, location):
        self.id = id
        self.phone = phone
        self.email = email
        self.location = location
        Customer.customer_count += 1

    def displayCount(self):
        print("Total Customer" + str(Customer.customer_count))

    def displayCustomer(self):
        print("ID : " + str(self.id) + " Phone: " + str(self.phone))

def strTimeProp(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def randomDate(start, end, prop):
    return strTimeProp(start, end, '%m/%d/%Y %I:%M %p', prop)

def load_customer():
    customer_list = dict()
    with open('customer_list.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            customer_list[line_count] = Customer(line_count, row["phone"], row["email"], row["location"])
            line_count += 1
    return customer_list

def generate():
    # filepath = "config.yaml"
    # data = yam.loader(filepath)
    # length = data.get('RECORDS')
    length = 2
    data = load_customer()
    msg = ''
    for i in range(1,length):
        for j in data:
            date = str(random.randint(1,12)) + "/" + str(random.randint(1,31))+"/2018 "
            hour = random.randint(1,24)
            minute = random.randint(1,60)
            second = random.randint(1,60)
            start_time = str(hour) + ":" + str(minute)+":" + str(second)
            end_time =  str(hour) + ":" + str(minute+random.randint(1,second+10))+":" + str(random.randint(1,second+10))
            msg += str(data[j].id) + ", " \
                   + str(data[j].phone)+ ", " \
                   + str(data[j].email) + ", " \
                   + str(data[j].location) + ", " \
                   + str("$"+str(round(random.uniform(0,20),2))) + ", " \
                   + date + start_time + ", " \
                   + date + end_time + '\n'
    #print(msg)
    return msg
