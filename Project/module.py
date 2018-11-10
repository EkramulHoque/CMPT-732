
import random,time
import yaml_loader as yam
import csv
from datetime import datetime, timedelta
import calendar

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

def month_converter(month):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    return months.index(month) + 1


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


def load_customer():
    customer_list = dict()
    with open('customer_list.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            customer_list[line_count] = Customer(line_count, row["caller"], row["reciever"], row["origin"],row["destination"])
            line_count += 1
    return customer_list


def check_event(event):
    events = []
    if (event == 'Data'):
        events['data'] = True
    elif (event == 'Voice'):
        events['voice'] = True
    else:
        events['voice'] = events['data'] = False
    return events

def generate():
    filepath = "config.yaml"
    data = yam.loader(filepath)
    length = data.get('RECORDS')


    # event_type = data.get('EVENT_TYPE')
    # if event_type == 'Data':
    #     event_type = 1
    # else:
    #     event_type = 0

    date = data.get('MONTH')
    start = month_converter(date[0])

    if len(date) > 1:
        end = month_converter(date[1])
    else:
        end = start

    dts = [dt.strftime('%Y%m%d %H%M%S') for dt in
           datetime_range(datetime(2018, start, 1, random.randint(0, 23)), datetime(2018, end, 31, random.randint(0, 23)),
                          timedelta(minutes=random.randint(1, 60)))]
    data = load_customer()
    msg = ''
    for i in range(1,length):
        for j in data:
            interval = random.randint(0,len(dts)-2)
            msg += str(data[j].id) + ", " \
                   + dts[interval]+ ", " \
                   + dts[interval+1] + ", " \
                   + str(data[j].origin) + ", " \
                   + str(data[j].destination) + ", " \
                   + str(data[j].caller)+ ", " \
                   + str(data[j].receiver) + ", " \
                   + str(random.randint(0,1)) + ", " \
                   + str(random.randint(0,1)) + '\n'
    #print(msg)
    return msg

if __name__ == "__main__":
    generate()



