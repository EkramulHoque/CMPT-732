#!/usr/bin/python3
#Mehdi Lebdi

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, json, datetime
import os
import gzip
from cassandra.cluster import Cluster
# add more functions as necessary

def split_input(input_line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    line_split = line_re.split(input_line.replace("'","_"))
    return line_split

def main(input_dir, keyspace, table_name):
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(keyspace)

    session.execute("""
        CREATE TABLE IF NOT EXISTS nasalogs (
            host TEXT,
            id uuid,
            datetime TIMESTAMP,
            path TEXT,
            bytes INT,
            PRIMARY KEY (host, id)
        )
        """)

    session.execute("""TRUNCATE nasalogs;""")

    for f in os.listdir(input_dir):
        query = "BEGIN BATCH "
        counter = 0
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                split_line = split_input(line)
                if len(split_line) > 4:
                    query = query + "INSERT INTO " + table_name + " (id, host, datetime, path, bytes) " + \
                        "VALUES (UUID(), '" + split_line[1] + "', '" + \
                        datetime.datetime.strptime(split_line[2], '%d/%b/%Y:%H:%M:%S').isoformat() + "', '" + \
                        split_line[3] + "', " + split_line[4] + ");"
                    counter += 1
                    if counter >= 300:
                        query = query + "APPLY BATCH;"
                        session.execute(query)
                        query = "BEGIN BATCH "
                        counter = 0

            query = query + "APPLY BATCH;"
            session.execute(query)
    cluster.shutdown()

if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_dir, keyspace, table_name)