from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import os, sys, gzip, re, uuid
import datetime


def main(inputs,key_space,table):
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(key_space)
    session.execute("""
            CREATE TABLE IF NOT EXISTS nasalogs (
                id UUID,
                host TEXT,
                datetime TIMESTAMP,
                path TEXT,
                bytes INT,
                PRIMARY KEY (host,id)
            )
            """)
    session.execute("""TRUNCATE nasalogs;""")
    insert_log = session.prepare("INSERT INTO "+ table + " (id,host,datetime,path,bytes) VALUES (?,?,?,?,?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
            count = 0
            for line in logfile:
                word = linex.split(line.replace("'", "_"))
                if len(word) > 4:
                	count += 1
                	batch.add(insert_log,(uuid.uuid1(),word[1],datetime.datetime.strptime(word[2], '%d/%b/%Y:%H:%M:%S'),word[3],int(word[4])))
                if (count == 200):
                	session.execute(batch)
                	count = 0
                	batch.clear()          	  
    session.execute(batch)
    cluster.shutdown()


if __name__ == "__main__":
    inputs = sys.argv[1]
    key_space = sys.argv[2]
    table = sys.argv[3]
    linex = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    main(inputs,key_space,table)