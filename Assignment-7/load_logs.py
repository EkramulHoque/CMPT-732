from cassandra.cluster import Cluster
import os, sys, gzip, re
import datetime


def main(inputs,key_space,table):
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(key_space)
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
    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
            count = 0
            cql = "BEGIN BATCH "
            for line in logfile:
                split_log = linex.split(line.replace("'", "_"))
                if len(split_log) > 4:
                    cql = cql + "INSERT INTO "+table+" (id, host, datetime, path, bytes) VALUES (UUID(),'"+ split_log[1]+"','"+datetime.datetime.strptime(split_log[2],'%d/%b/%Y:%H:%M:%S').isoformat()+"', '" +split_log[3]+ "', " +split_log[4]+ ");"
                    count += 1
                    if count >= 300:
                        cql = cql + "APPLY BATCH;"
                        session.execute(cql)
                        count = 0
                        cql = "BEGIN BATCH "
            cql = cql + "APPLY BATCH;"
            session.execute(cql)
    cluster.shutdown()


if __name__ == "__main__":
    inputs = sys.argv[1]
    key_space = sys.argv[2]
    table = sys.argv[3]
    linex = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    main(inputs,key_space,table)