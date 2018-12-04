Environment variables:

SPARK_HOME=D:\Softwares\Anaconda3\envs\billingEngine\Lib\site-packages\pyspark
HADOOP_HOME=D:\Softwares\Anaconda3\envs\billingEngine\Lib\site-packages\pyspark

#winutils inside $HADOOP_HOME/bin for windows

#APP_HOME variable is used to identify location where config files are kept
APP_HOME=D:\BillingEngine\Mediation

Launching kafka in windows:

1. Start zookeeper:
bin\windows\zookeeper-server-start.bat config/zookeeper.properties

2. Start kafka server:
bin\windows\kafka-server-start.bat config\server.properties

3. Start producer:(Simulator for project/producer console for local testing)
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic cdr-10

4. Start consumer:
bin\windows\kafka-console-consumer.bat --bootstrap-server 142.58.215.106:9092 --topic cdr-10 --from-beginning
