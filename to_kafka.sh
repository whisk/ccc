#/bin/bash

DATASET_DIR="/dataset_clean"
BANDWIDTH=100 # Mbps
TOPIC="ccc_1"
PARTITIONS=1
REPLICATION_FACTOR=1
ZOOKEEPER="kafka-1:2181"
BROKERS="kafka-1:9092"

# delete topic
kafka-topics.sh --zookeeper $ZOOKEEPER --topic $TOPIC --delete

# create again
kafka-topics.sh --zookeeper $ZOOKEEPER --topic $TOPIC --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --create

# now produce
for fname in $DATASET_DIR/*.txt; do
	echo "$fname -> kafka"
	cat $fname | kafka-console-producer.sh --broker-list $BROKERS --topic $TOPIC &
	size=`stat --format "%s" $fname`
	sleeptime=`echo "scale=2;$size/$BANDWIDTH/1024/1024*8" | bc`
	sleep $sleeptime
done
