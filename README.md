# Cloud Computing Capstone

Cloud Computing Capstone 2016

## Installing Cassandra on Ubuntu:

    > curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -
    > sudo sh -c 'echo "deb http://debian.datastax.com/community/ stable main" >>  /etc/apt/sources.list.d/datastax.list'
    > sudo add-apt-repository ppa:webupd8team/java
    > sudo apt-get update
    > sudo apt-get install oracle-java8-set-default
    > java -version
    java version "1.8.0_72"

    > sudo apt-get install dsc30 
    
Edit `/etc/init.d/cassandra`. Replace `CMD_PATT="Dcassandra-pidfile=.*cassandra\.pid"` with `CMD_PATT="cassandra"`

    > sudo service cassandra start

## Schema

    CREATE TABLE ccc_1.trips (
        origin ascii,
        destination ascii,
        departure_date ascii,
        departure_time int,
        departure_delay int,
        PRIMARY KEY (origin, destination, departure_date, departure_time)
    ) WITH CLUSTERING ORDER BY (destination ASC, departure_date ASC, departure_time ASC)
        AND bloom_filter_fp_chance = 0.01
        AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
        AND comment = ''
        AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
        AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
        AND crc_check_chance = 1.0
        AND dclocal_read_repair_chance = 0.1
        AND default_time_to_live = 0
        AND gc_grace_seconds = 864000
        AND max_index_interval = 2048
        AND memtable_flush_period_in_ms = 0
        AND min_index_interval = 128
        AND read_repair_chance = 0.0
        AND speculative_retry = '99PERCENTILE';
