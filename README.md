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
