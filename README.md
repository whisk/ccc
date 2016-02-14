# Codename CCC

Codename CCC-2016

This manual is for Ubuntu 14 LTS.

## Installing Hadoop 2.6/2.7

Sample configuration for 1 master node and 2 slave nodes. You can add more slaves if you wish.

### Configuring master node

Create a node. Rename it to `hadoop-master`:

    > sudo hostname hadoop-master
    > sudo echo -n "hadoop-master" > /etc/hostname

Open `/etc/hosts` and add Master IP (for AWS EC2 and Google Cloud **it must be** internal IP):

    # hadoop cluster
    <NODE IP> hadoop-master

Reboot. 

Install and setup Java:

    > sudo add-apt-repository ppa:webupd8team/java
    > sudo apt-get update
    > sudo apt-get install oracle-java8-installer

Edit `/etc/environment` and add:

    JAVA_HOME="/usr/lib/jvm/java-8-oracle/"

Create separate user and group for running and managing Hadoop:

    > sudo addgroup hadoop
    > sudo adduser -ingroup hadoop --disabled-password hduser

Grant `sudo` to `hduser` without password. Create new sudoers file using `visudo` (always use `visudo` to edit sudoers!):

    > sudo visudo -f /etc/sudoers.d/hduser

And paste:

    hduser ALL=(ALL) NOPASSWD: ALL

Login as `hduser` and generate SSH key:

    > sudo su hduser
    > ssh-keygen -t rsa -P ""
    > cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

Check SSH:

    > ssh hduser@localhost

Login as `hduser`. Install Hadoop (actual link may change — see <http://hadoop.apache.org/#Download+Hadoop>):

    > wget http://apache-mirror.rbc.ru/pub/apache/hadoop/common/hadoop-2.6.3/hadoop-2.6.3.tar.gz
    > tax xvf hadoop-2.6.3.tar.gz

Move Hadoop files to system-wide dir and symlink currently installed Hadoop version to just `hadoop`:

    > sudo mv hadoop-2.6.3 /usr/local/
    > cd /usr/local
    > ln -s hadoop-2.6.3 hadoop

Edit `/etc/bash.bashrc` and add following lines:

    # hadoop
    export HADOOP_INSTALL=/usr/local/hadoop
    export PATH=$PATH:$HADOOP_INSTALL/bin
    export PATH=$PATH:$HADOOP_INSTALL/sbin
    export HADOOP_MAPRED_HOME=$HADOOP_INSTALL
    export HADOOP_COMMON_HOME=$HADOOP_INSTALL
    export HADOOP_HDFS_HOME=$HADOOP_INSTALL
    export HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop
    export YARN_HOME=$HADOOP_INSTALL

Now configure Hadoop itself. Create `masters` file:

    cd /usr/local/hadoop/etc/hadoop
    > echo -n "hadoop-master" > masters

Edit `core-site.xml`. Add `fs.default.name` (or replace existing value) with master's hostname:

    <property>
        <name>fs.default.name</name>
        <value>hdfs://hadoop-master:9000</value>
    </property>

Edit `hdfs-site.xml`. Set replication factor to 3:

    <property>
      <name>dfs.replication</name>
      <value>3</value>
    </property>

Edit `yarn-site.xml`. Add:

    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>hadoop-master:8025</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>hadoop-master:8035</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>hadoop-master:8050</value>
    </property>

Create `mapred-site.xml` from defaults:

    > cp mapred-site.xml.template mapred-site.xml

Add:

    <property>
        <name>mapreduce.job.tracker</name>
        <value>hadoop-master:5431</value>
    </property>
    <property>
        <name>mapred.framework.name</name>
        <value>yarn</value>
    </property>

### Configuring slaves

Rename each slave as `hadoop-slave-N`:

    > sudo hostname hadoop-master
    > sudo echo -n "hadoop-master" > /etc/hostname

And reboot. 

Add slaves IPs to `/etc/hosts` on Master and all slaves (for AWS EC2 and Google Cloud **it must be** internal IP) right after master's IP:

    <Master IP>  hadoop-master  # already added
    <Slave 1 IP> hadoop-slave-1
    <Slave 2 IP> hadoop-slave-2

On Master node, add slaves hostnames to `slaves` files:

    > cd /usr/local/hadoop/etc/hadoop
    > vim slaves

Add lines:

    hadoop-slave-1
    hadoop-slave-2

Now transfer this file to all slaves. You may do it using `scp` and bash loop:

    > cd /usr/local/hadoop/etc/hadoop
    > for i in 1 2; do scp slaves hadoop-slave-$i:`pwd`/ ; done

You might be asked to confirm host identity. Type `yes` every time.

### Starting HDFS and YARN

On Master:

    > start-dfs.sh

Format namenode (**only on first start**):

    > hdfs namenode -format

Check processes by typing `jps`. It should diplay something like:

    5872 NameNode
    6038 DataNode
    6236 SecondaryNameNode
    6361 Jps

Note that `NameNode`, `DataNode` and `SecondaryNameNode` must be present. If not, there is something wrong.

Start YARN:

    > start-yarn.sh

and run `jps`. Note following processed were added:

    6592 ResourceManager
    6735 NodeManager

Finally check HDFS by putting and reading a file from it:

    > echo "hdfs works!" > test.txt
    > hdfs dfs -put test.txt /
    > hdfs dfs -cat /test.txt

It should display `hdfs works!`. No errors or extra messages should be shown.

## Installing Spark 1.6 with HDFS support

Login as `hduser` (created previously for Hadoop) on `hadoop-master`.

Go to <http://spark.apache.org/downloads.html> and select:

* Spark release: 1.6.x
* Package Type: Pre-built for Hadoop 2.6 and later
* Download type: _doesn't matter_

Get download link, download and untar:

    > wget http://apache-mirror.rbc.ru/pub/apache/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz
    > tar xvf spark-1.6.0-bin-hadoop2.6.tgz

Move Spark files to system-wide dir and symlink currently installed Spark version to just `spark`:

    > sudo mv spark-1.6.0-bin-hadoop2.6 /usr/local
    > cd /usr/local
    > sudo ln -s spark-1.6.0-bin-hadoop2.6 spark

Edit `/etc/bash.bashrc` and add following lines:

    # SPARK
    export SPARK_INSTALL=/usr/local/spark
    export PATH=$PATH:$SPARK_INSTALL/bin
    export PATH=$PATH:$SPARK_INSTALL/sbin
    export PYSPARK_PYTHON=python3.4

Repeat for all slave nodes.

### Pyspark preparations

Run on all nodes (master and slaves):

>   sudo apt-get install build-essential python3-dev python3-pip
>   sudo pip3 install hdfs

## Installing Cassandra 3.0

Assume you have 2 nodes. Repeat following steps for all nodes.

Install Java.

    > curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -
    > sudo sh -c 'echo "deb http://debian.datastax.com/community/ stable main" >>  /etc/apt/sources.list.d/datastax.list'
    > sudo add-apt-repository ppa:webupd8team/java
    > sudo apt-get update
    > sudo apt-get install oracle-java8-set-default

    # check java installation
    > java -version
    java version "1.8.0_72"

Now install Cassandra:

    > sudo apt-get install dsc30 
    
Edit `/etc/cassandra/cassandra.yaml` and change value of following variables:

* `cluster_name` - set to arbitrary value, the same on each node
* `seed_provider > parameters > seeds` — set to list of nodes IPs/hostnames: "<Node 1>,<Node 2>"
* `listen_address` — set to current node IP/hostname

Fix startup script: open `/etc/init.d/cassandra` and replace `CMD_PATT="Dcassandra-pidfile=.*cassandra\.pid"` with `CMD_PATT="cassandra"`

Finally start Cassandra:

    > sudo service cassandra start

Check that Cassandra works properly and can be connected to:

    > cqlsh <cassandra host>

It should display something like this and show prompt:

    Connected to MyCluster at cassandra-1:9042.
    [cqlsh 5.0.1 | Cassandra 3.0.3 | CQL spec 3.4.0 | Native protocol v4]
    Use HELP for help.
    cqlsh> 

### Install Datastax Python driver for Cassandra 3.0

    > sudo apt-get install build-essential python3-dev python3-pip libev4 libev-dev
    > sudo pip3 install cassandra-driver

## Cassandra Scheme for CCC

    CREATE KEYSPACE ccc_1 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}  
        AND durable_writes = true;

    CREATE TABLE ccc_1.airport_popularity (
        airport ascii PRIMARY KEY,
        popularity int
    )

    CREATE TABLE ccc_1.top_carriers_by_origin (
        origin ascii PRIMARY KEY,
        carriers text
    ) 

    CREATE TABLE ccc_1.top_destinations_by_origin (
        origin ascii PRIMARY KEY,
        destinations text
    )

    CREATE TABLE ccc_1.top_carriers_by_route (
        route ascii PRIMARY KEY,
        carriers text
    )

    CREATE TABLE ccc_1.arrival_delay_by_route (
        route ascii PRIMARY KEY,
        arrival_delay float
    )

    CREATE TABLE ccc_1.trips (
        origin ascii,
        destination ascii,
        departure_date ascii,
        departure_time int,
        departure_delay int,
        PRIMARY KEY (origin, destination, departure_date, departure_time)
    );