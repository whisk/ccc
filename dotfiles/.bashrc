# Cloud Computing
# edit lines below
export HADOOP_INSTALL=/usr/local/hadoop
export SPARK_INSTALL=/usr/local/spark
export PYSPARK_PYTHON=python3.4

# do not edit
# hadoop
export JAVA_HOME="/usr/lib/jvm/default-java"
export PATH=$PATH:$HADOOP_INSTALL/bin
export PATH=$PATH:$HADOOP_INSTALL/sbin
export HADOOP_MAPRED_HOME=$HADOOP_INSTALL
export HADOOP_COMMON_HOME=$HADOOP_INSTALL
export HADOOP_HDFS_HOME=$HADOOP_INSTALL
export YARN_HOME=$HADOOP_INSTALL

# spark
export PATH=$PATH:$SPARK_INSTALL/bin
export PATH=$PATH:$SPARK_INSTALL/sbin