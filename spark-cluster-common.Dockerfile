####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM openjdk:8

RUN apt update && \
    apt upgrade --yes && \
    apt install ssh openssh-server --yes

# Setup common SSH key.
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/shared_rsa -C common && \
    cat ~/.ssh/shared_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Setup HDFS/Spark resources here
RUN apt-get update && \
    apt-get install wget && \
    apt-get install scala --yes && \
    apt-get install rsync --yes

RUN wget https://www.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzvf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /usr/local/hadoop && \
    rm hadoop-3.3.6.tar.gz
RUN wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz && \
    tar -xzvf spark-3.4.1-bin-hadoop3.tgz && \
    mv spark-3.4.1-bin-hadoop3 /usr/local/spark && \
    rm spark-3.4.1-bin-hadoop3.tgz

# Install Python 3.9 directly from Debian Bullseye repository
RUN apt-get update && \
    apt-get install -y python3.9 python3.9-venv python3.9-distutils python3-pip && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1

# Install Kafka
RUN wget https://archive.apache.org/dist/kafka/3.5.0/kafka_2.12-3.5.0.tgz && \
    tar -xzvf kafka_2.12-3.5.0.tgz && \
    mv kafka_2.12-3.5.0 /usr/local/kafka && \
    rm kafka_2.12-3.5.0.tgz

# Install PySpark and Kafka Python dependencies
RUN python3 -m pip install --upgrade pip && \
    pip install pyspark==3.4.1 kafka-python

ENV KAFKA_HOME=/usr/local/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin
ENV HADOOP_HOME=/usr/local/hadoop 
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:/usr/local/spark/bin
ENV SPARK_MASTER="spark://main:7077"
ENV SPARK_MASTER_HOST=main
ENV SPARK_MASTER_PORT=7077
ENV KAFKA_BROKER_ID=1
ENV KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
ENV KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092

RUN mkdir $HADOOP_HOME/logs

COPY config/* /tmp/

RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    mv /tmp/spark-env.sh $SPARK_HOME/conf/spark-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml && \ 
    mv /tmp/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml && \
    mv /tmp/workers $HADOOP_HOME/etc/hadoop/workers && \
    mv /tmp/slaves $SPARK_HOME/conf/slaves && \
    mv /tmp/start-hadoop.sh ~/start-hadoop.sh && \
    mv /tmp/start-spark.sh ~/start-spark.sh

RUN chmod +x ~/start-hadoop.sh && \
    chmod +x ~/start-spark.sh && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh