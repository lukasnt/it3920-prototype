FROM apache/spark:v3.2.2 AS spark-latest

FROM ldbc/datagen-jar:latest AS ldbc-snb-datagen-jar

FROM ubuntu:20.04 AS base
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y git-all wget curl maven python3 python3-pip

FROM base AS spark-v2_4_0-download
RUN wget https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz && \
    tar -xvf spark-2.4.0-bin-hadoop2.7.tgz && \
    mv spark-2.4.0-bin-hadoop2.7 /opt/spark && \
    rm spark-2.4.0-bin-hadoop2.7.tgz

FROM base AS zeppelin-download
RUN wget https://dlcdn.apache.org/zeppelin/zeppelin-0.10.1/zeppelin-0.10.1-bin-all.tgz && \
    tar -xvf zeppelin-0.10.1-bin-all.tgz && \
    mv zeppelin-0.10.1-bin-all /opt/zeppelin && \
    rm zeppelin-0.10.1-bin-all.tgz

FROM base AS hadoop-download
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-3.0.0/hadoop-3.0.0.tar.gz && \
    tar -xvf hadoop-3.0.0.tar.gz && \
    mv hadoop-3.0.0 /opt/hadoop && \
    rm hadoop-3.0.0.tar.gz

FROM base AS openjdk
RUN apt-get install -y openjdk-8-jdk
RUN find /usr/lib/jvm -name "java-8-openjdk-*" | xargs -I {} mv {} /usr/lib/jvm/java-8-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

FROM openjdk AS spark-v2_4_0
COPY --from=spark-v2_4_0-download /opt/spark /opt/spark

FROM openjdk AS hadoop
COPY --from=hadoop-download /opt/hadoop /opt/hadoop
COPY hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
RUN hdfs namenode -format

FROM openjdk AS mvn-install
COPY pom.xml /usr/home/spark-graphx-scala/pom.xml
RUN cd /usr/home/spark-graphx-scala && mvn install

FROM mvn-install AS mvn-package
COPY /src /usr/home/spark-graphx-scala/src
RUN cd /usr/home/spark-graphx-scala && mvn clean package

FROM base AS ldbc-snb-datagen-env
RUN git clone https://github.com/ldbc/ldbc_snb_datagen_spark.git && \
    cd ldbc_snb_datagen_spark && \
    pip install -U pip && \
    pip install ./tools

FROM ldbc-snb-datagen-env AS ldbc-snb-datagen
COPY --from=spark-latest /opt/spark /opt/spark
COPY --from=openjdk /usr/lib/jvm/java-8-openjdk /lib/jvm/java-8-openjdk
COPY --from=ldbc-snb-datagen-jar /jar /ldbc-datagen.jar
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/lib/jvm/java-8-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PLATFORM_VERSION=3.2.2
ENV DATAGEN_VERSION=0.5.1
ENV LDBC_SNB_DATAGEN_JAR=/ldbc-datagen.jar


FROM base
COPY --from=zeppelin-download /opt/zeppelin /opt/zeppelin
COPY --from=spark-v2_4_0-download /opt/spark /opt/zeppelin/spark
COPY --from=openjdk /usr/lib/jvm/java-8-openjdk /lib/jvm/java-8-openjdk
COPY --from=mvn-package /usr/home/spark-graphx-scala/target /opt/zeppelin/target
COPY zeppelin-site.xml /opt/zeppelin/conf/zeppelin-site.xml
ENV JAVA_HOME=/lib/jvm/java-8-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH
WORKDIR /opt/zeppelin
CMD ["printenv"]