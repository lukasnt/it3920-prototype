FROM apache/spark:v3.2.3 AS spark

FROM ubuntu:20.04 AS base
RUN apt-get update && apt-get install -y wget curl maven

FROM base AS zeppelin
RUN wget https://dlcdn.apache.org/zeppelin/zeppelin-0.10.1/zeppelin-0.10.1-bin-all.tgz && \
    tar -xvf zeppelin-0.10.1-bin-all.tgz && \
    mv zeppelin-0.10.1-bin-all /opt/zeppelin && \
    rm zeppelin-0.10.1-bin-all.tgz

FROM base AS mvn-install
COPY pom.xml /usr/home/spark-graphx-scala/pom.xml
RUN cd /usr/home/spark-graphx-scala && mvn install

FROM base AS openjdk
RUN apt-get install -y openjdk-8-jdk
RUN find /usr/lib/jvm -name "java-8-openjdk-*" | xargs -I {} mv {} /usr/lib/jvm/java-8-openjdk

FROM mvn-install AS mvn-package
COPY /src /usr/home/spark-graphx-scala/src
RUN cd /usr/home/spark-graphx-scala && mvn clean package

FROM base
COPY --from=zeppelin /opt/zeppelin /opt/zeppelin
COPY --from=spark /opt/spark /opt/zeppelin/spark
COPY --from=openjdk /usr/lib/jvm/java-8-openjdk /lib/jvm/java-8-openjdk
COPY --from=mvn-package /usr/home/spark-graphx-scala/target /opt/zeppelin/target
COPY zeppelin-site.xml /opt/zeppelin/conf/zeppelin-site.xml
ENV JAVA_HOME=/lib/jvm/java-8-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH
WORKDIR /opt/zeppelin
CMD ["printenv"]