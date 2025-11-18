FROM apache/airflow:2.10.0


COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

USER root

COPY quarto.sh /
RUN cd / && bash /quarto.sh

RUN apt-get update && apt-get install -y --no-install-recommends default-jdk

RUN curl https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz -o spark-3.5.7-bin-hadoop3.tgz

RUN chmod 755 spark-3.5.7-bin-hadoop3.tgz

RUN mkdir -p /opt/spark && tar xvzf spark-3.5.7-bin-hadoop3.tgz --directory /opt/spark --strip-components=1

ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64'
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME='/opt/spark'
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

