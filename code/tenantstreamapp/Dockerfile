FROM amazoncorretto:11

RUN yum -y update && yum install -y tar gzip procps

RUN curl -O https://archive.apache.org/dist/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz
RUN tar xvf spark-3.2.4-bin-hadoop3.2.tgz

RUN mv spark-3.2.4-bin-hadoop3.2/ /opt/spark

RUN echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
RUN echo "export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin" >> ~/.bashrc

ENV PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.mongodb.spark:mongo-spark-connector_2.12:10.2.2"
RUN /opt/spark/bin/spark-shell --packages $PACKAGES


RUN yum install -y python3 python3-pip
COPY requirements.txt .
RUN pip3 install -r requirements.txt

WORKDIR /spark-processor

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

COPY . .

ENTRYPOINT ["bash", "start_spark.sh"]
CMD ["spark_processor.py"]
