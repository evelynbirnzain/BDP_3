FROM amazoncorretto:17-alpine

RUN apk --no-cache add curl bash tar python3

RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
RUN tar xvf spark-3.5.1-bin-hadoop3.tgz

RUN mv spark-3.5.1-bin-hadoop3/ /opt/spark

RUN echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
RUN echo "export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin" >> ~/.bashrc

WORKDIR /spark-processor

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin:/opt/spark/sbin
ENV PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

COPY . .

ENTRYPOINT ["bash", "start_spark.sh"]
CMD ["spark_processor.py"]
