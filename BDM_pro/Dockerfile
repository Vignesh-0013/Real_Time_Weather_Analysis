FROM python:3.10

# Java needed for Spark
RUN apt-get update && apt-get install -y default-jdk wget
# Install Spark
WORKDIR /opt
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
 && tar -xzf spark-3.5.1-bin-hadoop3.tgz \
 && mv spark-3.5.1-bin-hadoop3 spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash"]