FROM python:3.8-slim

# Set environment variables
ENV SPARK_VERSION=3.1.1
ENV POSTGRES_DRIVER_VERSION=42.2.23

# Install necessary packages
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk

# Download and install Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.2.tgz | tar xz -C /usr/local/
RUN ln -s /usr/local/spark-$SPARK_VERSION-bin-hadoop3.2 /usr/local/spark

# Set environment variables for Spark and PySpark
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Add PostgreSQL JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-$POSTGRES_DRIVER_VERSION.jar $SPARK_HOME/jars/

# Set up your working directory
WORKDIR /app

# Copy your Python script and other necessary files
COPY incrementalLoad.py /app/

# Set the entry point
ENTRYPOINT ["spark-submit", "--jars", "/usr/local/spark/jars/postgresql-$POSTGRES_DRIVER_VERSION.jar", "incremental.py"]

