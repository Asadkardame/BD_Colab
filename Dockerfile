FROM openjdk:8-jre-slim-buster

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark
RUN pip3 install pyspark==3.2.1
RUN pip3 install findspark

# Set the working directory
WORKDIR /app

# Copy the PySpark application code into the container
COPY incrementalLoad.py .

# Download and add the PostgreSQL JDBC driver
RUN apt-get update && \
    apt-get install -y wget && \
    wget -O postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

RUN mv postgresql.jar /app/postgresql-42.2.23.jar

# Set the entrypoint
CMD ["spark-submit", "--master", "local[*]", "--jars", "/app/postgresql-42.2.23.jar", "incrementalLoad.py"]

