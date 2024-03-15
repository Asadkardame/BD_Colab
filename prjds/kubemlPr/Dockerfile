FROM openjdk:8-jre-slim-buster

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark
RUN pip3 install pyspark==3.2.1
RUN pip3 install findspark
RUN pip3 install numpy
# Set the working directory
WORKDIR /app

# Copy the PySpark application code into the container
COPY predict_inc_model.py .
COPY models /models
# Set the entrypoint
CMD ["spark-submit", "--master", "local[*]", "predict_inc_model.py"]
