FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

WORKDIR /usr/src/app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

USER airflow