FROM apache/airflow:2.8.0


USER root

RUN apt-get update && \

    apt-get -y install git && \

    apt-get clean



USER airflow