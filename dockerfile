FROM apache/airflow:2.9.0-python3.8

USER root
RUN apt-get update && apt-get install -y \
    python3-venv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dags/ /opt/airflow/dags/
COPY source/ /opt/airflow/data/source/
COPY staging/ /opt/airflow/data/staging/