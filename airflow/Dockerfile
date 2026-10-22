FROM apache/airflow:3.2.0

# Install additional dependencies
USER root
RUN apt-get update && apt-get install -y gcc && apt-get clean
COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt