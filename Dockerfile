FROM apache/airflow:2.9.1-python3.11

# Copy the requirements file and install all dependencies (including pytest)
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
