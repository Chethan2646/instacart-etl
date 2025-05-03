FROM apache/airflow:2.7.3

# Install kafka-python
RUN pip install kafka-python
