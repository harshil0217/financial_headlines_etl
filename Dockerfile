FROM apache/airflow:2.5.0-python3.10
COPY . .
RUN pip3 install -r requirements.txt

