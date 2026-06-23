FROM apache/airflow:2.8.1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir "dbt-snowflake==1.7.0" "dbt-core==1.7.0"
