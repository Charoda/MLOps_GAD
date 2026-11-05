FROM apache/airflow:3.3.1-python3.13

COPY constraints-course.txt /tmp/constraints-course.txt
RUN pip install --no-cache-dir --constraint /tmp/constraints-course.txt \
    "apache-airflow==3.3.1" \
    "apache-airflow-providers-postgres==7.0.2" \
    "apache-airflow-providers-http==6.0.5" \
    "numpy==2.5.1" \
    "pytest==9.1.1" \
    && pip check
