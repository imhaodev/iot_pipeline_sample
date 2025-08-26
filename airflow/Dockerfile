# Bắt đầu từ image Airflow gốc
FROM apache/airflow:2.5.0

# Chuyển sang user root để có quyền cài đặt
USER root

# Cài đặt OpenJDK (bắt buộc cho spark-submit) và procps (để có lệnh 'ps')
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Quay trở lại user airflow mặc định
USER airflow

# Cài đặt gói provider cho Spark
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

# Thiết lập biến môi trường JAVA_HOME cho Airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64