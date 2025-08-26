import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

spark_submit_command = """
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,com.github.jnr:jnr-posix:3.1.15 \
  --conf spark.driver.host=airflow-scheduler \
  /opt/airflow/spark_apps/etl_cassandra_to_postgres.py \
  "{{ ds }}"
"""

with DAG(
    dag_id="iot_pipeline_bash_operator_fix",
    start_date=pendulum.datetime(2025, 8, 7, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["iot", "spark", "bash-fix"],
) as dag:
    
    run_spark_job_task = BashOperator(
        task_id="run_spark_job_via_bash",
        bash_command=spark_submit_command,
    )