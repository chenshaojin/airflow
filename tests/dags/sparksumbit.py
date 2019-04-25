from __future__ import print_function

from datetime import timedelta, datetime

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG

args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime.utcnow(),
    'email': ['chenshaojin@qutoutiao.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='spark_cpc',
    default_args=args,
    schedule_interval= '1 * 1 * *'
)

user_profile = SparkSubmitOperator(
    executor_config={
        "KubernetesExecutor": {
            "hostnetwork": True,
            "envs": {
                "HADOOP_USER_NAME": "spark"
            }
        }
    },
    task_id='spark_task',
    dag=dag,
    application='/opt/apps/spark-current/examples/jars/spark-examples_2.11-2.1.1.jar',
    conf={
        'spark.port.maxRetries': 100,
        'spark.yarn.executor.memoryOverhead': '20g',
        'spark.dynamicAllocation.maxExecutors': 40
    },
    conn_id='spark-cpc',
    java_class="org.apache.spark.examples.SparkPi",
    executor_cores=10,
    executor_memory="20g",
    driver_memory="10g",
    name='spark_user_profile_dag',
    num_executors=20,
    verbose=False
)

user_profile
