from __future__ import print_function

from datetime import timedelta, datetime

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['chenshaojin@qutoutiao.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'dag_name': "",
    'schedule_interval': ""
}

task_id = ''
HADOOP_USER_NAME = "spark"
application = '/opt/apps/spark-current/examples/jars/spark-examples_2.11-2.1.1.jar'
conn_id = 'spark-cpc'
java_class = "org.apache.spark.examples.SparkPi"
executor_cores = 10
executor_memory = "20g"
driver_memory = "10g"
num_executors = 20
maxRetries = 100
memoryOverhead = '20g'
maxExecutors = 40
application_args = []

dag = DAG(args['dag_name'], default_args=args, schedule_interval=args['schedule_interval'])

user_profile = SparkSubmitOperator(
    executor_config={
        "KubernetesExecutor": {
            "hostnetwork": True,
            "envs": {
                "HADOOP_USER_NAME": HADOOP_USER_NAME
            }
        }
    },
    task_id=task_id,
    dag=dag,
    application=application,
    conf={
        'spark.port.maxRetries': maxRetries,
        'spark.yarn.executor.memoryOverhead': memoryOverhead,
        'spark.dynamicAllocation.maxExecutors': maxExecutors
    },
    conn_id=conn_id,
    java_class=java_class,
    executor_cores=executor_cores,
    executor_memory=executor_memory,
    driver_memory=driver_memory,
    num_executors=num_executors,
    application_args=application_args,
    verbose=False
)

user_profile
