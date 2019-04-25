from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['csj@qutoutiao.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dag_name': "macros",
    'schedule_interval': '* * * * *'
}

jarpath = 'aaa/ggg/dds/ss'

dag = DAG(args['dag_name'], default_args=args, schedule_interval=args['schedule_interval'], user_defined_macros=dict(path=jarpath))

task = BashOperator(
    dag=dag,
    bash_command="echo {{path}}",
    task_id="macros_path"
)
