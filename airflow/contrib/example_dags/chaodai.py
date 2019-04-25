from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['siyi@qutoutiao.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dag_name': "chao_dai_user_loan_demand",
    'schedule_interval': '30 09 * * *'
}

add_partition_hql = "alter table rpt_finance.user_installed_apps add partition(day='${datadate}');"

user_loan_demand_hql = """
add file data_transform.py;
set mapreduce.job.queuename=root.chaodai.data;
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1600m;
set mapreduce.reduce.memory.mb=2048;
set mapreduce.reduce.java.opts=-Xmx1600m;
set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;

with temp1 as (
select uid, pkg
from dl_cpc.cpc_user_installed_apps
LATERAL VIEW EXPLODE(pkgs) pkgs_detail as pkg
where load_date between date_sub('${datadate}', 2) and '${datadate}'
),
temp2 as (
select a.uid, collect_list(b.pin_name) as pkgs
from temp1 a
inner join test.app_name_huzhu b on a.pkg = b.pkg
where a.uid is not null and b.pin_name is not null
group by a.uid
)
insert overwrite table rpt_finance.user_loan_demand partition (day='${datadate}')
select transform(uid, pkgs) using 'python3 data_transform.py' as uid, score
from temp2;
"""
conn_id = 'hadoop4_chaodai'

dag = DAG(args['dag_name'], default_args=args, schedule_interval=args['schedule_interval'])

add_partition = HiveOperator(
    task_id='add_partition',
    hql=add_partition_hql,
    hive_cli_conn_id=conn_id,
    dag=dag
)

user_loan_demand = HiveOperator(
    task_id='user_loan_demand',
    hql=user_loan_demand_hql,
    hive_cli_conn_id=conn_id,
    dag=dag
)

add_partition >> user_loan_demand
