from __future__ import print_function

from datetime import timedelta, datetime

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG

args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime.utcnow(),
    'email': ['yuzhaojing@qutoutiao.net'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

executor_config = {
    "KubernetesExecutor": {
        "hostnetwork": True,
        "envs": {
            "HADOOP_USER_NAME": "spark"
        }
    }
}

conn_id = 'spark-cpc'
verbose = False
date = '{{ yesterday_ds }}'
today = '{{ ds }}'
hour = '{{ hour }}'


dag = DAG(dag_id='spark_cpc', default_args=args, schedule_interval=None, user_defined_macros=dict(cpcpath='/opt/soft/airflow/dags/'))

update_pak_01 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='update_pak_01',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.dynamicAllocation.maxExecutors': '20', 'spark.yarn.executor.memoryOverhead': '5g',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.app.AppCategoryPackage',
    executor_cores=5,
    executor_memory='1g',
    driver_memory='2g',
    num_executors=1,
    application_args=[1, 'test', 'app_tar_conf', '{{ cpcpath }}/AppPakConf'],
    verbose=verbose,
    
)

userprofile_tagbyapps_daily_01 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='userprofile_tagbyapps_daily_01',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '5g', 'spark.dynamicAllocation.maxExecutors': '20',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.TagUserByApps',
    executor_cores=5,
    executor_memory='10g',
    driver_memory='6g',
    num_executors=10,
    application_args=[1],
    verbose=verbose,
    
)

user_profile_daily_0_01 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_0_01',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '5g', 'spark.dynamicAllocation.maxExecutors': '20',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.UserSeenMotivateAds',
    executor_cores=5,
    executor_memory='10g',
    driver_memory='6g',
    num_executors=10,
    application_args=[date],
    verbose=verbose,
    
)

user_profile_daily_0_02 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_0_02',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '10g', 'spark.dynamicAllocation.maxExecutors': '20',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.NoClickUser',
    executor_cores=5,
    executor_memory='10g',
    driver_memory='6g',
    num_executors=20,
    application_args=[3],
    verbose=verbose,
    
)

user_profile_daily_0_03 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_0_03',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '10g', 'spark.dynamicAllocation.maxExecutors': '30',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.userprofile.QttLaxinPackage',
    executor_cores=5,
    executor_memory='5g',
    driver_memory='10g',
    num_executors=30,
    application_args=[date],
    verbose=verbose,
    
)

user_profile_daily_0_04 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_0_04',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '20g', 'spark.dynamicAllocation.maxExecutors': '30',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.userprofile.TeacherStudents',
    executor_cores=5,
    executor_memory='10g',
    driver_memory='10g',
    num_executors=30,
    application_args=[date],
    verbose=verbose,
    
)

user_profile_daily_1_01 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_1_01',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '10g', 'spark.port.maxRetries': '100',
          'spark.dynamicAllocation.maxExecutors': '40'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.TagUserByZfb',
    executor_cores=5,
    executor_memory='10g',
    driver_memory='10g',
    num_executors=40,
    application_args=[10, True],
    verbose=verbose,
    
)

user_profile_daily_1_02 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_1_02',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '10g', 'spark.port.maxRetries': '100',
          'spark.dynamicAllocation.maxExecutors': '40'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.userprofile.GetNewUser2V2',
    executor_cores=5,
    executor_memory='10g',
    driver_memory='10g',
    num_executors=40,
    application_args=[today],
    verbose=verbose,
    
)

user_profile_daily_2_01 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_2_01',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.dynamicAllocation.maxExecutors': '50', 'spark.port.maxRetries': '100',
          'spark.yarn.executor.memoryOverhead': '4g'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.PredictNoClickUser',
    executor_cores=8,
    executor_memory='24g',
    driver_memory='8g',
    num_executors=30,
    application_args=[today],
    verbose=verbose,
    
)

user_profile_daily_2_02 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_2_02',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.dynamicAllocaton.maxExecutors': '50', 'spark.dynamicAllocation.maxExecutors': '50',
          'spark.port.maxRetries': '100', 'spark.yarn.executor.memoryOverhead': '4g'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.TagUserByDownload',
    executor_cores=8,
    executor_memory='24g',
    driver_memory='8g',
    num_executors=30,
    application_args=[date],
    verbose=verbose,
    
)

user_profile_daily_3_01 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_3_01',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.dynamicAllocation.maxExecutors': '20', 'spark.port.maxRetries': '128',
          'spark.default.parallelism': '500'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.userprofile.FinanceBagV1',
    executor_cores=4,
    executor_memory='6g',
    driver_memory='4g',
    num_executors=20,
    application_args=[104100100, 233],
    verbose=verbose,
    
)

user_profile_daily_3_02 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_3_02',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.dynamicAllocation.maxExecutors': '40', 'spark.port.maxRetries': '100',
          'spark.default.parallelism': '500', 'spark.yarn.executor.memoryOverhead': '20g'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.TagUserByLx',
    executor_cores=10,
    executor_memory='20g',
    driver_memory='10g',
    num_executors=20,
    application_args=[1, False],
    verbose=verbose,
    
)

user_profile_daily_3_03 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_3_03',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.dynamicAllocation.maxExecutors': '40', 'spark.port.maxRetries': '100',
          'spark.default.parallelism': '500', 'spark.yarn.executor.memoryOverhead': '20g'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.TagUserByPV',
    executor_cores=10,
    executor_memory='20g',
    driver_memory='10g',
    num_executors=20,
    application_args=[5, False],
    verbose=verbose,
    
)

user_profile_daily_3_04 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_3_04',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.dynamicAllocation.maxExecutors': '30', 'spark.port.maxRetries': '100',
          'spark.default.parallelism': '500', 'spark.yarn.executor.memoryOverhead': '2g'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.userprofile.GetNewUserV2',
    executor_cores=2,
    executor_memory='4g',
    driver_memory='4g',
    num_executors=30,
    application_args=[today],
    verbose=verbose,
    
)

user_profile_daily_4_01 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_4_01',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '20g', 'spark.dynamicAllocation.maxExecutors': '30',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.CheckTaobaoTargetUser',
    executor_cores=5,
    executor_memory='10g',
    driver_memory='10g',
    num_executors=30,
    application_args=[date],
    verbose=verbose,
    
)

user_profile_daily_4_02 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_4_02',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '5g', 'spark.dynamicAllocation.maxExecutors': '30',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.usercoin.CoinLikers',
    executor_cores=10,
    executor_memory='10g',
    driver_memory='20g',
    num_executors=30,
    application_args=[],
    verbose=verbose,
    
)

user_profile_daily_4_03 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_4_03',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.yarn.executor.memoryOverhead': '20g', 'spark.dynamicAllocation.maxExecutors': '50',
          'spark.port.maxRetries': '100'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.TagDownloadUser',
    executor_cores=5,
    executor_memory='10g',
    driver_memory='10g',
    num_executors=50,
    application_args=[date],
    verbose=verbose,
    
)

user_profile_daily_to_redis_01 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_to_redis_01',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.port.maxRetries': '100', 'spark.dynamicAllocation.maxExecutors': '40',
          'spark.yarn.executor.memoryOverhead': '20g'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.userprofile.SetUserProfileTag',
    executor_cores=10,
    executor_memory='20g',
    driver_memory='10g',
    num_executors=20,
    application_args=[False, 0],
    verbose=verbose,
    
)

user_profile_daily_to_redis_02 = SparkSubmitOperator(
    executor_config=executor_config,
    task_id='user_profile_daily_to_redis_02',
    dag=dag,
    application='{{ cpcpath }}/cpc_anal_userprofile.jar',
    conf={'spark.port.maxRetries': '100', 'spark.dynamicAllocation.maxExecutors': '40',
          'spark.yarn.executor.memoryOverhead': '20g'},
    conn_id=conn_id,
    java_class='com.cpc.spark.qukan.interest.TagFromHivetoRedis',
    executor_cores=10,
    executor_memory='20g',
    driver_memory='10g',
    num_executors=20,
    application_args=[0, today, hour],
    verbose=verbose,
    
)

update_pak_01 >> userprofile_tagbyapps_daily_01 >> [user_profile_daily_to_redis_01, user_profile_daily_to_redis_02]
[user_profile_daily_0_01, user_profile_daily_0_02, user_profile_daily_0_03, user_profile_daily_0_04] >> user_profile_daily_to_redis_01
[user_profile_daily_0_01, user_profile_daily_0_02, user_profile_daily_0_03, user_profile_daily_0_04] >> user_profile_daily_to_redis_02
[user_profile_daily_1_01, user_profile_daily_1_02] >> user_profile_daily_to_redis_01
[user_profile_daily_1_01, user_profile_daily_1_02] >> user_profile_daily_to_redis_02
[user_profile_daily_2_01, user_profile_daily_2_02] >> user_profile_daily_to_redis_01
[user_profile_daily_2_01, user_profile_daily_2_02] >> user_profile_daily_to_redis_02
[user_profile_daily_3_01, user_profile_daily_3_02, user_profile_daily_3_03, user_profile_daily_3_04] >> user_profile_daily_to_redis_01
[user_profile_daily_3_01, user_profile_daily_3_02, user_profile_daily_3_03, user_profile_daily_3_04] >> user_profile_daily_to_redis_02
[user_profile_daily_4_01, user_profile_daily_4_02, user_profile_daily_4_03] >> user_profile_daily_to_redis_01
[user_profile_daily_4_01, user_profile_daily_4_02, user_profile_daily_4_03] >> user_profile_daily_to_redis_02
