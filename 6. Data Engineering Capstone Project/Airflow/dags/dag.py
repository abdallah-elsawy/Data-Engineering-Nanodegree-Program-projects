from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
config = configparser.ConfigParser()
config.read('config.cfg')

REDSHIFT_ARN = config['CLUSTER']['ARN']
s3_bucket = config['S3']['BUCKET']


}
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 22),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

immigration_to_redshift = StageToRedshiftOperator(
    task_id='immigration',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id= "aws_credentials",
    table="public.immigration",
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json",
    role_arn=REDSHIFT_ARN,
    s3_bucket=s3_bucket,
    s3_key="log_data", 
    file_format=table['file_format'],
    delimiter=table['sep']
)



temperature_to_redshift = StageToRedshiftOperator(
    task_id='temperature',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='public.temperature',
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json",
    role_arn=REDSHIFT_ARN,
    s3_bucket=s3_bucket,
    s3_key="log_data", 
    file_format=table['file_format'],
    delimiter=table['sep']
)


load_immigration_table = LoadFactOperator(
    task_id='Load_immigration_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.simmigration",
    truncate_table=True,
    sql_query=SqlQueries.immigration_table_insert
)



load_temperature_table = LoadFactOperator(
    task_id='Load_temperature_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.temperature",
    truncate_table=True,
    sql_query=SqlQueries.temperature_table_insert
)






run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[ "immigration", "temperature"]
)
    
    

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> immigration_to_redshift
start_operator >> temperature_to_redshift

immigration_to_redshift >> load_immigration_table
temperature_to_redshift >> load_temperature_table

load_immigration_table >> run_quality_checks
load_temperature_table >> run_quality_checks

run_quality_checks>> end_operator