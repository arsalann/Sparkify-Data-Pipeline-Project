
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlCreate

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

'''
DEFAULT PARAMETERS:
    * The DAG does not have dependencies on past runs
    * On failure, the task are retried 3 times
    * Retries happen every 5 minutes
    * Catchup is turned off
    * Do not email on retry
'''

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay' : timedelta(minutes=5),
    'email_on_failure' : False,
    'email_on_retry' : False
}

dag = DAG(
    'dag_create_tables',
    default_args = default_args,
    description = 'Create Fact and Dimension Template Tables in Redshift',
    schedule_interval='@once',
    catchup = False
)

start_operator = DummyOperator(task_id = 'Begin_execution',  dag=dag)

create_table_artists = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_table_artists',
  postgres_conn_id="redshift"
)

create_table_songplays = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_table_songplays',
  postgres_conn_id="redshift"
)

create_table_songs = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_table_songs',
  postgres_conn_id="redshift"
)

create_table_staging_events = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_table_staging_events',
  postgres_conn_id="redshift"
)

create_table_staging_songs = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_table_staging_songs',
  postgres_conn_id="redshift"
)

create_table_time = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_table_time',
  postgres_conn_id="redshift"
)

create_table_users = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_table_users',
  postgres_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [
    create_table_artists,
    create_table_songplays,
    create_table_songs,
    create_table_staging_events,
    create_table_staging_songs,
    create_table_time,
    create_table_users
] >> end_operator