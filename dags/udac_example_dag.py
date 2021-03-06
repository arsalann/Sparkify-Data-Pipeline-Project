from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

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
    # TODO change default dag name
    'udac_example_dag',
    default_args = default_args,
    description = 'Load and transform data in Redshift with Airflow',
    schedule_interval = '@hourly',
    catchup = False
)

start_operator = DummyOperator(task_id = 'Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag=dag)
