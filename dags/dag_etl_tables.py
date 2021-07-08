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
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False
}

dag = DAG(
    'dag_etl_tables',
    default_args = default_args,
    description = 'Load and transform data in Redshift with Airflow',
    schedule_interval = '@hourly'
)


start_operator = DummyOperator(task_id = 'Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data",
    region="us-west-2",
    format_type="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song-data/A/",
    region="us-west-2",
    format_type="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql="songplay_table_insert"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dimension_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    sql="song_table_insert",
    truncate="True"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dimension_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    sql="user_table_insert",
    truncate="True"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dimension_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    sql="artist_table_insert",
    truncate="True"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    sql="time_table_insert",
    truncate="True"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=["songplays", "songs", "users", "artists", "time"],
    sql_check_queries=[
        { 'sql': 'SELECT COUNT(*) FROM public.songplays WHERE songplay_id IS NULL', 'result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.users WHERE userid IS NULL', 'result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.songs WHERE song_id IS NULL', 'result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.artists WHERE artist_id IS NULL', 'result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public."time" WHERE start_time IS NULL', 'result': 0 },

        { 'sql': 'SELECT MAX(COUNT("start_time")) FROM public."time" GROUP BY start_time', 'result': 1 },

        { 'sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'result': 2},
        { 'sql': 'SELECT COUNT(DISTINCT "gender") FROM public.songplays', 'result': 3},

        { 'sql': 'SELECT MAX(LENGTH("songplay_id")) FROM public.songplays', 'result': 32},
        { 'sql': 'SELECT MIN(LENGTH("songplay_id")) FROM public.songplays', 'result': 32},

        { 'sql': 'SELECT LENGTH(MAX("artist_latitude")) FROM public.artists', 'result': 3},
        { 'sql': 'SELECT LENGTH(MAX("artist_longitude")) FROM public.artists', 'result': 3},
        
        { 'sql': 'SELECT LENGTH(MIN("duration")) FROM public.artists', 'result': 1}
    ],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, 
                   stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, 
                         load_song_dimension_table, 
                         load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator