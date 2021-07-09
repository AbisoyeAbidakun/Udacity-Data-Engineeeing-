from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag_name = 'udac_sparkify_example_dag'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table_name="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log-data",
    file_format="JSON",
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table_name="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format="JSON",
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    truncate_table=True,
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="users",
    delete_load=True,
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songs",
    delete_load=True,
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="artists",
    delete_load=True,
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="time",
    delete_load=True,
    sql_query=SqlQueries.time_table_insert
)

dq_checks=[{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0, "comparison": '=='},
           {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0, "comparison": '=='},
          {'check_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 1, "comparison": '>='},
          {'check_sql': "SELECT COUNT(*) FROM users", 'expected_result': 1, "comparison": '>='},
          {'check_sql': "SELECT COUNT(*) FROM songplays", 'expected_result': 1, "comparison": '>='},
          {'check_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 1, "comparison": '>='},
          {'check_sql': "SELECT COUNT(*) FROM time", 'expected_result': 1, "comparison": '>='}]


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >>  [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
