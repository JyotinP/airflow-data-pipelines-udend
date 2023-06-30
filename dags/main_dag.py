from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import logging
import datetime
import sys
sys.path.insert(0, "/home/workspace/airflow/plugins/operators")
from create_table_redshift import CreateTablesOnRedshiftOperator
from stage_s3_to_redshift import StageS3ToRedshiftOperator
from data_quality_check import DataQualityCheckOperator
from load_dim_from_stage import LoadDimensionFromStageOperator
from load_fact_from_stage import LoadFactFromStageOperator
import sql_queries

default_args = {
    'start_date': datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    'depends_on_past ': False,
    'owner' :  'udacity',
    'depends_on_past' :  False,
    'retries' :  3,
    'retry_delay' : datetime.timedelta(minutes=5) ,
    'email_on_retry': False
    }

dag = DAG(
    'pipeline_project',
    default_args= default_args,
    max_active_runs = 1,
    schedule_interval= '@hourly',
    catchup = False
)

def begin_execution():
    logging.info("Execution has started")

begin_execution_task = PythonOperator(
    dag = dag,
    task_id = 'Begin_execution',
    python_callable= begin_execution
)

def end_execution():
    logging.info("Execution has ended")

end_execution_task = PythonOperator(
    dag = dag,
    task_id = 'End_execution',
    python_callable= end_execution
)

create_staging_events_table_task = CreateTablesOnRedshiftOperator(
    dag = dag,
    task_id = 'create_staging_events_table_task',
    table = 'staging_events',
    create_sql = sql_queries.staging_events_table_create,
    redshift_conn_id= 'redshift'
)

create_staging_songs_table_task = CreateTablesOnRedshiftOperator(
    dag = dag,
    task_id = 'create_staging_songs_table_task',
    table = 'staging_songs',
    create_sql = sql_queries.staging_songs_table_create,
    redshift_conn_id= 'redshift'
)

create_songplays_table_task = CreateTablesOnRedshiftOperator(
    dag = dag,
    task_id = 'create_songplays_table_task',
    table = 'songplays',
    create_sql = sql_queries.songplays_table_create,
    redshift_conn_id= 'redshift'
)

create_users_table_task = CreateTablesOnRedshiftOperator(
    dag = dag,
    task_id = 'create_users_table_task',
    table = 'users',
    create_sql = sql_queries.users_table_create,
    redshift_conn_id= 'redshift'
)

create_songs_table_task = CreateTablesOnRedshiftOperator(
    dag = dag,
    task_id = 'create_songs_table_task',
    table = 'songs',
    create_sql = sql_queries.songs_table_create,
    redshift_conn_id= 'redshift'
)

create_artists_table_task = CreateTablesOnRedshiftOperator(
    dag = dag,
    task_id = 'create_artists_table_task',
    table = 'artists',
    create_sql = sql_queries.artists_table_create,
    redshift_conn_id= 'redshift'
)

create_time_table_task = CreateTablesOnRedshiftOperator(
    dag = dag,
    task_id = 'create_time_table_task',
    table = 'time',
    create_sql = sql_queries.time_table_create,
    redshift_conn_id= 'redshift'
)

load_staging_events_table_task = StageS3ToRedshiftOperator(
    dag = dag,
    task_id = 'Stage_events',
    aws_conn_id= "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    redshift_conn_id="redshift",
    table="staging_events",
    json="s3://udacity-dend/log_json_path.json"
)

load_staging_songs_table_task = StageS3ToRedshiftOperator(
    dag = dag,
    task_id = 'Stage_songs',
    aws_conn_id= "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    redshift_conn_id="redshift",
    table="staging_songs",
    json="auto ignorecase"
)

load_songplays_task = LoadFactFromStageOperator(
    dag = dag,
    task_id = 'Load_songplays_fact_table',
    redshift_conn_id = "redshift",
    table = "songplays",
    load_sql = sql_queries.songplays_table_insert,
)

load_users_task = LoadDimensionFromStageOperator(
    dag = dag,
    task_id = 'load_users_dim_table',
    redshift_conn_id = "redshift",
    table = "users",
    load_sql = sql_queries.users_table_insert,
    truncate= True
)

load_songs_task = LoadDimensionFromStageOperator(
    dag = dag,
    task_id = 'load_songs_dim_table',
    redshift_conn_id = "redshift",
    table = "songs",
    load_sql = sql_queries.songs_table_insert,
)

load_artists_task = LoadDimensionFromStageOperator(
    dag = dag,
    task_id = 'load_artists_dim_table',
    redshift_conn_id = "redshift",
    table = "artists",
    load_sql = sql_queries.artists_table_insert,
)

load_time_task = LoadDimensionFromStageOperator(
    dag = dag,
    task_id = 'load_time_dim_table',
    redshift_conn_id = "redshift",
    table = "time",
    load_sql = sql_queries.time_table_insert,
)

run_data_quality_checks_task = DataQualityCheckOperator(
    dag = dag,
    task_id = "Run_data_quality_checks",
    redshift_conn_id = "redshift",
    checks= [{'title': "songplays Count Check", 'sql': sql_queries.songplays_count_check , 'expected': 333},
             {'title': "songplays Duplicates Check", 'sql': sql_queries.songplays_duplicate_check , 'expected': 0},
             {'title': "artists Count Check", 'sql': sql_queries.artists_count_check , 'expected': 9553},
             {'title': "artists Duplicates Check", 'sql': sql_queries.artists_duplicate_check , 'expected': 0},
             {'title': "songs Count Check", 'sql': sql_queries.songs_count_check , 'expected': 14896},
             {'title': "songs Duplicates Check", 'sql': sql_queries.songs_duplicate_check , 'expected': 0},
             {'title': "time Count Check", 'sql': sql_queries.time_count_check , 'expected': 333},
             {'title': "time Duplicates Check", 'sql': sql_queries.time_duplicate_check , 'expected': 0},
             {'title': "users Count Check", 'sql': sql_queries.users_count_check , 'expected': 97},
             {'title': "users Duplicates Check", 'sql': sql_queries.users_duplicate_check , 'expected': 0}
             ]
)


begin_execution_task >> create_staging_events_table_task  >> load_staging_events_table_task
begin_execution_task >> create_staging_songs_table_task >> load_staging_events_table_task
begin_execution_task >> create_users_table_task >> load_staging_events_table_task
begin_execution_task >> create_songs_table_task >> load_staging_events_table_task
begin_execution_task >> create_artists_table_task >> load_staging_events_table_task
begin_execution_task >> create_time_table_task >> load_staging_events_table_task
begin_execution_task >> create_songplays_table_task >> load_staging_events_table_task

create_staging_events_table_task >> load_staging_songs_table_task
create_staging_songs_table_task >> load_staging_songs_table_task
create_users_table_task >> load_staging_songs_table_task
create_songs_table_task >> load_staging_songs_table_task
create_artists_table_task >> load_staging_songs_table_task
create_time_table_task >> load_staging_songs_table_task
create_songplays_table_task >> load_staging_songs_table_task

load_staging_events_table_task >> load_songplays_task
load_staging_songs_table_task >> load_songplays_task

load_songplays_task >> load_songs_task
load_songplays_task >> load_users_task
load_songplays_task >> load_artists_task
load_songplays_task >> load_time_task

load_songplays_task >> run_data_quality_checks_task
load_users_task >> run_data_quality_checks_task
load_songs_task >> run_data_quality_checks_task
load_artists_task >> run_data_quality_checks_task
load_time_task >> run_data_quality_checks_task

run_data_quality_checks_task >> end_execution_task
