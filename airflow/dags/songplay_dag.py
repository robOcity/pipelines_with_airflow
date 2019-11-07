from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    "owner": "udacity",
    "start_date": datetime(2019, 1, 12),
}

dag = DAG(
    "songplay_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval=None,  #'0 * * * *'
)

start_task = DummyOperator(task_id="Begin_execution", dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    database="pipedb",
    autocommit=True,
    sql=f"""
    DROP TABLE IF EXISTS event_staging;
    CREATE TABLE event_staging (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);""",
)

stage_events_to_redshift_task = StageToRedshiftOperator(
    task_id="stage_events_to_redshift_task",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="event_staging",
    json_format_file="s3://dend-util/data/events_log_jsonpath.json",
    s3_bucket="udacity-dend",
    s3_key="log_json_path.json",
    role_arn="arn:aws:iam::921412997039:role/dwhRole",
    aws_region="us-west-2",
)

# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag
# )

# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

end_task = DummyOperator(task_id="Stop_execution", dag=dag)

start_task >> create_tables_task >> stage_events_to_redshift_task >> end_task
