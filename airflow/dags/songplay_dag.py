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
    # TODO - Replace None with '0 * * * *'
    schedule_interval=None,  #'0 * * * *'
)

start_task = DummyOperator(task_id="Begin_execution", dag=dag)

drop_tables_task = PostgresOperator(
    # Note: drop_tables.sql needs to be in the airflow/dags folder in order to be picked up
    task_id="drop_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="drop_tables.sql",
)

create_tables_task = PostgresOperator(
    # Note: create_tables.sql needs to be in the airflow/dags folder in order to be picked up
    task_id="create_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql",
)

stage_events_to_redshift_task = StageToRedshiftOperator(
    task_id="stage_events_to_redshift_task",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_events",
    json_paths="s3://dend-util/data/events_log_jsonpath.json",
    s3_bucket="udacity-dend",
    s3_key="log_json_path.json",
    role_arn="arn:aws:iam::921412997039:role/dwhRole",
    aws_region="us-west-2",
)

stage_songs_to_redshift_task = StageToRedshiftOperator(
    task_id="stage_songs_to_redshift_task",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_songs",
    json_paths="auto",
    s3_bucket="udacity-dend",
    # TODO - Remove /A/Z to process all songs
    s3_key="song-data/A/Z",
    role_arn="arn:aws:iam::921412997039:role/dwhRole",
    aws_region="us-west-2",
)

load_songplays_table = LoadFactOperator(
    task_id="load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="pipedb",
    destination_table="public.songplays",
    sql=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="pipedb",
    destination_table="public.users",
    sql=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="pipedb",
    destination_table="public.songs",
    sql=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="pipedb",
    destination_table="public.artists",
    sql=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="pipedb",
    destination_table="public.time",
    sql=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id="run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["public.songplays", "public.artists", "public.time", "public.songs", "public.users",],
)

end_task = DummyOperator(task_id="Stop_execution", dag=dag)

start_task >> drop_tables_task >> create_tables_task >> [
    stage_events_to_redshift_task,
    stage_songs_to_redshift_task,
] >> load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks >> end_task
