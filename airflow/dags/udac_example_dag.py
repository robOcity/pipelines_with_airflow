from datetime import datetime, timedelta
import os
from configparser import SafeConfigParser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries
from datetime import datetime

default_args = {"start_date": datetime(2019, 10, 24)}


def get_config(section, key, config_file=".env/pipeline.cfg"):
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(cfg_file, encoding="utf-8")
    return parser.get(section, key)


dag = DAG(
    "udac_example_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval=None,  # TODO run manually for now
)

# start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="load_events_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="event_staging",
    s3_bucket="udacity-dend",
    s3_key="log_json_path.json",
    role_arn="arn:aws:iam::921412997039:role/dwhRole",
    aws_region="us-west-2",
)

# stage_songs_to_redshift = StageToRedshiftOperator(task_id="Stage_songs", dag=dag)

# load_songplays_table = LoadFactOperator(task_id="Load_songplays_fact_table", dag=dag)

# load_user_dimension_table = LoadDimensionOperator(task_id="Load_user_dim_table", dag=dag)

# load_song_dimension_table = LoadDimensionOperator(task_id="Load_song_dim_table", dag=dag)

# load_artist_dimension_table = LoadDimensionOperator(task_id="Load_artist_dim_table", dag=dag)

# load_time_dimension_table = LoadDimensionOperator(task_id="Load_time_dim_table", dag=dag)
# run_quality_checks = DataQualityOperator(task_id="Run_data_quality_checks", dag=dag)

# end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

stage_events_to_redshift
