from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# assign the default args values
default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2018, 11, 1),
    'catchup': False,
    'email_on_retry': False,
}

# constant operator variables
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'
REGION = 'us-west-2'

# --------- initialize the pipeline dag ---------
dag = DAG('loading-datapipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *' # @hourly
        )


# --------- initialize the operators ---------
# dummy start operator
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# stagging operators
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}',
    region=REGION,
    data_store_format="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region=REGION,
    data_store_format="JSON 'auto'"
)

# ====== loading operators ======
# load fact table operator
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    table='songplays',
    sql=SqlQueries.songplay_table_insert,        
    truncate_flag=False
)

# load dimension tables operators
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    table='users',
    sql=SqlQueries.user_table_insert,        
    truncate_flag=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    table='songs',
    sql=SqlQueries.song_table_insert,        
    truncate_flag=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    table='artists',
    sql=SqlQueries.artist_table_insert,        
    truncate_flag=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    table='time',
    sql=SqlQueries.time_table_insert,        
    truncate_flag=True
)

# quality check operator
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    tables=["songplays", "users", "songs", "artists", "time"]
)

# dummy end operator
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


# --------- dependencies ---------
start_operator \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator

