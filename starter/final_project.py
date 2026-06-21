from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

from custom_operators.stage_redshift import StageToRedshiftOperator
from custom_operators.load_fact import LoadFactOperator
from custom_operators.load_dimension import LoadDimensionOperator
from custom_operators.data_quality import DataQualityOperator


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='redshift',
        sql=[SqlQueries.create_staging_events, SqlQueries.create_staging_songs]
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        table='staging_events',
        s3_bucket=Variable.get("s3_bucket"),
        s3_key='log-data',
        iam_role='arn:aws:iam::896254294593:role/my-redshift-service-role',
        region='us-west-2',
        json_format='s3://udacity-dend/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        table='staging_songs',
        s3_bucket=Variable.get("s3_bucket"),
        s3_key='song-data/A/A/A',
        iam_role='arn:aws:iam::896254294593:role/my-redshift-service-role',
        region='us-west-2',
        json_format='auto'
    )

    create_fact_table = PostgresOperator(
        task_id='create_fact_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.create_songplays
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert   
    )

    create_user_dimension_table = PostgresOperator(
        task_id='create_user_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.create_users_table
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert,
        mode='delete-load'
    )

    create_song_dimension_table = PostgresOperator(
        task_id='create_song_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.create_songs_table
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert,
        mode='delete-load'
    )

    create_artist_dimension_table = PostgresOperator(
        task_id='create_artist_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.create_artists_table
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        mode='delete-load'
    )

    create_time_dimension_table = PostgresOperator(
        task_id='create_time_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.create_time_table
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert,
        mode='delete-load'
    )
    
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']  
    )

    start_operator >> create_staging_tables >> [stage_events_to_redshift, stage_songs_to_redshift]

    [stage_events_to_redshift, stage_songs_to_redshift] >> create_fact_table

    create_fact_table >> load_songplays_table

    load_songplays_table >> [create_user_dimension_table, create_song_dimension_table, create_artist_dimension_table, create_time_dimension_table]
    
    # [create_user_dimension_table, create_song_dimension_table, create_artist_dimension_table, create_time_dimension_table] >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] 
    
    create_user_dimension_table >> load_user_dimension_table
    create_song_dimension_table >> load_song_dimension_table
    create_artist_dimension_table >> load_artist_dimension_table
    create_time_dimension_table >> load_time_dimension_table
    
    
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks


final_project_dag = final_project()