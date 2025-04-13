from datetime import datetime, timedelta
import logging
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.run_sql import RunListSQLOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'nalina',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'max_active_runs': 3
}

@dag(
    default_args=default_args,
    catchup=False,
    schedule_interval='0 * * * *',
    description='Load and transform data in Redshift with Airflow'
)


def final_project():

    sql_obj = SqlQueries()

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')

    create_table_task = RunListSQLOperator(
        task_id = "Create_table",
        conn_id="my_redshift",
        list_sql=sql_obj.create_table_list
    )


    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        postgres_conn_id="my_redshift",
        aws_credentials_id="myaws_credentials",
        table="staging_events",
        s3_bucket="nali-data-pipeline",
        s3_key= "log_data/2018/11",
        json_format="s3://nali-data-pipeline/log_json_path.json",
        execution_date=datetime.now()
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='staging_songs_table',
        postgres_conn_id="my_redshift",
        aws_credentials_id="myaws_credentials",
        table="staging_songs",
        s3_bucket="nali-data-pipeline",
        s3_key= "song_data/A/A",
        json_format="auto"

    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        postgres_conn_id="my_redshift",
        sql=sql_obj.songplay_table_insert

    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        append=True,
        table='users',
        postgres_conn_id="my_redshift",
        sql=sql_obj.user_table_insert

    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        append=True,
        table='songs',
        postgres_conn_id="my_redshift",
        sql=sql_obj.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        append=True,
        table='artists',
        postgres_conn_id="my_redshift",
        sql=sql_obj.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        append=True,
        table='times',
        postgres_conn_id="my_redshift",
        sql=sql_obj.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        postgres_conn_id="my_redshift",
        table = "songplays"

    )

    start_operator >> create_table_task

    create_table_task >> stage_events_to_redshift
    create_table_task >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_user_dimension_table>>run_quality_checks
    load_song_dimension_table>>run_quality_checks
    load_artist_dimension_table>>run_quality_checks
    load_time_dimension_table>>run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()