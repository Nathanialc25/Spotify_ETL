import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

from spotify_etl_script import spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2024, 2, 27),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes = 1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Spotify ETL process 1-min',
    schedule_interval='0 9 * * 0',
)

def ETL():
    print("started")
    transformed_artist_df, transformed_song_df = spotify_etl()
    conn = BaseHook.get_connection('postgres_sql')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    transformed_song_df.to_sql("song_of_the_week", engine, index=False, if_exists='append')
    transformed_artist_df.to_sql("artist_table", engine, index=False, if_exists='replace')


with dag:    
    #song table will continuosly add the new top 10 of the week
    create_song_table = PostgresOperator(
        task_id = 'create_song_table',
        postgres_conn_id = 'postgres_sql',
        sql = """
        CREATE TABLE IF NOT EXISTS song_of_the_week(
            chart_position INT,
            track_name VARCHAR(200),
            track_uri VARCHAR(200),
            artist VARCHAR(200),
            artist_uri VARCHAR(200),
            release_date DATE,
            etxract_ts TIMESTAMP,
            pk VARCHAR(200),
            CONSTRAINT song_primary_key_constraint PRIMARY KEY (pk)
        )
        """
    )
    #Artist table will add all new artist, and when it encounter a current primary key- it will update the row
    create_artist_table = PostgresOperator(
        task_id = 'create_artist_table',
        postgres_conn_id = 'postgres_sql',
        sql="""
        CREATE TABLE IF NOT EXISTS artist_table(
            artist_id VARCHAR(200),
            artist_name VARCHAR(200),
            artist_genre VARCHAR(200),
            artist_followers INT,
            etxract_ts TIMESTAMP,
            CONSTRAINT artist_primary_key_constraint PRIMARY KEY (artist_id)
        )
        """
    )

    run_etl = PythonOperator(
        task_id = 'spotify_etl_task',
        python_callable = ETL,
        dag = dag,
    )

[create_artist_table, create_song_table] >> run_etl 