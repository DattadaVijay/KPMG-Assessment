from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'swimming_rankings_process',
    default_args=default_args,
    description='Process swimming rankings data and create views',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False
)

# Start point for both paths
start = DummyOperator(
    task_id='start',
    dag=dag
)

# Path 1: File sensing path
check_for_new_files = FileSensor(
    task_id='check_for_new_files',
    filepath='/scripts/raw_data/*.csv',  # Updated path to match container volume
    poke_interval=300,
    timeout=600,
    soft_fail=True,  # Continue even if no files found
    dag=dag
)

# Path 2: Direct processing path
trigger_direct_process = DummyOperator(
    task_id='trigger_direct_process',
    dag=dag
)

# Process swimming data using Docker container
process_swimming_data = DockerOperator(
    task_id='process_swimming_data',
    image='swimming-process',
    container_name='swimming-process',
    api_version='auto',
    auto_remove=True,
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    trigger_rule='one_success',  # Will trigger if either path completes
    dag=dag
)

# Create or update views
create_views = PostgresOperator(
    task_id='create_views',
    postgres_conn_id='swimming_olympics_db',
    sql="""
    -- Olympic Qualifications View
    CREATE OR REPLACE VIEW vw_olympic_qualifications AS 
    WITH SwimmerQualifications AS (
        SELECT 
            s.swimmer_sk,
            s.name,
            s.nationality,
            s.birth_date,
            EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birth_date)) as age,
            CASE 
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birth_date)) BETWEEN 12 AND 14 THEN '12-14'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birth_date)) BETWEEN 15 AND 17 THEN '15-17'
                ELSE '17+'
            END as age_group,
            e.stroke,
            COUNT(DISTINCT sp.competition_sk) as total_competitions,
            COUNT(DISTINCT CASE WHEN sp.time_seconds BETWEEN 22 AND 50 THEN sp.competition_sk END) as qualifying_competitions
        FROM 
            dim_swimmer s
            JOIN fact_swimming_performance sp ON s.swimmer_sk = sp.swimmer_sk
            JOIN dim_event e ON sp.event_sk = e.event_sk
            JOIN dim_competition c ON sp.competition_sk = c.competition_sk
        WHERE 
            s.is_current = true 
            AND EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birth_date)) >= 12
        GROUP BY 
            s.swimmer_sk, s.name, s.nationality, s.birth_date, e.stroke
        HAVING 
            COUNT(DISTINCT sp.competition_sk) >= 50
    )
    SELECT 
        sq.name,
        sq.nationality,
        sq.age,
        sq.age_group,
        sq.stroke,
        sq.total_competitions,
        sq.qualifying_competitions
    FROM SwimmerQualifications sq
    WHERE sq.qualifying_competitions >= 5
    ORDER BY sq.name, sq.stroke;

    -- Olympic Cuts View
    CREATE OR REPLACE VIEW vw_olympic_cuts AS
    SELECT 
        s.name,
        s.nationality,
        e.stroke,
        MIN(sp.time_seconds) as best_time,
        CASE 
            WHEN MIN(sp.time_seconds) <= 25 THEN 'A Cut'
            WHEN MIN(sp.time_seconds) <= 30 THEN 'B Cut'
            ELSE 'Not Qualified'
        END as olympic_cut
    FROM 
        dim_swimmer s
        JOIN fact_swimming_performance sp ON s.swimmer_sk = sp.swimmer_sk
        JOIN dim_event e ON sp.event_sk = e.event_sk
    WHERE 
        s.is_current = true
    GROUP BY 
        s.name, s.nationality, e.stroke
    ORDER BY 
        e.stroke, best_time;

    -- Last Five Rankings View
    CREATE OR REPLACE VIEW vw_last_five_rankings AS
    WITH LastFiveCompetitions AS (
        SELECT 
            s.name,
            s.nationality,
            e.stroke,
            sp.overall_rank,
            sp.competition_sk,
            ROW_NUMBER() OVER (PARTITION BY s.swimmer_sk, e.stroke ORDER BY c.competition_date DESC) as comp_order
        FROM 
            dim_swimmer s
            JOIN fact_swimming_performance sp ON s.swimmer_sk = sp.swimmer_sk
            JOIN dim_event e ON sp.event_sk = e.event_sk
            JOIN dim_competition c ON sp.competition_sk = c.competition_sk
        WHERE 
            s.is_current = true
    )
    SELECT 
        name,
        nationality,
        stroke,
        COUNT(CASE WHEN overall_rank = 1 THEN 1 END) as rank_1_count
    FROM 
        LastFiveCompetitions
    WHERE 
        comp_order <= 5
    GROUP BY 
        name, nationality, stroke
    HAVING 
        COUNT(CASE WHEN overall_rank = 1 THEN 1 END) >= 3
    ORDER BY 
        name, stroke;

    -- Final Olympic Qualified Swimmers View
    CREATE OR REPLACE VIEW vw_olympic_qualified_swimmers AS
    WITH qualified_swimmers AS (
        SELECT DISTINCT
            q.name,
            q.nationality,
            q.age,
            q.age_group,
            q.stroke,
            c.olympic_cut,
            COALESCE(r.rank_1_count, 0) as rank_1_count,
            q.total_competitions,
            q.qualifying_competitions
        FROM 
            vw_olympic_qualifications q
            JOIN vw_olympic_cuts c ON q.name = c.name AND q.stroke = c.stroke
            LEFT JOIN vw_last_five_rankings r ON q.name = r.name AND q.stroke = r.stroke
        WHERE 
            c.olympic_cut IN ('A Cut', 'B Cut')
            AND q.age >= 12
            AND q.total_competitions >= 50
            AND q.qualifying_competitions >= 5
    )
    SELECT *
    FROM qualified_swimmers
    ORDER BY 
        CASE olympic_cut 
            WHEN 'A Cut' THEN 1 
            WHEN 'B Cut' THEN 2 
            ELSE 3 
        END,
        stroke,
        rank_1_count DESC,
        name;
    """,
    dag=dag
)

# Set task dependencies
start >> [check_for_new_files, trigger_direct_process]  # Split into two paths
[check_for_new_files, trigger_direct_process] >> process_swimming_data  # Both paths lead to processing
process_swimming_data >> create_views  # Always create views after processing 