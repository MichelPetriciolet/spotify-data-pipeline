from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers. snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args ={
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spotify_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spotify', 'snowflake', 'dbt'],
) as dag:
    
    #1. Detectar cualquier .csv nuevo en S3
    check_s3_file = S3KeySensor(
        task_id='check_s3_file',
        bucket_name='data-raw-try',
        bucket_key='spotify/*.csv',
        wildcard_match=True,
        aws_conn_id='aws_default',
        poke_interval=60,
        timeout=300,
    )
    
    #2. Cargar datos a RAW extrayendo particion del path S3
    load_raw = SnowflakeOperator(
        task_id= 'load_raw',
        snowflake_conn_id='snowflake_default',
        sql="""
            COPY INTO spotify_db.bronze.spotify_tracks (
                row_index_2, row_index_1, track_id, artists, album_name, track_name,
                popularity, duration_ms, explicit, danceability, energy, key, loudness,
                mode, speechiness, acousticness, instrumentalness, liveness, valence,
                tempo, time_signature, track_genre, metric_date,
                year, month, day
            )
            FROM (
                SELECT
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                    $15, $16, $17, $18, $19, $20, $21, $22, $23,
                    TRY_CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=([0-9]+)', 1, 1, 'e', 1) AS NUMBER),
                    TRY_CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'month=([0-9]+)', 1, 1, 'e', 1) AS NUMBER),
                    TRY_CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'day=([0-9]+)', 1, 1, 'e', 1) AS NUMBER)
                FROM @spotify_db.bronze.stage_spotify_s3
            )
        """,  
    )
    
    
    #3. Validar que se cargaron los datos
    def check_rows(**context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        result = hook.get_first("Select count(*) from spotify_db.bronze.spotify_tracks")
        row_count= result[0]
        context['ti'].xcom_push(key='row_count', value=row_count)
        return row_count > 0
    
    validate_load = ShortCircuitOperator(
        task_id='validate_load',
        python_callable=check_rows,
        provide_context=True,
    )
    
    
    #4. Email si no hay datos
    email_no_data = EmailOperator(
        task_id='email_no_data',
        to=['josuemichelpetricioletcortes@gmail.com'],
        subject='⚠️ Spotify Pipeline — Sin datos cargados {{ ds }}',
        html_content="""
            <h2>Pipeline de Spotify — Advertencia</h2>
            <p>El pipeline del <b>{{ ds }}</b> no encontró datos para cargar.</p>
            <ul>
                <li><b>DAG:</b> spotify_etl_dag</li>
                <li><b>Fecha:</b> {{ ds }}</li>
                <li><b>Tabla:</b> spotify_db.bronze.spotify_tracks</li>
            </ul>
            <p>El pipeline se detuvo sin modificar los marts existentes.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    
    #5. Registrar ejecucion en log
    register_load = SnowflakeOperator(
        task_id='register_load',
        snowflake_conn_id='snowflake_default',
        sql="""
            INSERT INTO spotify_db.bronze.pipeline_log
                (execution_date, rows_loaded, status, loaded_at)
            SELECT 
                CURRENT_DATE(),
                COUNT(*),
                'SUCCESS',
                CURRENT_TIMESTAMP()
            FROM spotify_db.bronze.spotify_tracks;
        """,
    )
    
    
    #6. Correr modelos dbt
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command="""
            cd /opt/airflow/spotify_dbt && \
            dbt run \
              --profiles-dir /opt/airflow/spotify_dbt \
              --project-dir /opt/airflow/spotify_dbt
        """,
    )
    
    
    #7. Correr test dbt
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command="""
            cd /opt/airflow/spotify_dbt && \
            dbt test \
              --profiles-dir /opt/airflow/spotify_dbt \
              --project-dir /opt/airflow/spotify_dbt
        """,
    )
    
    
    #4. Email si corrio correcto el pipeline
    email_success_data = EmailOperator(
        task_id='email_success_data',
        to=['josuemichelpetricioletcortes@gmail.com'],
        subject=' Spotify Pipeline — Terminado excitosamente',
        html_content="""
            <h2>Pipeline de Spotify — Terminado</h2>
            <p>El pipeline del <b>{{ ds }}</b> termino sin errores.</p>
            <ul>
                <li><b>DAG:</b> spotify_etl_dag</li>
                <li><b>Fecha:</b> {{ ds }}</li>
                <li><b>Tabla:</b> spotify_db.gold.spotify_tracks</li>
            </ul>
            <p>El pipeline ejecuto las cargas a las tablas correspondientes.</p>
        """,
        trigger_rule='all_success',
    )
    
    
    #DEPENDENCIAS
    check_s3_file >> load_raw >> validate_load
    validate_load >> email_no_data
    validate_load >> register_load >> run_dbt_models >> run_dbt_tests >> email_success_data
    