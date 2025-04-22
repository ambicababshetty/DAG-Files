from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='metacategory_dim',
    default_args=default_args,
    description='Insert CPR and Bid performance data into enriched_domain_performance_fact',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'performance', 'enrichment'],
) as dag:

    def run_bigquery_query():
        client = bigquery.Client()

        sql_query = """
        MERGE `ox-wissp-devint.enriched.enriched_metacategory_dim` AS T
USING (
SELECT
src.METACATEGORY_ID AS METACATEGORY_ID,
src.NAME AS METACATEGORY_NAME,
src.DESCRIPTION AS METACATEGORY_DESCRIPTION,
src.METACATEGORY_CODE AS METACATEGORY_CODE,
CURRENT_TIMESTAMP() AS current_time,
COALESCE(uid_lookup.UID, GENERATE_UUID()) AS UID
FROM `ox-wissp-devint.wissp_views.exchange_views_metacategory_dim` src
LEFT JOIN (
SELECT METACATEGORY_ID, UID
FROM `ox-wissp-devint.enriched.enriched_metacategory_dim`
WHERE UID IS NOT NULL
) AS uid_lookup
ON src.METACATEGORY_ID = uid_lookup.METACATEGORY_ID
) AS S
ON T.METACATEGORY_ID = S.METACATEGORY_ID

WHEN MATCHED AND (
T.METACATEGORY_NAME != S.METACATEGORY_NAME OR
T.METACATEGORY_DESCRIPTION != S.METACATEGORY_DESCRIPTION OR
T.METACATEGORY_CODE != S.METACATEGORY_CODE
)
THEN
UPDATE SET
METACATEGORY_NAME = S.METACATEGORY_NAME,
METACATEGORY_DESCRIPTION = S.METACATEGORY_DESCRIPTION,
METACATEGORY_CODE = S.METACATEGORY_CODE,
last_modified_at = S.current_time

WHEN NOT MATCHED THEN
INSERT (
METACATEGORY_ID,
METACATEGORY_NAME,
METACATEGORY_DESCRIPTION,
METACATEGORY_CODE,
UID,
created_at,
last_modified_at
)
VALUES (
S.METACATEGORY_ID,
S.METACATEGORY_NAME,
S.METACATEGORY_DESCRIPTION,
S.METACATEGORY_CODE,
S.UID,
S.current_time,
S.current_time
);
        """

        query_job = client.query(sql_query)
        query_job.result()
        print("BigQuery query executed successfully.")

    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_bigquery_query,
    )

    run_query_task
