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

# Start date: today at 07:30 AM UTC
start_datetime = datetime.utcnow().replace(hour=06, minute=30, second=0, microsecond=0)


with DAG(
    dag_id='domain_dim',
    default_args=default_args,
    description='domain_dim',
    schedule_interval="30 5 * * *",  # Cron expression for 5:30 AM UTC
    start_date=tomorrow_530am,
    catchup=False,
    tags=['bigquery', 'domain_dim', 'enrichment'],
) as dag:

    def run_bigquery_query():
        client = bigquery.Client()

        sql_query = """        
        MERGE `ox-wissp-devint.enriched.enriched_domain_dim` AS T
USING (
SELECT
d.DOMAIN_NAME,
TRIM(metacategory) AS METACATEGORY_ID, -- Keep METACATEGORY_ID as is
m.METACATEGORY_NAME,
m.METACATEGORY_DESCRIPTION,
m.METACATEGORY_CODE,
d.iab2_tier_1_name,
d.iab2_tier_2_name,
d.iab2_tier_3_name,
d.iab2_tier_4_name,
CURRENT_TIMESTAMP() AS update_time,
-- Generate UID for domain only if it doesn't exist already
COALESCE(domain_lookup.uid, GENERATE_UUID()) AS UID
FROM `ox-wissp-devint.wissp_views.exchange_views_domain_dim` d
LEFT JOIN UNNEST(SPLIT(IFNULL(d.METACATEGORY, ''), ',')) AS metacategory
LEFT JOIN `ox-wissp-devint.enriched.enriched_metacategory_dim` m
ON TRIM(metacategory) = CAST(m.UID AS STRING) -- Join on METACATEGORY_UID
LEFT JOIN (
SELECT DISTINCT domain_name, uid
FROM `ox-wissp-devint.enriched.enriched_domain_dim`
WHERE uid IS NOT NULL
) AS domain_lookup
ON d.DOMAIN_NAME = domain_lookup.domain_name -- Match only on domain name
) AS S
ON T.DOMAIN_NAME = S.DOMAIN_NAME AND T.METACATEGORY_ID = S.METACATEGORY_ID -- Join condition on DOMAIN_NAME and METACATEGORY_ID

WHEN MATCHED AND (
T.METACATEGORY_NAME != S.METACATEGORY_NAME OR
T.METACATEGORY_DESCRIPTION != S.METACATEGORY_DESCRIPTION OR
T.METACATEGORY_CODE != S.METACATEGORY_CODE OR
T.IAB2_TIER_1_NAME != S.IAB2_TIER_1_NAME OR
T.IAB2_TIER_2_NAME != S.IAB2_TIER_2_NAME OR
T.IAB2_TIER_3_NAME != S.IAB2_TIER_3_NAME OR
T.IAB2_TIER_4_NAME != S.IAB2_TIER_4_NAME
)
THEN UPDATE SET
METACATEGORY_NAME = S.METACATEGORY_NAME,
METACATEGORY_DESCRIPTION = S.METACATEGORY_DESCRIPTION,
METACATEGORY_CODE = S.METACATEGORY_CODE,
IAB2_TIER_1_NAME = S.IAB2_TIER_1_NAME,
IAB2_TIER_2_NAME = S.IAB2_TIER_2_NAME,
IAB2_TIER_3_NAME = S.IAB2_TIER_3_NAME,
IAB2_TIER_4_NAME = S.IAB2_TIER_4_NAME,
last_modified_at = S.update_time

WHEN NOT MATCHED THEN
INSERT (
DOMAIN_NAME, METACATEGORY_ID, METACATEGORY_NAME,
METACATEGORY_DESCRIPTION, METACATEGORY_CODE,
IAB2_TIER_1_NAME, IAB2_TIER_2_NAME,
IAB2_TIER_3_NAME, IAB2_TIER_4_NAME,
created_at, last_modified_at, uid
)
VALUES (
S.DOMAIN_NAME, S.METACATEGORY_ID, S.METACATEGORY_NAME,
S.METACATEGORY_DESCRIPTION, S.METACATEGORY_CODE,
S.IAB2_TIER_1_NAME, S.IAB2_TIER_2_NAME,
S.IAB2_TIER_3_NAME, S.IAB2_TIER_4_NAME,
S.update_time, S.update_time, S.UID
);
        """

        client.query(sql_query).result()
        print("✅ BigQuery job completed")

    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_bigquery_query,
    )

    run_query_task  # This line was added
