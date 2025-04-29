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

# Start date: today at 06:00 AM UTC (which is 11:30 AM IST)
start_datetime = datetime.utcnow().replace(hour=6, minute=0, second=0, microsecond=0)

with DAG(
    dag_id='domain_dim',
    default_args=default_args,
    description='domain_dim',
    schedule_interval="0 6 * * *",  # Every day at 06:00 UTC = 11:30 IST
    start_date=start_datetime,
    catchup=False,
    tags=['bigquery', 'domain_dim', 'enrichment'],
) as dag:

    def run_bigquery_query():
        client = bigquery.Client()

        sql_query = """        
        MERGE `ox-wissp-devint.enriched.enriched_domain_dim_upt` AS target
USING (
WITH max_id AS (
SELECT IFNULL(MAX(ID), 1000) AS max_existing_id
FROM `ox-wissp-devint.enriched.enriched_domain_dim_upt`
),
new_data AS (
SELECT
combined_name AS DOMAIN_NAME,
APP_NAME,
SourceTable,
METACATEGORY_ID,
METACATEGORY_NAME,
METACATEGORY_DESCRIPTION,
METACATEGORY_CODE,
iab2_tier_1_name,
iab2_tier_2_name,
iab2_tier_3_name,
iab2_tier_4_name,
CURRENT_TIMESTAMP() AS last_updated_at,
CURRENT_TIMESTAMP() AS created_at
FROM (
-- domain_dim
SELECT
d.DOMAIN_NAME AS combined_name,
NA' AS APP_NAME,
domain_dim' AS SourceTable,
TRIM(metacategory) AS METACATEGORY_ID,
m.METACATEGORY_NAME,
m.METACATEGORY_DESCRIPTION,
m.METACATEGORY_CODE,
d.iab2_tier_1_name,
d.iab2_tier_2_name,
d.iab2_tier_3_name,
d.iab2_tier_4_name
FROM `ox-wissp-devint.wissp_views.exchange_views_domain_dim` d
LEFT JOIN UNNEST(
CASE
WHEN ARRAY_LENGTH(SPLIT(IFNULL(d.METACATEGORY, ''), ',')) = 0 THEN ARRAY<STRING>[NULL]
ELSE SPLIT(IFNULL(d.METACATEGORY, ''), ',')
END
) AS metacategory
LEFT JOIN `ox-wissp-devint.enriched.enriched_metacategory_dim` m
ON TRIM(metacategory) = CAST(m.METACATEGORY_ID AS STRING)
WHERE d.DOMAIN_NAME IS NOT NULL AND d.DOMAIN_NAME <> ''

UNION ALL

-- bundle_dim
SELECT
b.BUNDLE_ID AS combined_name,
b.APP_NAME,
bundle_dim' AS SourceTable,
TRIM(meta_cat) AS METACATEGORY_ID,
m.METACATEGORY_NAME,
m.METACATEGORY_DESCRIPTION,
m.METACATEGORY_CODE,
b.IAB2_TIER_1_NAME,
b.IAB2_TIER_2_NAME,
b.IAB2_TIER_3_name,
b.IAB2_TIER_4_name
FROM `ox-wissp-devint.wissp_views.exchange_views_bundle_dim` b
LEFT JOIN UNNEST(
CASE
WHEN ARRAY_LENGTH(SPLIT(IFNULL(b.METACATEGORY, ''), ',')) = 0 THEN ARRAY<STRING>[NULL]
ELSE SPLIT(IFNULL(b.METACATEGORY, ''), ',')
END
) AS meta_cat
LEFT JOIN `ox-wissp-devint.enriched.enriched_metacategory_dim` m
ON TRIM(meta_cat) = CAST(m.METACATEGORY_ID AS STRING)
WHERE b.BUNDLE_ID IS NOT NULL AND b.BUNDLE_ID <> ''
)
),

domain_id_map AS (
SELECT
d.DOMAIN_NAME,
COALESCE(existing.ID, max_id.max_existing_id + ROW_NUMBER() OVER (ORDER BY d.DOMAIN_NAME)) AS ID
FROM (
SELECT DISTINCT DOMAIN_NAME FROM new_data
) d
LEFT JOIN `ox-wissp-devint.enriched.enriched_domain_dim_upt` existing
ON d.DOMAIN_NAME = existing.DOMAIN_NAME
CROSS JOIN max_id
),

data_with_id AS (
SELECT
d.*,
id_map.ID
FROM new_data d
LEFT JOIN domain_id_map id_map
ON d.DOMAIN_NAME = id_map.DOMAIN_NAME
)

SELECT * FROM data_with_id
) AS source
ON target.DOMAIN_NAME = source.DOMAIN_NAME
AND target.METACATEGORY_ID = source.METACATEGORY_ID -- optional for multi-category
WHEN MATCHED THEN
UPDATE SET
target.APP_NAME = source.APP_NAME,
target.SourceTable = source.SourceTable,
target.METACATEGORY_ID = source.METACATEGORY_ID,
target.METACATEGORY_NAME = source.METACATEGORY_NAME,
target.METACATEGORY_DESCRIPTION = source.METACATEGORY_DESCRIPTION,
target.METACATEGORY_CODE = source.METACATEGORY_CODE,
target.iab2_tier_1_name = source.iab2_tier_1_name,
target.iab2_tier_2_name = source.iab2_tier_2_name,
target.iab2_tier_3_name = source.iab2_tier_3_name,
target.iab2_tier_4_name = source.iab2_tier_4_name,
target.last_updated_at = source.last_updated_at,
target.created_at = source.created_at
WHEN NOT MATCHED THEN
INSERT (
ID,
DOMAIN_NAME,
APP_NAME,
SourceTable,
METACATEGORY_ID,
METACATEGORY_NAME,
METACATEGORY_DESCRIPTION,
METACATEGORY_CODE,
iab2_tier_1_name,
iab2_tier_2_name,
iab2_tier_3_name,
iab2_tier_4_name,
last_updated_at,
created_at
)
VALUES (
source.ID,
source.DOMAIN_NAME,
source.APP_NAME,
source.SourceTable,
source.METACATEGORY_ID,
source.METACATEGORY_NAME,
source.METACATEGORY_DESCRIPTION,
source.METACATEGORY_CODE,
source.iab2_tier_1_name,
source.iab2_tier_2_name,
source.iab2_tier_3_name,
source.iab2_tier_4_name,
source.last_updated_at,
source.created_at
);
        """

        client.query(sql_query).result()
        print("✅ BigQuery job completed")

    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_bigquery_query,
    )

    run_query_task
