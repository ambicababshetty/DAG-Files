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
    dag_id='domain_performance_fact',
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
        INSERT INTO `ox-wissp-devint.enriched.enriched_domain_performance_fact_updated` (
    RewrittenPageURLDomain,
    domain_id,
    app_name,
    ReportDay,
    IABDistributionChannel,
    IABMediaSubtype,
    UserGeoCountry,
    VideoPlcmt,
    IABDeviceType,
    PublisherAccountID,
    IsDirect,
    CPRClicks,
    ImpressionsWithClickMacro,
    CPRMarketDemandPartnerSpendInUSD,
    CPRMarketPublisherRevenueInUSD,
    CPRMarketRequests,
    CPRAllRequests,
    CPRPrivateMarketImpressions,
    CompleteFirstOccurrence,
    StartFirstOccurrence,
    Viewables,
    ViewabilityMeasures,
    Fills,
    NonZeroBids,
    Impressions,
    DealVolumeDealInfo,
    NonZeroBidsValueInUSD,
    BidSpendInUSD,
    ClickThroughRate,
    RevenuePerImpression,
    CostPerImpression,
    VTR,
    Viewability,
    ox_win_rate,
    pub_win_rate,
    bid_cpm,
    Bid_Guidance,
    created_at,
    last_modified_at
)
WITH domain_meta AS (
    SELECT DISTINCT
        domain_name,
        id AS domain_id,
        APP_NAME,
        SourceTable
    FROM `ox-wissp-devint.enriched.enriched_domain_dim_upt`
   
),
iab_device_type AS (
    SELECT DISTINCT
        id,
        type_name
    FROM `ox-wissp-devint.wissp_views.exchange_views_iab_device_type_dim`
),
cpr_agg AS (
    SELECT
        RewrittenPageURLDomain,
        ReportDay,
        IABDistributionChannel,
        IABMediaSubtype,
        UserGeoCountry,
        VideoPlcmt,
        dt.type_name AS IABDeviceType,
        PublisherAccountID,
        IsDirect,
        SUM(Clicks) AS Clicks,
        SUM(ImpressionsWithClickMacro) AS ImpressionsWithClickMacro,
        SUM(CPRMarketDemandPartnerSpendInUSD) AS CPRMarketDemandPartnerSpendInUSD,
        SUM(CPRMarketPublisherRevenueInUSD) AS CPRMarketPublisherRevenueInUSD,
        SUM(CPRMarketImpressions) AS CPRMarketImpressions,
        SUM(CPRMarketRequests) AS CPRMarketRequests,
        SUM(CPRAllRequests) AS CPRAllRequests,
        SUM(CPRExchangeFills) AS CPRExchangeFills,
        SUM(CPRPrivateMarketImpressions) AS CPRPrivateMarketImpressions
    FROM `ox-wissp-devint.wissp_views.exchange_views_cpr_daily_fact_v2` cpr
    INNER JOIN domain_meta dm ON cpr.RewrittenPageURLDomain = dm.domain_name
    LEFT JOIN iab_device_type dt ON cpr.IABDeviceType = dt.id
    WHERE cpr.ReportDay = '2025-04-07'
    GROUP BY 1,2,3,4,5,6,7,8,9
),
bid_agg AS (
    SELECT
        RewrittenPageURLDomain,
        ReportDay,
        UserGeoCountry,
        IABMediaSubtype,
        IABDistributionChannel,
        dt.type_name AS IABDeviceType,
        PublisherAccountID,
        SUM(CompleteFirstOccurrence) AS CompleteFirstOccurrence,
        SUM(StartFirstOccurrence) AS StartFirstOccurrence,
        SUM(Viewables) AS Viewables,
        SUM(ViewabilityMeasures) AS ViewabilityMeasures,
        SUM(Fills) AS Fills,
        SUM(NonZeroBids) AS NonZeroBids,
        SUM(Impressions) AS Impressions,
        SUM(DealVolumeDealInfo) AS DealVolumeDealInfo,
        SUM(NonZeroBidsValueInUSD) AS NonZeroBidsValueInUSD,
        SUM(SpendInUSD) AS BidSpendInUSD
    FROM `ox-wissp-devint.wissp_views.exchange_views_bid_performance_daily_fact` bid
    INNER JOIN domain_meta dm ON bid.RewrittenPageURLDomain = dm.domain_name
    LEFT JOIN iab_device_type dt ON bid.IABDeviceType = dt.id
    WHERE bid.ReportDay = '2025-04-07'
    GROUP BY 1,2,3,4,5,6,7
)
SELECT DISTINCT
    dm.domain_name AS RewrittenPageURLDomain,
    CAST(dm.domain_id AS STRING) AS domain_id,
    dm.app_name,
    b.ReportDay,
    b.IABDistributionChannel,
    b.IABMediaSubtype,
    b.UserGeoCountry,
    c.VideoPlcmt,
    b.IABDeviceType,
    CAST(b.PublisherAccountID AS STRING) AS PublisherAccountID,
    c.IsDirect,
    c.Clicks AS CPRClicks,
    c.ImpressionsWithClickMacro,
    c.CPRMarketDemandPartnerSpendInUSD,
    c.CPRMarketPublisherRevenueInUSD,
    c.CPRMarketRequests,
    c.CPRAllRequests,
    c.CPRPrivateMarketImpressions,
    b.CompleteFirstOccurrence,
    b.StartFirstOccurrence,
    b.Viewables,
    b.ViewabilityMeasures,
    b.Fills,
    b.NonZeroBids,
    b.Impressions,
    b.DealVolumeDealInfo,
    b.NonZeroBidsValueInUSD,
    b.BidSpendInUSD,
    SAFE_DIVIDE(c.Clicks, NULLIF(c.ImpressionsWithClickMacro, 0)) AS ClickThroughRate,
    SAFE_DIVIDE(c.CPRMarketPublisherRevenueInUSD, NULLIF(c.CPRMarketImpressions, 0)) AS RevenuePerImpression,
    SAFE_DIVIDE(c.CPRMarketDemandPartnerSpendInUSD, NULLIF(c.CPRMarketImpressions, 0)) AS CostPerImpression,
    SAFE_DIVIDE(b.CompleteFirstOccurrence, NULLIF(b.StartFirstOccurrence, 0)) * 100 AS VTR,
    SAFE_DIVIDE(b.Viewables, NULLIF(b.ViewabilityMeasures, 0)) * 100 AS Viewability,
    SAFE_DIVIDE(b.Fills, NULLIF(b.NonZeroBids, 0)) AS ox_win_rate,
    SAFE_DIVIDE(b.Impressions, NULLIF(b.Fills, 0)) AS pub_win_rate,
    SAFE_DIVIDE(b.NonZeroBidsValueInUSD, NULLIF(b.NonZeroBids, 0)) AS bid_cpm,
    SAFE_DIVIDE(b.BidSpendInUSD, NULLIF(c.CPRMarketImpressions, 0)) AS Bid_Guidance,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS last_modified_at
FROM domain_meta dm
INNER JOIN bid_agg b ON dm.domain_name = b.RewrittenPageURLDomain
LEFT JOIN cpr_agg c ON dm.domain_name = c.RewrittenPageURLDomain
    AND c.ReportDay = b.ReportDay
    AND c.UserGeoCountry = b.UserGeoCountry
    AND c.IABMediaSubtype = b.IABMediaSubtype
    AND c.IABDistributionChannel = b.IABDistributionChannel
    AND c.IABDeviceType = b.IABDeviceType
    AND c.PublisherAccountID = b.PublisherAccountID
WHERE
    (COALESCE(SAFE_DIVIDE(c.Clicks, NULLIF(c.ImpressionsWithClickMacro, 0)), 0.0) != 0
    OR COALESCE(c.CPRMarketDemandPartnerSpendInUSD, 0.0) != 0
    OR COALESCE(c.CPRMarketPublisherRevenueInUSD, 0.0) != 0
    OR SAFE_DIVIDE(c.CPRMarketPublisherRevenueInUSD, NULLIF(c.CPRMarketImpressions, 0)) != 0
    OR SAFE_DIVIDE(c.CPRMarketDemandPartnerSpendInUSD, NULLIF(c.CPRMarketImpressions, 0)) != 0
    OR SAFE_DIVIDE(b.CompleteFirstOccurrence, NULLIF(b.StartFirstOccurrence, 0)) * 100 != 0
    OR SAFE_DIVIDE(b.Viewables, NULLIF(b.ViewabilityMeasures, 0)) * 100 != 0
    OR SAFE_DIVIDE(b.Fills, NULLIF(b.NonZeroBids, 0)) != 0
    OR SAFE_DIVIDE(b.Impressions, NULLIF(b.Fills, 0)) != 0
    OR SAFE_DIVIDE(b.NonZeroBidsValueInUSD, NULLIF(b.NonZeroBids, 0)) != 0
    OR SAFE_DIVIDE(b.BidSpendInUSD, NULLIF(c.CPRMarketImpressions, 0)) != 0
    );

        """

        client.query(sql_query).result()
        print("✅ BigQuery job completed for 2025-04-16")

    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_bigquery_query,
    )

    run_query_task  # This line was added
