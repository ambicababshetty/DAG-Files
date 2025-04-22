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
        
        INSERT INTO `ox-wissp-devint.enriched.enriched_domain_performance_fact` (
 RewrittenPageURLDomain,
 domain_id,
 ReportDay,
 IABDistributionChannel,
 IABMediaSubtype,
 UserGeoCountry,
 VideoPlcmt,
 ClickThroughRate,
 TotalSpendInUSD,
 PublisherRevenueInUSD,
 RevenuePerImpression,
 CostPerImpression,
 TotalMarketRequests,
 TotalAdRequests,
 TotalExchangeFills,
 TotalImpressions,
 PrivateMarketImpressions,
 TotalClicks,
 VTR,
 Viewability,
 WinRate,
 BidRate,
 bid_cpm,
 BidGuidance,
 created_at,
 last_modified_at
)
WITH domain_meta AS (
 SELECT DISTINCT
  domain_name,
  uid AS domain_id
 FROM `ox-wissp-devint.enriched.enriched_domain_dim`
),

cpr_base AS (
 SELECT
  cpr.RewrittenPageURLDomain,
  cpr.ReportDay,
  cpr.IABDistributionChannel,
  cpr.IABMediaSubtype,
  cpr.UserGeoCountry,
  cpr.VideoPlcmt,
  cpr.IABVideoPlacementSubtype,
  SAFE_DIVIDE(SUM(cpr.Clicks), NULLIF(SUM(cpr.ImpressionsWithClickMacro), 0)) AS ClickThroughRate,
  SUM(cpr.CPRMarketDemandPartnerSpendInUSD) AS TotalSpendInUSD,
  SUM(cpr.CPRMarketPublisherRevenueInUSD) AS PublisherRevenueInUSD,
  SAFE_DIVIDE(SUM(cpr.CPRMarketPublisherRevenueInUSD), NULLIF(SUM(cpr.CPRMarketImpressions), 0)) AS RevenuePerImpression,
  SAFE_DIVIDE(SUM(cpr.CPRMarketDemandPartnerSpendInUSD), NULLIF(SUM(cpr.CPRMarketImpressions), 0)) AS CostPerImpression,
  SUM(cpr.CPRMarketRequests) AS TotalMarketRequests,
  SUM(cpr.CPRAllRequests) AS TotalAdRequests,
  SUM(cpr.CPRExchangeFills) AS TotalExchangeFills,
  SUM(cpr.CPRMarketImpressions) AS TotalImpressions,
  SUM(cpr.CPRPrivateMarketImpressions) AS PrivateMarketImpressions,
  SUM(cpr.Clicks) AS TotalClicks
 FROM `ox-wissp-devint.wissp_views.exchange_views_cpr_daily_fact_v2` cpr
 INNER JOIN domain_meta dm ON cpr.RewrittenPageURLDomain = dm.domain_name
 WHERE cpr.ReportDay = '2025-04-16'
 GROUP BY cpr.RewrittenPageURLDomain, cpr.ReportDay, cpr.IABDistributionChannel, cpr.IABMediaSubtype, cpr.UserGeoCountry, cpr.VideoPlcmt, cpr.IABVideoPlacementSubtype
),

bid_base AS (
 SELECT
  bid.RewrittenPageURLDomain,
  bid.ReportDay,
  bid.UserGeoCountry,
  bid.IABMediaSubtype,
  bid.IABDistributionChannel,
  bid.IABVideoPlacementSubtype,
  SAFE_DIVIDE(SUM(bid.CompleteFirstOccurrence), NULLIF(SUM(bid.StartFirstOccurrence), 0)) * 100 AS VTR,
  SAFE_DIVIDE(SUM(bid.Viewables), NULLIF(SUM(bid.ViewabilityMeasures), 0)) * 100 AS Viewability,
  SAFE_DIVIDE(SUM(bid.Fills), NULLIF(SUM(bid.NonZeroBids), 0)) AS WinRate,
  SAFE_DIVIDE(SUM(bid.NonZeroBids), NULLIF(SUM(bid.DealVolumeDealInfo), 0)) AS BidRate,
  SAFE_DIVIDE(SUM(bid.NonZeroBidsValueInUSD), NULLIF(SUM(bid.NonZeroBids), 0)) AS bid_cpm,
  SUM(bid.SpendInUSD) AS BidSpendInUSD
 FROM `ox-wissp-devint.wissp_views.exchange_views_bid_performance_daily_fact` bid
 INNER JOIN domain_meta dm ON bid.RewrittenPageURLDomain = dm.domain_name
 WHERE bid.ReportDay = '2025-04-16'
 GROUP BY bid.RewrittenPageURLDomain, bid.ReportDay, bid.UserGeoCountry, bid.IABMediaSubtype, bid.IABDistributionChannel, bid.IABVideoPlacementSubtype
)

SELECT DISTINCT
 dm.domain_name AS RewrittenPageURLDomain,
 dm.domain_id,  
 bid.ReportDay,
 bid.IABDistributionChannel,
 bid.IABMediaSubtype,
 bid.UserGeoCountry,
 cpr.VideoPlcmt,
 COALESCE(cpr.ClickThroughRate, 0.0),
 COALESCE(cpr.TotalSpendInUSD, 0.0),
 COALESCE(cpr.PublisherRevenueInUSD, 0.0),
 COALESCE(cpr.RevenuePerImpression, 0.0),
 COALESCE(cpr.CostPerImpression, 0.0),
 COALESCE(cpr.TotalMarketRequests, 0),
 COALESCE(cpr.TotalAdRequests, 0),
 COALESCE(cpr.TotalExchangeFills, 0),
 COALESCE(cpr.TotalImpressions, 0),
 COALESCE(cpr.PrivateMarketImpressions, 0),
 COALESCE(cpr.TotalClicks, 0),
 COALESCE(bid.VTR, 0.0),
 COALESCE(bid.Viewability, 0.0),
 COALESCE(bid.WinRate, 0.0),
 COALESCE(bid.BidRate, 0.0),
 COALESCE(bid.bid_cpm, 0.0),
 SAFE_DIVIDE(bid.BidSpendInUSD, NULLIF(cpr.TotalImpressions, 0)),
 CURRENT_TIMESTAMP() AS created_at,
 CURRENT_TIMESTAMP() AS last_modified_at
FROM domain_meta dm
INNER JOIN bid_base bid ON dm.domain_name = bid.RewrittenPageURLDomain
LEFT JOIN cpr_base cpr ON dm.domain_name = cpr.RewrittenPageURLDomain
 AND cpr.ReportDay = bid.ReportDay
 AND cpr.UserGeoCountry = bid.UserGeoCountry
 AND cpr.IABMediaSubtype = bid.IABMediaSubtype
 AND cpr.IABDistributionChannel = bid.IABDistributionChannel
 AND cpr.IABVideoPlacementSubtype = bid.IABVideoPlacementSubtype
WHERE
 COALESCE(cpr.ClickThroughRate, 0.0) != 0
 OR COALESCE(cpr.TotalSpendInUSD, 0.0) != 0
 OR COALESCE(cpr.PublisherRevenueInUSD, 0.0) != 0
 OR COALESCE(cpr.RevenuePerImpression, 0.0) != 0
 OR COALESCE(cpr.CostPerImpression, 0.0) != 0
 OR COALESCE(cpr.TotalMarketRequests, 0) != 0
 OR COALESCE(cpr.TotalAdRequests, 0) != 0
 OR COALESCE(cpr.TotalExchangeFills, 0) != 0
 OR COALESCE(cpr.TotalImpressions, 0) != 0
 OR COALESCE(cpr.PrivateMarketImpressions, 0) != 0
 OR COALESCE(cpr.TotalClicks, 0) != 0
 OR COALESCE(bid.VTR, 0.0) != 0
 OR COALESCE(bid.Viewability, 0.0) != 0
 OR COALESCE(bid.WinRate, 0.0) != 0
 OR COALESCE(bid.BidRate, 0.0) != 0
 OR COALESCE(bid.bid_cpm, 0.0) != 0
 OR SAFE_DIVIDE(bid.BidSpendInUSD, NULLIF(cpr.TotalImpressions, 0)) != 0;
"""

    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_bigquery_query,
    )

    run_query_task # This line was added
