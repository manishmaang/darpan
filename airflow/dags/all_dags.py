"""
airflow/dags/daily_ingest.py
============================
Daily DAG: Run EC, MCA21, PFMS, GeM scrapers in order.
Runs at 02:00 IST every day.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

SCRAPERS_PATH = "/opt/airflow/scrapers"

default_args = {
    "owner": "vigilant",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": True,
    "email": ["alerts@vigilant.in"],
}

with DAG(
    dag_id="daily_ingest",
    description="Daily scraping: EC affidavits, MCA21, PFMS, GeM tenders",
    schedule_interval="0 20 * * *",  # 02:00 IST = 20:30 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["ingest", "daily"],
) as daily_ingest_dag:

    # ── Task 1: EC Affidavits ─────────────────────────────────────────────────
    # Only runs if new elections have been scheduled (checks ECI calendar)
    ec_scrape = BashOperator(
        task_id="scrape_ec_affidavits",
        bash_command=f"cd {SCRAPERS_PATH} && python ec_scraper.py --state all --year 2024",
        execution_timeout=timedelta(hours=4),
    )

    # ── Task 2: MCA21 Company Updates ─────────────────────────────────────────
    # Incremental: only fetches PANs whose companies haven't been updated in 7d
    mca21_fetch = BashOperator(
        task_id="fetch_mca21_companies",
        bash_command=f"cd {SCRAPERS_PATH} && python mca21_fetcher.py --from-affidavits",
        execution_timeout=timedelta(hours=3),
    )

    # ── Task 3: PFMS Fund Releases ────────────────────────────────────────────
    pfms_watch = BashOperator(
        task_id="watch_pfms_funds",
        bash_command=f"cd {SCRAPERS_PATH} && python pfms_gem_rera_rti.py --scraper pfms --lookback-days 30",
        execution_timeout=timedelta(hours=2),
    )

    # ── Task 4: GeM Tender Awards ─────────────────────────────────────────────
    gem_crawl = BashOperator(
        task_id="crawl_gem_tenders",
        bash_command=f"cd {SCRAPERS_PATH} && python pfms_gem_rera_rti.py --scraper gem --lookback-days 30",
        execution_timeout=timedelta(hours=2),
    )

    # Task order: EC and MCA run first (provide entity data), then PFMS+GeM
    ec_scrape >> mca21_fetch >> [pfms_watch, gem_crawl]


# =============================================================================

"""
airflow/dags/weekly_ingest.py
=============================
Weekly DAG: RERA and RTI scrapers.
Runs every Sunday at 03:00 IST.
"""

with DAG(
    dag_id="weekly_ingest",
    description="Weekly scraping: RERA land registry, RTI responses",
    schedule_interval="0 21 * * 0",  # 03:00 IST Sunday = 21:30 UTC Saturday
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["ingest", "weekly"],
) as weekly_ingest_dag:

    rera_scrape = BashOperator(
        task_id="scrape_rera",
        bash_command=f"cd {SCRAPERS_PATH} && python pfms_gem_rera_rti.py --scraper rera",
        execution_timeout=timedelta(hours=6),
    )

    rti_index = BashOperator(
        task_id="index_rti_responses",
        bash_command=f"cd {SCRAPERS_PATH} && python pfms_gem_rera_rti.py --scraper rti",
        execution_timeout=timedelta(hours=4),
    )

    rera_scrape >> rti_index


# =============================================================================

"""
airflow/dags/process_graph.py
==============================
Entity graph rebuild DAG.
Runs at 06:00 IST daily, AFTER daily_ingest completes.
"""

with DAG(
    dag_id="process_graph",
    description="Entity graph rebuild after daily ingestion",
    schedule_interval="0 0 * * *",  # 06:00 IST = 00:30 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["processing", "graph"],
) as process_graph_dag:

    build_graph = BashOperator(
        task_id="build_entity_graph",
        bash_command="cd /opt/airflow/engine && python entity_graph.py",
        execution_timeout=timedelta(hours=2),
    )

    # Optional: validate graph statistics after build
    validate_graph = BashOperator(
        task_id="validate_graph",
        bash_command="""
            python -c "
from neo4j import GraphDatabase
import os
driver = GraphDatabase.driver(os.environ['NEO4J_URI'],
    auth=('neo4j', os.environ['NEO4J_PASSWORD']))
with driver.session() as s:
    r = s.run('MATCH (n) RETURN labels(n)[0] AS label, COUNT(*) AS cnt')
    for rec in r: print(f'{rec[\"label\"]}: {rec[\"cnt\"]} nodes')
driver.close()
"
        """,
    )

    build_graph >> validate_graph


# =============================================================================

"""
airflow/dags/trace_and_score.py
================================
Fund tracing + scoring pipeline.
Runs at 07:00 IST daily, AFTER process_graph completes.
"""

with DAG(
    dag_id="trace_and_score",
    description="Fund flow tracing and politician risk scoring",
    schedule_interval="30 1 * * *",  # 07:00 IST = 01:30 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["processing", "scoring"],
) as trace_and_score_dag:

    # Task 1: Run fund tracer
    trace_funds = BashOperator(
        task_id="trace_fund_flows",
        bash_command="cd /opt/airflow/engine && python fund_tracer.py --lookback-days 365",
        execution_timeout=timedelta(hours=1),
    )

    # Task 2: Score all politicians
    score_politicians = BashOperator(
        task_id="score_politicians",
        bash_command="cd /opt/airflow/engine && python scorer.py --all",
        execution_timeout=timedelta(hours=1),
    )

    # Task 3: Invalidate API cache
    invalidate_cache = BashOperator(
        task_id="invalidate_api_cache",
        bash_command="""
            python -c "
import redis, os
r = redis.from_url(os.environ['REDIS_URL'])
keys = r.keys('vigilant:*')
if keys: r.delete(*keys)
print(f'Invalidated {len(keys)} cache keys')
"
        """,
    )

    # Task 4: Send alerts for new CRITICAL suspects
    send_alerts = BashOperator(
        task_id="send_new_critical_alerts",
        bash_command="""
            python -c "
import psycopg2, os, json
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
# Find politicians whose score crossed CRITICAL threshold today
cur.execute('''
    SELECT p.name_normalized, p.state, p.constituency, rs.total_score
    FROM risk_scores rs
    JOIN politicians p ON p.id = rs.politician_id
    WHERE rs.risk_classification = 'CRITICAL'
      AND rs.scored_at::date = CURRENT_DATE
    ORDER BY rs.total_score DESC
''')
alerts = cur.fetchall()
conn.close()
print(f'NEW CRITICAL SUSPECTS TODAY: {len(alerts)}')
for a in alerts:
    print(f'  {a[0]} ({a[1]}, {a[2]}) — Score: {a[3]}')
"
        """,
    )

    trace_funds >> score_politicians >> invalidate_cache >> send_alerts
