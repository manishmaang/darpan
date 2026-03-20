# DARPAN.IN — Political Corruption Intelligence Platform

A civic-tech system that connects Indian public databases to flag politicians as corruption suspects by tracking fund movements from government schemes to politician-linked businesses via tenders and land records.

**All data is legally public** under: RTI Act 2005, Companies Act 2013, Representation of the People Act 1951, RERA Act 2016, PFMS public dashboard, and the Supreme Court 2002 order on mandatory candidate asset disclosure.

---

## Architecture

```
Public Databases → Scrapers → PostgreSQL + Neo4j → Engine → FastAPI → React UI
```

**4 layers:**
1. **Ingestion** — 8 Python scrapers (6 live, 2 supplementary)
2. **Storage** — PostgreSQL (relational) + Neo4j (entity graph) + Redis (cache/queue)
3. **Processing** — Entity graph builder → Identity resolver → Fund tracer → Scorer
4. **Presentation** — FastAPI REST API + React spider web UI

---

## Project Structure

```
darpan/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── Dockerfile.api
├── .env.example
├── darpan_cli.py              ← Admin CLI (11 commands)
│
├── db/
│   ├── postgres_schema.sql      ← 9 tables + 2 views + all indexes
│   └── neo4j_schema.py          ← Graph constraints and indexes
│
├── scrapers/
│   ├── base_scraper.py          ← Base class: retry, rate limiting, audit logging
│   ├── ec_scraper.py            ← Election Commission affidavit PDF scraper
│   ├── mca21_fetcher.py         ← MCA21 company + director fetcher (4-hop recursive)
│   ├── pfms_gem_rera_rti.py     ← PFMS + GeM + RERA + RTI scrapers (combined)
│   └── pan_resolver_pmla.py     ← PAN resolver + ED/PMLA press release checker
│
├── engine/
│   ├── entity_graph.py          ← Neo4j graph builder
│   ├── identity_resolver.py     ← Cross-dataset deduplication
│   ├── fund_tracer.py           ← Temporal correlation engine
│   └── scorer.py                ← 6-factor 100-point risk scorer
│
├── api/
│   ├── main.py                  ← FastAPI application (9 endpoints)
│   ├── models.py                ← Pydantic models
│   └── database.py              ← Connection pooling
│
├── airflow/
│   └── dags/
│       └── all_dags.py          ← 4 scheduled DAGs
│
├── frontend/
│   └── darpan-in.jsx          ← React spider web UI
│
└── tests/
    ├── test_scorer.py            ← 31 tests
    ├── test_entity_graph.py      ← 20 tests
    ├── test_fund_tracer.py       ← 35 tests
    └── fixtures/
        └── sample_data.py        ← Test data factories
```

---

## Quick Start

```bash
cp .env.example .env               # Configure passwords
docker-compose up -d               # Start Postgres + Neo4j + Redis + API
psql $DATABASE_URL < db/postgres_schema.sql
python db/neo4j_schema.py
python darpan_cli.py scrape --source ec --state Maharashtra --year 2024
python darpan_cli.py scrape --source mca21
python darpan_cli.py scrape --source pfms
python darpan_cli.py scrape --source gem
python darpan_cli.py pipeline    # resolve → trace → score
```

---

## Admin CLI

```bash
python darpan_cli.py status
python darpan_cli.py stats
python darpan_cli.py report --top 20
python darpan_cli.py report --politician "Patil"
python darpan_cli.py scrape --source all
python darpan_cli.py pipeline
python darpan_cli.py export --format csv
```

---

## Scoring (0–100)

| Criterion | Max |
|---|---|
| Asset Growth Anomaly | 25 |
| Tender-to-Relative Linkage | 25 |
| Fund Flow Correlation | 20 |
| Land Registration Spike | 15 |
| RTI Contradiction | 10 |
| Network Depth (Shell Companies) | 5 |

CRITICAL ≥75 · HIGH ≥50 · WATCH ≥30 · LOW <30

---

## API Endpoints

`GET /api/stats` · `/api/politicians` · `/api/politicians/{id}` · `/api/politicians/{id}/score` · `/api/politicians/{id}/trails` · `/api/politicians/{id}/graph` · `/api/fund-trails` · `/api/search`

---

## Tests

86 tests covering all core algorithms. All pure-logic assertions verified passing.

```bash
pytest tests/ -v
```

---

## Legal Notice

DARPAN.IN only collects data legally required to be publicly disclosed under Indian law. All politician asset declarations are mandatory under *Union of India v. Association for Democratic Reforms* (Supreme Court, 2002).
