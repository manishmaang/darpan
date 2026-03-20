"""
db/neo4j_schema.py
==================
Sets up Neo4j constraints and indexes for the VIGILANT.IN entity graph.
Run once during setup: python db/neo4j_schema.py
"""

import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()


def setup_schema(driver):
    """Create all constraints and indexes in Neo4j."""
    with driver.session() as session:

        # ── Constraints (also create unique indexes automatically) ──────────

        constraints = [
            # Politician node — PAN is the universal anchor
            "CREATE CONSTRAINT politician_pan IF NOT EXISTS FOR (p:Politician) REQUIRE p.pan IS UNIQUE",
            "CREATE CONSTRAINT politician_id IF NOT EXISTS FOR (p:Politician) REQUIRE p.pg_id IS UNIQUE",

            # Company node — CIN is the unique identifier
            "CREATE CONSTRAINT company_cin IF NOT EXISTS FOR (c:Company) REQUIRE c.cin IS UNIQUE",

            # Individual node — DIN for directors
            "CREATE CONSTRAINT individual_din IF NOT EXISTS FOR (i:Individual) REQUIRE i.din IS UNIQUE",

            # FundRelease — PFMS reference
            "CREATE CONSTRAINT fund_release_ref IF NOT EXISTS FOR (f:FundRelease) REQUIRE f.pfms_ref_id IS UNIQUE",

            # Tender — GeM/portal tender ID
            "CREATE CONSTRAINT tender_ref IF NOT EXISTS FOR (t:Tender) REQUIRE t.tender_ref_id IS UNIQUE",

            # Property — RERA registration number
            "CREATE CONSTRAINT property_rera IF NOT EXISTS FOR (p:Property) REQUIRE p.rera_reg_no IS UNIQUE",
        ]

        for constraint in constraints:
            try:
                session.run(constraint)
                print(f"  ✓ Constraint: {constraint[:60]}...")
            except Exception as e:
                print(f"  ⚠ Skipped (likely exists): {e}")

        # ── Additional indexes for query performance ─────────────────────────

        indexes = [
            "CREATE INDEX politician_name IF NOT EXISTS FOR (p:Politician) ON (p.name_normalized)",
            "CREATE INDEX politician_state IF NOT EXISTS FOR (p:Politician) ON (p.state)",
            "CREATE INDEX politician_party IF NOT EXISTS FOR (p:Politician) ON (p.party)",
            "CREATE INDEX company_name IF NOT EXISTS FOR (c:Company) ON (c.name_normalized)",
            "CREATE INDEX company_state IF NOT EXISTS FOR (c:Company) ON (c.state_of_reg)",
            "CREATE INDEX individual_name IF NOT EXISTS FOR (i:Individual) ON (i.name_normalized)",
            "CREATE INDEX individual_pan IF NOT EXISTS FOR (i:Individual) ON (i.pan)",
            "CREATE INDEX tender_district IF NOT EXISTS FOR (t:Tender) ON (t.district)",
            "CREATE INDEX tender_date IF NOT EXISTS FOR (t:Tender) ON (t.award_date)",
            "CREATE INDEX fund_district IF NOT EXISTS FOR (f:FundRelease) ON (f.district)",
            "CREATE INDEX fund_date IF NOT EXISTS FOR (f:FundRelease) ON (f.release_date)",
        ]

        for index in indexes:
            try:
                session.run(index)
                print(f"  ✓ Index: {index[:60]}...")
            except Exception as e:
                print(f"  ⚠ Skipped: {e}")

        print("\n✅ Neo4j schema setup complete.")


# ── Node and relationship type reference ─────────────────────────────────────
#
# NODE TYPES:
#   (:Politician)  — pan, pg_id, name_raw, name_normalized, party, state,
#                    constituency, election_year, position_held, risk_score
#
#   (:Company)     — cin, name_raw, name_normalized, company_type, status,
#                    registration_date, state_of_reg, registered_address,
#                    authorized_capital, paid_up_capital, gst_number
#
#   (:Individual)  — din, pan, name_raw, name_normalized, role
#
#   (:FundRelease) — pfms_ref_id, scheme_name, scheme_category, state,
#                    district, amount_cr, release_date, financial_year
#
#   (:Tender)      — tender_ref_id, source_portal, department, category,
#                    state, district, award_date, contract_value_cr,
#                    winner_name, winner_cin, completion_status
#
#   (:Property)    — rera_reg_no, project_name, state, district,
#                    declared_value_cr, registration_date
#
# RELATIONSHIP TYPES:
#   (Politician)-[:IS_DIRECTOR {since, confidence, appointed_date}]->(Company)
#   (Politician)-[:IS_SHAREHOLDER {share_pct, confidence}]->(Company)
#   (Individual)-[:IS_DIRECTOR {since, confidence}]->(Company)
#   (Individual)-[:IS_SHAREHOLDER {share_pct}]->(Company)
#   (Individual)-[:FAMILY_OF {relation, confidence}]->(Politician)
#   (Company)-[:RECEIVED_TENDER {match_confidence}]->(Tender)
#   (Tender)-[:FUNDED_BY {lag_days, amount_match_pct}]->(FundRelease)
#   (Company)-[:OWNS_PROPERTY {acquisition_date}]->(Property)
#   (Individual)-[:OWNS_PROPERTY {acquisition_date}]->(Property)
#   (Company)-[:SUBSIDIARY_OF {depth}]->(Company)
#   (Company)-[:SHARES_DIRECTOR_WITH]->(Company)  # inferred relationship
#
# ─────────────────────────────────────────────────────────────────────────────

EXAMPLE_QUERIES = """
-- Find all companies 2 hops from a politician
MATCH (p:Politician {pan: 'XXXXX0000X'})-[*1..2]-(c:Company)
RETURN p.name_normalized, c.name_normalized, c.cin;

-- Find full corruption trail for a politician
MATCH (p:Politician)-[:IS_DIRECTOR|IS_SHAREHOLDER*1..3]-(c:Company)
      -[:RECEIVED_TENDER]->(t:Tender)
      -[:FUNDED_BY]->(f:FundRelease)
WHERE p.pan = 'XXXXX0000X'
RETURN p.name_normalized, c.name_normalized, t.contract_value_cr,
       f.amount_cr, f.release_date, t.award_date,
       duration.between(f.release_date, t.award_date).days AS lag_days;

-- Find all family-linked companies
MATCH (p:Politician)-[:FAMILY_OF*1..1]-(i:Individual)
      -[:IS_DIRECTOR]->(c:Company)
WHERE p.state = 'Maharashtra'
RETURN p.name_normalized, i.name_normalized, i.relation, c.name_normalized;

-- Detect shell company chains
MATCH chain = (p:Politician)-[:IS_DIRECTOR]->(c1:Company)
              -[:SUBSIDIARY_OF*1..4]->(c2:Company)
              -[:RECEIVED_TENDER]->(t:Tender)
WHERE p.risk_score > 50
RETURN p.name_normalized, length(chain) AS chain_depth,
       c2.name_normalized, t.contract_value_cr
ORDER BY chain_depth DESC;
"""


if __name__ == "__main__":
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")

    if not password:
        raise ValueError("NEO4J_PASSWORD environment variable not set")

    print(f"Connecting to Neo4j at {uri}...")
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        driver.verify_connectivity()
        print("✓ Connected to Neo4j\n")
        setup_schema(driver)
    finally:
        driver.close()
