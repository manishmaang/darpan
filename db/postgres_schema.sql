-- =============================================================================
-- VIGILANT.IN — PostgreSQL Schema
-- Run: psql $DATABASE_URL < db/postgres_schema.sql
-- =============================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- Trigram similarity for fuzzy search

-- =============================================================================
-- POLITICIANS
-- Source: ec_scraper.py → Election Commission affidavits
-- =============================================================================

CREATE TABLE IF NOT EXISTS politicians (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ec_affidavit_id     VARCHAR(64) UNIQUE NOT NULL,  -- ECI internal ID
    name_raw            VARCHAR(255) NOT NULL,          -- As declared on affidavit
    name_normalized     VARCHAR(255),                  -- Cleaned by NLP
    pan                 VARCHAR(10),                   -- PAN card (anchor key)
    aadhaar_last4       VARCHAR(4),                    -- Last 4 digits only
    party               VARCHAR(100),
    state               VARCHAR(100) NOT NULL,
    constituency        VARCHAR(200) NOT NULL,
    election_year       SMALLINT NOT NULL,
    election_type       VARCHAR(20),                   -- 'LS', 'VS', 'RS'
    won_election        BOOLEAN,
    position_held       VARCHAR(200),                  -- Minister, MLA, MP etc.
    scraped_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT pan_format CHECK (pan ~ '^[A-Z]{5}[0-9]{4}[A-Z]$' OR pan IS NULL)
);

CREATE INDEX idx_politicians_pan ON politicians(pan);
CREATE INDEX idx_politicians_state ON politicians(state);
CREATE INDEX idx_politicians_name_trgm ON politicians USING gin(name_normalized gin_trgm_ops);

-- Family members declared in affidavit
CREATE TABLE IF NOT EXISTS politician_family (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    politician_id   UUID NOT NULL REFERENCES politicians(id) ON DELETE CASCADE,
    name_raw        VARCHAR(255) NOT NULL,
    name_normalized VARCHAR(255),
    relation        VARCHAR(50) NOT NULL,   -- 'spouse', 'child', 'parent', 'sibling'
    pan             VARCHAR(10),
    scraped_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_family_politician ON politician_family(politician_id);
CREATE INDEX idx_family_pan ON politician_family(pan);
CREATE INDEX idx_family_name_trgm ON politician_family USING gin(name_normalized gin_trgm_ops);

-- =============================================================================
-- ASSETS — Declared in EC affidavits (year over year)
-- Source: ec_scraper.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS politician_assets (
    id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    politician_id               UUID NOT NULL REFERENCES politicians(id) ON DELETE CASCADE,
    election_year               SMALLINT NOT NULL,
    -- Movable assets (in lakhs)
    cash_in_hand_lakh           NUMERIC(12,2),
    bank_deposits_lakh          NUMERIC(12,2),
    investments_lakh            NUMERIC(12,2),   -- stocks, bonds, MFs
    vehicles_lakh               NUMERIC(12,2),
    jewellery_lakh              NUMERIC(12,2),
    other_movable_lakh          NUMERIC(12,2),
    -- Immovable assets (in lakhs)
    agricultural_land_lakh      NUMERIC(12,2),
    residential_property_lakh   NUMERIC(12,2),
    commercial_property_lakh    NUMERIC(12,2),
    other_immovable_lakh        NUMERIC(12,2),
    -- Business interests (in lakhs)
    business_shares_lakh        NUMERIC(12,2),
    -- Liabilities
    total_liabilities_lakh      NUMERIC(12,2),
    -- Computed totals
    total_assets_lakh           NUMERIC(12,2) GENERATED ALWAYS AS (
        COALESCE(cash_in_hand_lakh,0) + COALESCE(bank_deposits_lakh,0) +
        COALESCE(investments_lakh,0) + COALESCE(vehicles_lakh,0) +
        COALESCE(jewellery_lakh,0) + COALESCE(other_movable_lakh,0) +
        COALESCE(agricultural_land_lakh,0) + COALESCE(residential_property_lakh,0) +
        COALESCE(commercial_property_lakh,0) + COALESCE(other_immovable_lakh,0) +
        COALESCE(business_shares_lakh,0)
    ) STORED,
    -- Declared income
    declared_annual_income_lakh NUMERIC(12,2),
    source_pdf_url              TEXT,
    scraped_at                  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(politician_id, election_year)
);
CREATE INDEX idx_assets_politician ON politician_assets(politician_id);

-- =============================================================================
-- COMPANIES — From MCA21 registry
-- Source: mca21_fetcher.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS companies (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cin                 VARCHAR(21) UNIQUE NOT NULL,   -- Corporate Identity Number
    name                VARCHAR(500) NOT NULL,
    name_normalized     VARCHAR(500),
    company_type        VARCHAR(100),                  -- 'Private', 'Public', 'LLP' etc.
    status              VARCHAR(50),                   -- 'Active', 'Struck Off', 'Dissolved'
    registration_date   DATE,
    state_of_reg        VARCHAR(100),
    registered_address  TEXT,
    pin_code            VARCHAR(6),
    authorized_capital  NUMERIC(15,2),
    paid_up_capital     NUMERIC(15,2),
    gst_number          VARCHAR(15),
    scraped_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_companies_cin ON companies(cin);
CREATE INDEX idx_companies_name_trgm ON companies USING gin(name_normalized gin_trgm_ops);

-- Company directors and shareholders
CREATE TABLE IF NOT EXISTS company_persons (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id      UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    din             VARCHAR(8),                -- Director Identification Number
    pan             VARCHAR(10),
    name_raw        VARCHAR(255) NOT NULL,
    name_normalized VARCHAR(255),
    role            VARCHAR(50) NOT NULL,       -- 'director', 'shareholder', 'promoter'
    share_pct       NUMERIC(6,3),
    appointed_date  DATE,
    ceased_date     DATE,
    is_active       BOOLEAN DEFAULT TRUE,
    scraped_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_cp_company ON company_persons(company_id);
CREATE INDEX idx_cp_pan ON company_persons(pan);
CREATE INDEX idx_cp_din ON company_persons(din);
CREATE INDEX idx_cp_name_trgm ON company_persons USING gin(name_normalized gin_trgm_ops);

-- =============================================================================
-- FUND RELEASES — From PFMS
-- Source: pfms_watcher.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS fund_releases (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pfms_ref_id         VARCHAR(100) UNIQUE,            -- PFMS internal reference
    scheme_code         VARCHAR(50),
    scheme_name         VARCHAR(500) NOT NULL,
    scheme_category     VARCHAR(100),                   -- 'NREGA', 'PMAY', 'SmartCity', etc.
    state               VARCHAR(100) NOT NULL,
    district            VARCHAR(200) NOT NULL,
    implementing_agency VARCHAR(500),
    amount_cr           NUMERIC(12,2) NOT NULL,         -- Amount in crores
    release_date        DATE NOT NULL,
    financial_year      VARCHAR(7),                     -- '2023-24'
    beneficiary_type    VARCHAR(100),
    scraped_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_fund_releases_district ON fund_releases(district);
CREATE INDEX idx_fund_releases_date ON fund_releases(release_date);
CREATE INDEX idx_fund_releases_scheme ON fund_releases(scheme_category);
CREATE INDEX idx_fund_releases_state ON fund_releases(state);

-- =============================================================================
-- TENDERS — From GeM portal and state portals
-- Source: gem_crawler.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS tenders (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tender_ref_id       VARCHAR(200) UNIQUE NOT NULL,   -- GeM/state tender ID
    source_portal       VARCHAR(50) DEFAULT 'gem',       -- 'gem', 'cppp', state name
    department          VARCHAR(500),
    category            VARCHAR(200),                    -- 'Construction', 'IT', 'Supply' etc.
    state               VARCHAR(100) NOT NULL,
    district            VARCHAR(200),
    tender_description  TEXT,
    -- Award details
    award_date          DATE NOT NULL,
    contract_value_cr   NUMERIC(12,2) NOT NULL,
    -- Winner details
    winner_name         VARCHAR(500) NOT NULL,
    winner_cin          VARCHAR(21),
    winner_gst          VARCHAR(15),
    winner_pan          VARCHAR(10),
    -- Status
    completion_status   VARCHAR(50),                     -- 'Completed', 'Ongoing', 'Cancelled'
    completion_pct      SMALLINT,
    scraped_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_tenders_district ON tenders(district);
CREATE INDEX idx_tenders_award_date ON tenders(award_date);
CREATE INDEX idx_tenders_winner_cin ON tenders(winner_cin);
CREATE INDEX idx_tenders_state ON tenders(state);

-- =============================================================================
-- RERA PROPERTIES — Land registrations
-- Source: rera_scraper.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS rera_properties (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rera_reg_no         VARCHAR(100) UNIQUE,
    project_name        VARCHAR(500),
    promoter_name       VARCHAR(500) NOT NULL,
    promoter_cin        VARCHAR(21),
    promoter_pan        VARCHAR(10),
    state               VARCHAR(100) NOT NULL,
    district            VARCHAR(200),
    location_address    TEXT,
    pin_code            VARCHAR(6),
    land_area_sqft      NUMERIC(12,2),
    declared_value_cr   NUMERIC(12,2),
    registration_date   DATE NOT NULL,
    scraped_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_rera_pan ON rera_properties(promoter_pan);
CREATE INDEX idx_rera_cin ON rera_properties(promoter_cin);
CREATE INDEX idx_rera_date ON rera_properties(registration_date);
CREATE INDEX idx_rera_district ON rera_properties(district);

-- =============================================================================
-- RTI FLAGS — Contradictions and disclosures
-- Source: rti_indexer.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS rti_flags (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rti_application_no  VARCHAR(100),
    filing_date         DATE,
    response_date       DATE,
    public_authority    VARCHAR(500),
    subject             TEXT,
    contractor_name     VARCHAR(500),                   -- Extracted by NLP
    fund_amount_cr      NUMERIC(12,2),                  -- Extracted by NLP
    contradiction_type  VARCHAR(100),                   -- 'hidden_contractor', 'incomplete_work', etc.
    contradiction_detail TEXT,
    source_url          TEXT,
    scraped_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_rti_contractor_trgm ON rti_flags USING gin(contractor_name gin_trgm_ops);

-- =============================================================================
-- ENTITY LINKS — Computed by entity_graph.py
-- Stores resolved politician → company links with confidence scores
-- =============================================================================

CREATE TABLE IF NOT EXISTS entity_links (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    politician_id       UUID NOT NULL REFERENCES politicians(id),
    company_id          UUID NOT NULL REFERENCES companies(id),
    link_type           VARCHAR(50) NOT NULL,    -- 'direct', 'family', 'associate', 'shell'
    relation_via        VARCHAR(255),             -- 'spouse', 'son', 'nominee', etc.
    confidence          NUMERIC(4,3) NOT NULL,    -- 0.0 to 1.0
    evidence_sources    TEXT[],                  -- Array of source IDs
    graph_depth         SMALLINT DEFAULT 1,       -- Hops from politician to company
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(politician_id, company_id)
);
CREATE INDEX idx_el_politician ON entity_links(politician_id);
CREATE INDEX idx_el_company ON entity_links(company_id);
CREATE INDEX idx_el_confidence ON entity_links(confidence);

-- =============================================================================
-- FUND TRAILS — Computed by fund_tracer.py
-- The key output: confirmed public fund → linked company correlations
-- =============================================================================

CREATE TABLE IF NOT EXISTS fund_trails (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    politician_id       UUID NOT NULL REFERENCES politicians(id),
    fund_release_id     UUID NOT NULL REFERENCES fund_releases(id),
    tender_id           UUID NOT NULL REFERENCES tenders(id),
    company_id          UUID NOT NULL REFERENCES companies(id),
    -- Correlation details
    lag_days            SMALLINT NOT NULL,               -- Days between fund release and tender
    amount_match_pct    NUMERIC(5,2),                    -- How closely amounts match (%)
    district_match      BOOLEAN DEFAULT TRUE,
    -- Risk classification
    risk_tier           VARCHAR(10) NOT NULL,             -- 'CRITICAL', 'HIGH', 'MEDIUM'
    risk_score_contrib  SMALLINT,                         -- Points contributed to politician score
    -- Evidence narrative
    evidence_summary    TEXT,
    -- Audit
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(fund_release_id, tender_id)
);
CREATE INDEX idx_ft_politician ON fund_trails(politician_id);
CREATE INDEX idx_ft_risk ON fund_trails(risk_tier);

-- =============================================================================
-- RISK SCORES — Final output of scorer.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS risk_scores (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    politician_id           UUID NOT NULL REFERENCES politicians(id),
    -- Component scores
    score_asset_growth      SMALLINT DEFAULT 0 CHECK (score_asset_growth BETWEEN 0 AND 25),
    score_tender_linkage    SMALLINT DEFAULT 0 CHECK (score_tender_linkage BETWEEN 0 AND 25),
    score_fund_flow         SMALLINT DEFAULT 0 CHECK (score_fund_flow BETWEEN 0 AND 20),
    score_land_reg          SMALLINT DEFAULT 0 CHECK (score_land_reg BETWEEN 0 AND 15),
    score_rti_contradiction SMALLINT DEFAULT 0 CHECK (score_rti_contradiction BETWEEN 0 AND 10),
    score_network_depth     SMALLINT DEFAULT 0 CHECK (score_network_depth BETWEEN 0 AND 5),
    -- Total
    total_score             SMALLINT GENERATED ALWAYS AS (
        score_asset_growth + score_tender_linkage + score_fund_flow +
        score_land_reg + score_rti_contradiction + score_network_depth
    ) STORED,
    risk_classification     VARCHAR(20),  -- 'CRITICAL', 'HIGH', 'WATCH', 'LOW'
    -- Explanation data (JSON)
    score_reasons           JSONB,
    raw_metrics             JSONB,
    -- Metadata
    scored_at               TIMESTAMPTZ DEFAULT NOW(),
    score_version           VARCHAR(10) DEFAULT 'v1.0',
    UNIQUE(politician_id)
);
CREATE INDEX idx_scores_total ON risk_scores(total_score DESC);
CREATE INDEX idx_scores_classification ON risk_scores(risk_classification);

-- =============================================================================
-- AUDIT LOG — Every automated action logged
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    module      VARCHAR(100) NOT NULL,
    action      VARCHAR(200) NOT NULL,
    entity_type VARCHAR(100),
    entity_id   UUID,
    details     JSONB,
    status      VARCHAR(20),   -- 'success', 'error', 'warning'
    error_msg   TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_audit_module ON audit_log(module);
CREATE INDEX idx_audit_created ON audit_log(created_at DESC);

-- =============================================================================
-- USEFUL VIEWS
-- =============================================================================

-- Full politician risk summary
CREATE OR REPLACE VIEW v_politician_risk_summary AS
SELECT
    p.id,
    p.name_normalized AS name,
    p.pan,
    p.party,
    p.state,
    p.constituency,
    p.election_year,
    p.position_held,
    rs.total_score,
    rs.risk_classification,
    rs.score_asset_growth,
    rs.score_tender_linkage,
    rs.score_fund_flow,
    rs.score_land_reg,
    rs.score_rti_contradiction,
    rs.score_network_depth,
    rs.scored_at,
    -- Asset stats
    latest_assets.total_assets_lakh AS latest_assets_lakh,
    earliest_assets.total_assets_lakh AS earliest_assets_lakh,
    ROUND(
        CASE WHEN earliest_assets.total_assets_lakh > 0
        THEN ((latest_assets.total_assets_lakh - earliest_assets.total_assets_lakh)
              / earliest_assets.total_assets_lakh * 100)
        ELSE 0 END, 1
    ) AS asset_growth_pct,
    -- Fund trail count
    (SELECT COUNT(*) FROM fund_trails ft WHERE ft.politician_id = p.id) AS fund_trail_count,
    -- Linked company count
    (SELECT COUNT(*) FROM entity_links el WHERE el.politician_id = p.id) AS linked_company_count
FROM politicians p
LEFT JOIN risk_scores rs ON rs.politician_id = p.id
LEFT JOIN LATERAL (
    SELECT total_assets_lakh FROM politician_assets
    WHERE politician_id = p.id ORDER BY election_year DESC LIMIT 1
) latest_assets ON TRUE
LEFT JOIN LATERAL (
    SELECT total_assets_lakh FROM politician_assets
    WHERE politician_id = p.id ORDER BY election_year ASC LIMIT 1
) earliest_assets ON TRUE;

-- Active fund trails with full context
CREATE OR REPLACE VIEW v_active_fund_trails AS
SELECT
    ft.id AS trail_id,
    p.name_normalized AS politician_name,
    p.constituency,
    p.state,
    fr.scheme_name,
    fr.amount_cr AS fund_amount_cr,
    fr.release_date,
    t.winner_name AS company_name,
    t.contract_value_cr AS tender_value_cr,
    t.award_date,
    ft.lag_days,
    ft.risk_tier,
    el.link_type,
    el.relation_via,
    el.confidence AS entity_link_confidence
FROM fund_trails ft
JOIN politicians p ON p.id = ft.politician_id
JOIN fund_releases fr ON fr.id = ft.fund_release_id
JOIN tenders t ON t.id = ft.tender_id
JOIN entity_links el ON el.politician_id = ft.politician_id AND el.company_id = ft.company_id
ORDER BY ft.risk_tier, ft.lag_days;
