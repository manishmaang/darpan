#!/usr/bin/env python3
"""
vigilant_cli.py
===============
Admin command-line interface for DARPAN.IN operations.
Provides all management commands in one place.

Usage:
    python vigilant_cli.py status
    python vigilant_cli.py scrape --source ec --state Maharashtra --year 2024
    python vigilant_cli.py build-graph
    python vigilant_cli.py score --all
    python vigilant_cli.py score --politician-id <uuid>
    python vigilant_cli.py report --top 20
    python vigilant_cli.py report --politician "Rajendra Patil"
    python vigilant_cli.py check-pan-conflicts
    python vigilant_cli.py stats
    python vigilant_cli.py reset --confirm
"""

import os
import sys
import json
import argparse
import logging
from datetime import date, datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ANSI colors for terminal output
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

RISK_COLORS = {
    "CRITICAL": RED + BOLD,
    "HIGH": YELLOW,
    "WATCH": CYAN,
    "LOW": GREEN,
}

logging.basicConfig(level=logging.WARNING)


def get_db():
    return psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor)


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_status(args):
    """Show system status — database row counts, last run times."""
    print(f"\n{BOLD}DARPAN.IN — System Status{RESET}")
    print("=" * 50)

    try:
        conn = get_db()
        with conn.cursor() as cur:
            tables = [
                "politicians", "politician_family", "politician_assets",
                "companies", "company_persons", "fund_releases", "tenders",
                "rera_properties", "rti_flags", "entity_links",
                "fund_trails", "risk_scores", "audit_log",
            ]
            for table in tables:
                cur.execute(f"SELECT COUNT(*) AS cnt FROM {table}")
                count = cur.fetchone()["cnt"]
                bar = "█" * min(50, count // max(1, count // 20)) if count else ""
                print(f"  {table:25s} {count:>8,d}  {DIM}{bar}{RESET}")

            # Last scoring run
            cur.execute("SELECT MAX(scored_at) AS last FROM risk_scores")
            last_scored = cur.fetchone()["last"]

            # Last scrape
            cur.execute("""
                SELECT module, MAX(created_at) AS last
                FROM audit_log WHERE action = 'scrape_complete'
                GROUP BY module ORDER BY last DESC LIMIT 5
            """)
            last_scrapes = cur.fetchall()

        print(f"\n{BOLD}Last Activity:{RESET}")
        print(f"  Last scoring:  {last_scored or 'Never'}")
        for s in last_scrapes:
            print(f"  {s['module']:20s} {s['last']}")

        conn.close()
        print(f"\n{GREEN}✓ Database connection OK{RESET}")

    except Exception as e:
        print(f"\n{RED}✗ Database error: {e}{RESET}")
        sys.exit(1)


def cmd_stats(args):
    """Show corruption intelligence statistics."""
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM politicians) AS total_pols,
                (SELECT COUNT(*) FROM risk_scores WHERE risk_classification='CRITICAL') AS critical,
                (SELECT COUNT(*) FROM risk_scores WHERE risk_classification='HIGH') AS high,
                (SELECT COUNT(*) FROM risk_scores WHERE risk_classification='WATCH') AS watch,
                (SELECT COUNT(*) FROM risk_scores WHERE risk_classification='LOW') AS low,
                (SELECT COUNT(*) FROM fund_trails) AS trails,
                (SELECT COUNT(*) FROM fund_trails WHERE risk_tier='CRITICAL') AS critical_trails,
                (SELECT COALESCE(SUM(contract_value_cr),0) FROM tenders
                 WHERE winner_cin IN (SELECT cin FROM companies c
                    JOIN entity_links el ON el.company_id = c.id)) AS flagged_value,
                (SELECT COUNT(DISTINCT state) FROM politicians) AS states
        """)
        s = cur.fetchone()

    print(f"\n{BOLD}DARPAN.IN — Intelligence Summary{RESET}")
    print("=" * 50)
    print(f"  Politicians tracked:      {s['total_pols']:>8,d}")
    print(f"  States covered:           {s['states']:>8,d}")
    print()
    print(f"  {RED+BOLD}CRITICAL suspects:        {s['critical']:>8,d}{RESET}")
    print(f"  {YELLOW}HIGH risk:                {s['high']:>8,d}{RESET}")
    print(f"  {CYAN}WATCH list:               {s['watch']:>8,d}{RESET}")
    print(f"  {GREEN}LOW risk / clean:         {s['low']:>8,d}{RESET}")
    print()
    print(f"  Fund trails detected:     {s['trails']:>8,d}")
    print(f"  Critical fund trails:     {s['critical_trails']:>8,d}")
    print(f"  Flagged tender value:   ₹{float(s['flagged_value'] or 0):>8,.1f} Cr")

    conn.close()


def cmd_report(args):
    """Print top-N politicians by risk score, or a single politician's full report."""
    conn = get_db()

    if args.politician:
        _report_single_politician(conn, args.politician)
    else:
        _report_top_politicians(conn, args.top)

    conn.close()


def _report_top_politicians(conn, top_n: int):
    """Print top politicians by risk score."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.name_normalized, p.party, p.state, p.constituency,
                   rs.total_score, rs.risk_classification,
                   rs.score_asset_growth, rs.score_tender_linkage,
                   rs.score_fund_flow, rs.score_land_reg,
                   rs.score_rti_contradiction, rs.score_network_depth,
                   (SELECT COALESCE(SUM(contract_value_cr), 0)
                    FROM tenders t JOIN entity_links el ON t.winner_cin =
                        (SELECT cin FROM companies WHERE id = el.company_id)
                    WHERE el.politician_id = p.id) AS linked_tender_cr
            FROM politicians p
            JOIN risk_scores rs ON rs.politician_id = p.id
            ORDER BY rs.total_score DESC
            LIMIT %s
        """, (top_n,))
        rows = cur.fetchall()

    print(f"\n{BOLD}Top {top_n} Politicians by Risk Score{RESET}")
    print("=" * 90)
    print(f"  {'Name':30s} {'Party':12s} {'State':12s} {'Score':6s} {'Classification':12s} {'Tender₹Cr':10s}")
    print(f"  {'-'*30} {'-'*12} {'-'*12} {'-'*6} {'-'*12} {'-'*10}")

    for r in rows:
        color = RISK_COLORS.get(r["risk_classification"], "")
        score_bar = "▓" * (r["total_score"] // 5)
        print(
            f"  {color}{r['name_normalized']:30s} "
            f"{(r['party'] or 'N/A'):12s} "
            f"{r['state']:12s} "
            f"{r['total_score']:3d}/100 "
            f"{(r['risk_classification'] or ''):12s}{RESET} "
            f"₹{float(r['linked_tender_cr'] or 0):>8.1f}Cr"
        )


def _report_single_politician(conn, name_query: str):
    """Print full report for a single politician."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.*, rs.*, rs.total_score AS score
            FROM politicians p
            LEFT JOIN risk_scores rs ON rs.politician_id = p.id
            WHERE p.name_normalized ILIKE %s
            ORDER BY rs.total_score DESC NULLS LAST
            LIMIT 1
        """, (f"%{name_query.upper()}%",))
        pol = cur.fetchone()

    if not pol:
        print(f"{RED}Politician not found: {name_query}{RESET}")
        return

    color = RISK_COLORS.get(pol["risk_classification"], "")
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}{pol['name_normalized']}{RESET}")
    print(f"  Party: {pol['party']}  |  State: {pol['state']}")
    print(f"  Constituency: {pol['constituency']}")
    print(f"  Position: {pol.get('position_held', 'N/A')}")
    print(f"  PAN: {pol.get('pan', 'Not disclosed')}")
    print()
    print(f"  {color}RISK SCORE: {pol['score']}/100 — {pol['risk_classification']}{RESET}")
    print()
    print(f"  Score Breakdown:")
    components = [
        ("Asset Growth Anomaly", "score_asset_growth", 25),
        ("Tender-to-Relative Linkage", "score_tender_linkage", 25),
        ("Fund Flow Correlation", "score_fund_flow", 20),
        ("Land Registration Spike", "score_land_reg", 15),
        ("RTI Contradiction", "score_rti_contradiction", 10),
        ("Network Depth (Shell Cos)", "score_network_depth", 5),
    ]
    for label, key, max_score in components:
        val = pol.get(key, 0) or 0
        bar = "█" * val + "░" * (max_score - val)
        print(f"    {label:30s} {val:2d}/{max_score}  {bar}")

    # Reasons
    reasons = pol.get("score_reasons") or {}
    if reasons:
        print(f"\n  Evidence:")
        for criterion, reason in reasons.items():
            print(f"    [{criterion}]")
            # Wrap reason text at 70 chars
            words = reason.split()
            line = "      "
            for word in words:
                if len(line) + len(word) > 76:
                    print(line)
                    line = "      " + word + " "
                else:
                    line += word + " "
            if line.strip():
                print(line)

    # Fund trails
    with get_db().cursor() as cur:
        cur.execute("""
            SELECT ft.risk_tier, ft.lag_days, fr.scheme_name,
                   fr.amount_cr, t.contract_value_cr, t.winner_name, t.award_date
            FROM fund_trails ft
            JOIN fund_releases fr ON fr.id = ft.fund_release_id
            JOIN tenders t ON t.id = ft.tender_id
            WHERE ft.politician_id = %s::uuid
            ORDER BY CASE ft.risk_tier
                WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END
            LIMIT 5
        """, (str(pol["id"]),))
        trails = cur.fetchall()

    if trails:
        print(f"\n  Fund Trails ({len(trails)} shown):")
        for t in trails:
            tier_color = RED if t["risk_tier"] == "CRITICAL" else YELLOW
            print(
                f"    {tier_color}[{t['risk_tier']:8s}]{RESET} "
                f"₹{float(t['amount_cr'] or 0):.1f}Cr {t['scheme_name'][:25]} "
                f"→ {t['winner_name'][:25]} "
                f"(lag: {t['lag_days']}d)"
            )


def cmd_scrape(args):
    """Run a specific scraper."""
    source = args.source.lower()

    if source == "ec":
        from scrapers.ec_scraper import ECAffidavitScraper
        scraper = ECAffidavitScraper()
        scraper.run(state=args.state or "all", year=args.year or 2024)

    elif source == "mca21":
        from scrapers.mca21_fetcher import MCA21Fetcher
        fetcher = MCA21Fetcher()
        fetcher.run(from_affidavits=True)

    elif source == "pfms":
        from scrapers.pfms_gem_rera_rti import PFMSWatcher
        PFMSWatcher().run(lookback_days=args.lookback_days or 90)

    elif source == "gem":
        from scrapers.pfms_gem_rera_rti import GeMAwardCrawler
        GeMAwardCrawler().run(lookback_days=args.lookback_days or 90)

    elif source == "rera":
        from scrapers.pfms_gem_rera_rti import RERAScraperScraper
        RERAScraperScraper().run(states=[args.state] if args.state else None)

    elif source == "rti":
        from scrapers.pfms_gem_rera_rti import RTIIndexer
        RTIIndexer().run()

    elif source == "pan":
        from scrapers.pan_resolver_pmla import PANResolver
        PANResolver().run()

    elif source == "pmla":
        from scrapers.pan_resolver_pmla import PMMAChecker
        PMMAChecker().run()

    elif source == "all":
        print("Running all scrapers in sequence...")
        cmd_scrape(argparse.Namespace(source="ec", state=args.state,
                                      year=args.year, lookback_days=args.lookback_days))
        cmd_scrape(argparse.Namespace(source="mca21", state=None,
                                      year=None, lookback_days=None))
        cmd_scrape(argparse.Namespace(source="pfms", state=None,
                                      year=None, lookback_days=args.lookback_days or 90))
        cmd_scrape(argparse.Namespace(source="gem", state=None,
                                      year=None, lookback_days=args.lookback_days or 90))
    else:
        print(f"{RED}Unknown source: {source}{RESET}")
        print("Valid sources: ec, mca21, pfms, gem, rera, rti, pan, pmla, all")
        sys.exit(1)


def cmd_build_graph(args):
    """Build or update the entity graph."""
    print("Building entity graph...")
    from engine.entity_graph import EntityGraphBuilder
    builder = EntityGraphBuilder()
    try:
        if args.politician_id:
            builder.update_for_politician(args.politician_id)
        else:
            builder.build_full_graph()
    finally:
        builder.close()
    print(f"{GREEN}✓ Entity graph build complete{RESET}")


def cmd_resolve(args):
    """Run identity resolution."""
    print("Running identity resolution...")
    from engine.identity_resolver import IdentityResolver
    resolver = IdentityResolver()
    try:
        resolver.resolve_all()
    finally:
        resolver.close()
    print(f"{GREEN}✓ Identity resolution complete{RESET}")


def cmd_score(args):
    """Run the scoring engine."""
    from engine.scorer import PoliticianScorer
    scorer = PoliticianScorer()
    try:
        if args.politician_id:
            score = scorer.score_politician(args.politician_id)
            scorer._save_score(score)
            print(f"\n{BOLD}{score.politician_name}{RESET}")
            print(f"Total score: {score.total_score}/100 ({score.risk_classification})")
        else:
            print(f"Scoring all politicians...")
            results = scorer.score_all()
            print(f"\n{GREEN}✓ Scored {len(results)} politicians{RESET}")

            # Print summary
            from collections import Counter
            classification_counts = Counter(r.risk_classification for r in results)
            for label in ["CRITICAL", "HIGH", "WATCH", "LOW"]:
                color = RISK_COLORS.get(label, "")
                count = classification_counts.get(label, 0)
                print(f"  {color}{label:10s}: {count}{RESET}")
    finally:
        scorer.close()


def cmd_trace(args):
    """Run fund flow tracing."""
    print(f"Tracing fund flows (lookback: {args.lookback_days} days)...")
    from engine.fund_tracer import FundTracer
    tracer = FundTracer()
    try:
        new_trails = tracer.run_full_trace(lookback_days=args.lookback_days)
        print(f"{GREEN}✓ {new_trails} new fund trails detected{RESET}")
    finally:
        tracer.close()


def cmd_pipeline(args):
    """Run the full processing pipeline: trace → resolve → score."""
    print(f"\n{BOLD}Running full DARPAN.IN pipeline...{RESET}\n")

    print("Step 1/3: Identity resolution...")
    cmd_resolve(argparse.Namespace())

    print("\nStep 2/3: Fund flow tracing...")
    cmd_trace(argparse.Namespace(lookback_days=args.lookback_days or 365))

    print("\nStep 3/3: Risk scoring...")
    cmd_score(argparse.Namespace(politician_id=None, all=True))

    print(f"\n{GREEN}{BOLD}✓ Full pipeline complete{RESET}")
    cmd_stats(args)


def cmd_check_pan_conflicts(args):
    """Report PAN conflicts in the dataset."""
    from engine.identity_resolver import IdentityResolver
    resolver = IdentityResolver()
    conflicts = resolver.find_pan_conflicts()
    resolver.close()

    if not conflicts:
        print(f"{GREEN}No PAN conflicts detected.{RESET}")
        return

    print(f"\n{YELLOW}PAN Conflicts Found: {len(conflicts)}{RESET}")
    for c in conflicts:
        print(f"  PAN {c['pan']}: {c['names']}")


def cmd_export(args):
    """Export scored politicians to JSON or CSV."""
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id::text, p.name_normalized AS name, p.pan, p.party,
                   p.state, p.constituency, p.election_year,
                   rs.total_score AS score, rs.risk_classification,
                   rs.score_asset_growth, rs.score_tender_linkage,
                   rs.score_fund_flow, rs.score_land_reg,
                   rs.score_rti_contradiction, rs.score_network_depth,
                   rs.raw_metrics, rs.scored_at::text
            FROM politicians p
            JOIN risk_scores rs ON rs.politician_id = p.id
            ORDER BY rs.total_score DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    output_file = args.output or f"vigilant_export_{date.today().isoformat()}.json"

    if args.format == "csv":
        import csv
        output_file = output_file.replace(".json", ".csv")
        with open(output_file, "w", newline="") as f:
            if rows:
                writer = csv.DictWriter(f, fieldnames=[k for k in rows[0] if k != "raw_metrics"])
                writer.writeheader()
                for row in rows:
                    row.pop("raw_metrics", None)
                    writer.writerow(row)
    else:
        with open(output_file, "w") as f:
            json.dump(rows, f, indent=2, default=str)

    print(f"{GREEN}✓ Exported {len(rows)} records to {output_file}{RESET}")


def cmd_reset(args):
    """Dangerous: Reset all computed data (not raw scraped data)."""
    if not args.confirm:
        print(f"{RED}This will delete all entity_links, fund_trails, and risk_scores.{RESET}")
        print("Add --confirm to proceed.")
        return

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE fund_trails, entity_links, risk_scores")
        conn.commit()
    conn.close()
    print(f"{YELLOW}⚠ Computed data reset. Run pipeline to recompute.{RESET}")


# ── Argument parser ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="DARPAN.IN Admin CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python vigilant_cli.py status
  python vigilant_cli.py stats
  python vigilant_cli.py report --top 20
  python vigilant_cli.py report --politician "Patil"
  python vigilant_cli.py scrape --source ec --state Maharashtra --year 2024
  python vigilant_cli.py scrape --source all --lookback-days 30
  python vigilant_cli.py build-graph
  python vigilant_cli.py resolve
  python vigilant_cli.py trace --lookback-days 365
  python vigilant_cli.py score --all
  python vigilant_cli.py pipeline
  python vigilant_cli.py export --format csv
        """
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # status
    sub.add_parser("status", help="Show system status")

    # stats
    sub.add_parser("stats", help="Show intelligence statistics")

    # report
    rp = sub.add_parser("report", help="Print risk reports")
    rp.add_argument("--top", type=int, default=20)
    rp.add_argument("--politician", help="Politician name to report on")

    # scrape
    sc = sub.add_parser("scrape", help="Run a scraper")
    sc.add_argument("--source", required=True,
                    choices=["ec", "mca21", "pfms", "gem", "rera", "rti", "pan", "pmla", "all"])
    sc.add_argument("--state", help="State name")
    sc.add_argument("--year", type=int, help="Election year")
    sc.add_argument("--lookback-days", type=int, default=90)

    # build-graph
    bg = sub.add_parser("build-graph", help="Build entity graph")
    bg.add_argument("--politician-id", help="Single politician UUID")

    # resolve
    sub.add_parser("resolve", help="Run identity resolution")

    # trace
    tr = sub.add_parser("trace", help="Run fund flow tracing")
    tr.add_argument("--lookback-days", type=int, default=365)

    # score
    sc2 = sub.add_parser("score", help="Run risk scoring")
    sc2.add_argument("--politician-id", help="Score one politician")
    sc2.add_argument("--all", action="store_true", default=True)

    # pipeline
    pl = sub.add_parser("pipeline", help="Run full processing pipeline")
    pl.add_argument("--lookback-days", type=int, default=365)

    # check-pan-conflicts
    sub.add_parser("check-pan-conflicts", help="Find PAN conflicts in dataset")

    # export
    ex = sub.add_parser("export", help="Export scored data")
    ex.add_argument("--format", choices=["json", "csv"], default="json")
    ex.add_argument("--output", help="Output file path")

    # reset
    rs = sub.add_parser("reset", help="Reset computed data (dangerous!)")
    rs.add_argument("--confirm", action="store_true")

    args = parser.parse_args()

    commands = {
        "status": cmd_status,
        "stats": cmd_stats,
        "report": cmd_report,
        "scrape": cmd_scrape,
        "build-graph": cmd_build_graph,
        "resolve": cmd_resolve,
        "trace": cmd_trace,
        "score": cmd_score,
        "pipeline": cmd_pipeline,
        "check-pan-conflicts": cmd_check_pan_conflicts,
        "export": cmd_export,
        "reset": cmd_reset,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
