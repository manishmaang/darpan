"""
scrapers/mca21_fetcher.py
=========================
Fetches company and director data from MCA21 (Ministry of Corporate Affairs).
URL: https://www.mca.gov.in

What it does:
1. For every politician PAN in our database, looks up all companies where
   that PAN appears as Director or Shareholder
2. For each company, fetches all directors and shareholders
3. Recursively fetches subsidiary companies (up to depth 4)
4. Saves everything to PostgreSQL (companies, company_persons tables)
5. Fires events to trigger entity_graph.py rebuild

Two data sources on MCA21:
    a) MCA21 API v3 (official, requires registration)
    b) HTML scraping fallback (public search pages)
"""

import re
import json
import time
import argparse
import logging
from typing import Generator, Optional
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from base_scraper import BaseScraper

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MCA_BASE = "https://www.mca.gov.in"
MCA_API_BASE = "https://api.mca.gov.in/rest/2.0"    # MCA21 v3 API

# MCA21 public search endpoints
MCA_COMPANY_SEARCH = f"{MCA_BASE}/mcafoportal/viewCompanyMasterData.do"
MCA_DIN_SEARCH = f"{MCA_BASE}/mcafoportal/viewDINStatus.do"
MCA_COMPANY_MASTER = f"{MCA_BASE}/mcafoportal/getCompanyMasterData.do"
MCA_DIRECTOR_SEARCH = f"{MCA_BASE}/mcafoportal/viewSignatoryDetails.do"

# CIN format: L/U + 5 digits + 2 letters + 4 digits + 3 letters + 6 digits
CIN_PATTERN = re.compile(r"\b([LU][0-9]{5}[A-Z]{2}[0-9]{4}[A-Z]{3}[0-9]{6})\b")
DIN_PATTERN = re.compile(r"\b([0-9]{8})\b")


class MCA21Fetcher(BaseScraper):
    """
    Fetches company and directorship data from MCA21.

    The core intelligence of VIGILANT.IN depends on this data:
    - We know politician PAN from EC affidavits
    - MCA21 tells us which companies they direct/own
    - Cross-matching family member names finds indirect links

    Strategy:
    1. Direct PAN lookup: find companies where politician is director
    2. Family cross-match: find companies where family members are directors
    3. Subsidiary traversal: go 4 hops deep into corporate hierarchy
    4. Geographical filter: prioritize companies in politician's state
    """

    SCRAPER_NAME = "mca21_fetcher"
    BASE_URL = MCA_BASE
    REQUEST_DELAY_SEC = 2.5
    REQUEST_DELAY_JITTER = 1.5
    MAX_SUBSIDIARY_DEPTH = 4   # How many corporate hops to follow

    def scrape(self, from_affidavits: bool = True,
               pan_list: list = None) -> Generator[dict, None, None]:
        """
        Fetch companies for all politician PANs from database, or a given list.

        Args:
            from_affidavits: If True, load PANs from politicians table
            pan_list: Optional explicit list of PANs to process
        """
        if from_affidavits:
            pan_list = self._load_politician_pans()
            family_pans = self._load_family_pans()
            all_pans = list(set(pan_list + family_pans))
        else:
            all_pans = pan_list or []

        self.logger.info(f"Fetching MCA21 data for {len(all_pans)} PAN numbers...")

        for pan in all_pans:
            try:
                for record in self._fetch_companies_for_pan(pan):
                    yield record
            except Exception as e:
                self.logger.error(f"Failed to fetch companies for PAN {pan}: {e}")

    def _load_politician_pans(self) -> list[str]:
        """Load all politician PANs from PostgreSQL."""
        with self.db_cursor() as cur:
            cur.execute("SELECT DISTINCT pan FROM politicians WHERE pan IS NOT NULL")
            return [row["pan"] for row in cur.fetchall()]

    def _load_family_pans(self) -> list[str]:
        """Load all family member PANs from PostgreSQL."""
        with self.db_cursor() as cur:
            cur.execute("SELECT DISTINCT pan FROM politician_family WHERE pan IS NOT NULL")
            return [row["pan"] for row in cur.fetchall()]

    def _fetch_companies_for_pan(self, pan: str) -> Generator[dict, None, None]:
        """Fetch all companies associated with a given PAN."""
        self.logger.debug(f"Looking up companies for PAN: {pan}")

        # Try API first, fall back to HTML scraping
        try:
            companies = self._api_lookup_pan(pan)
        except Exception:
            companies = self._html_lookup_pan(pan)

        for company_stub in companies:
            try:
                # Get full company details
                company = self._fetch_company_details(company_stub["cin"])
                if company:
                    company["source_pan"] = pan
                    yield company

                    # Recursively fetch subsidiaries
                    for sub in self._fetch_subsidiaries(company_stub["cin"], depth=1):
                        yield sub

            except Exception as e:
                self.logger.error(f"  Failed to fetch company {company_stub.get('cin')}: {e}")

    def _api_lookup_pan(self, pan: str) -> list[dict]:
        """
        Use MCA21 API v3 to find companies by PAN.
        Requires API key in environment: MCA21_API_KEY
        """
        import os
        api_key = os.getenv("MCA21_API_KEY")
        if not api_key:
            raise ValueError("MCA21_API_KEY not set")

        response = self.get(
            f"{MCA_API_BASE}/directordetails",
            params={"pan": pan},
            headers={"Authorization": f"Bearer {api_key}"}
        )
        data = response.json()
        return [
            {"cin": item["cin"], "name": item.get("company_name", "")}
            for item in data.get("directorships", [])
        ]

    def _html_lookup_pan(self, pan: str) -> list[dict]:
        """
        HTML scraping fallback for PAN lookup.
        Uses MCA21 public signatory search.
        """
        try:
            response = self.post(
                MCA_DIRECTOR_SEARCH,
                data={"signType": "pan", "signValue": pan}
            )
            soup = BeautifulSoup(response.text, "lxml")
            companies = []

            # MCA21 returns a table of company affiliations
            for row in soup.select("table#companyList tbody tr"):
                cells = row.find_all("td")
                if len(cells) >= 3:
                    cin_match = CIN_PATTERN.search(cells[0].get_text())
                    if cin_match:
                        companies.append({
                            "cin": cin_match.group(1),
                            "name": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                            "role": cells[2].get_text(strip=True) if len(cells) > 2 else "director",
                        })
            return companies
        except Exception as e:
            self.logger.warning(f"HTML lookup failed for PAN {pan}: {e}")
            return []

    def _fetch_company_details(self, cin: str) -> Optional[dict]:
        """Fetch full company master data for a CIN."""
        if self.is_already_scraped(f"mca_company_{cin}", max_age_days=7):
            return self._load_company_from_db(cin)

        try:
            response = self.post(MCA_COMPANY_MASTER, data={"companyID": cin})
            soup = BeautifulSoup(response.text, "lxml")

            # Parse company master data table
            data = {}
            for row in soup.select("table.companyData tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).lower().replace(" ", "_")
                    data[key] = cells[1].get_text(strip=True)

            company = {
                "cin": cin,
                "name": data.get("company_name", ""),
                "name_normalized": self.normalize_name(data.get("company_name", "")),
                "company_type": data.get("company_type", ""),
                "status": data.get("company_status", ""),
                "registration_date": self._parse_date(data.get("date_of_incorporation")),
                "state_of_reg": data.get("state_of_incorporation", ""),
                "registered_address": data.get("registered_office_address", ""),
                "authorized_capital": self._parse_capital(data.get("authorised_capital")),
                "paid_up_capital": self._parse_capital(data.get("paid_up_capital")),
                "directors": self._fetch_directors(cin),
            }

            self.mark_scraped(f"mca_company_{cin}")
            return company

        except Exception as e:
            self.logger.error(f"Failed to fetch company details for {cin}: {e}")
            return None

    def _fetch_directors(self, cin: str) -> list[dict]:
        """Fetch all directors and shareholders for a company."""
        directors = []
        try:
            response = self.post(MCA_DIRECTOR_SEARCH, data={"companyID": cin})
            soup = BeautifulSoup(response.text, "lxml")

            for row in soup.select("table#directorList tbody tr"):
                cells = row.find_all("td")
                if len(cells) >= 4:
                    din_match = DIN_PATTERN.search(cells[0].get_text())
                    pan_match = self.extract_pan(cells[2].get_text() if len(cells) > 2 else "")
                    directors.append({
                        "din": din_match.group(1) if din_match else None,
                        "name_raw": cells[1].get_text(strip=True),
                        "name_normalized": self.normalize_name(cells[1].get_text(strip=True)),
                        "pan": pan_match,
                        "role": "director",
                        "appointed_date": self._parse_date(cells[3].get_text(strip=True) if len(cells) > 3 else None),
                        "ceased_date": self._parse_date(cells[4].get_text(strip=True) if len(cells) > 4 else None),
                        "is_active": not bool(cells[4].get_text(strip=True) if len(cells) > 4 else ""),
                    })
        except Exception as e:
            self.logger.warning(f"Failed to fetch directors for {cin}: {e}")

        return directors

    def _fetch_subsidiaries(self, cin: str, depth: int) -> Generator[dict, None, None]:
        """
        Recursively fetch subsidiary companies up to MAX_SUBSIDIARY_DEPTH.
        Subsidiaries are found from the company's annual returns on MCA21.
        """
        if depth >= self.MAX_SUBSIDIARY_DEPTH:
            return

        try:
            response = self.post(
                f"{MCA_BASE}/mcafoportal/getSubsidiaryCompanies.do",
                data={"companyID": cin}
            )
            soup = BeautifulSoup(response.text, "lxml")

            for row in soup.select("table#subsidiaryList tbody tr"):
                cin_cell = row.find("td")
                if cin_cell:
                    sub_cin_match = CIN_PATTERN.search(cin_cell.get_text())
                    if sub_cin_match:
                        sub_cin = sub_cin_match.group(1)
                        sub_company = self._fetch_company_details(sub_cin)
                        if sub_company:
                            sub_company["parent_cin"] = cin
                            sub_company["subsidiary_depth"] = depth
                            yield sub_company
                            # Recurse
                            yield from self._fetch_subsidiaries(sub_cin, depth + 1)

        except Exception as e:
            self.logger.debug(f"No subsidiaries found for {cin} at depth {depth}: {e}")

    def _load_company_from_db(self, cin: str) -> Optional[dict]:
        """Load already-scraped company from DB."""
        with self.db_cursor() as cur:
            cur.execute("SELECT * FROM companies WHERE cin = %s", (cin,))
            row = cur.fetchone()
            if row:
                return dict(row)
        return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse various Indian date formats."""
        if not date_str:
            return None
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %b %Y", "%B %d, %Y"]:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except (ValueError, AttributeError):
                continue
        return None

    def _parse_capital(self, capital_str: Optional[str]) -> Optional[float]:
        """Parse authorized/paid-up capital string to crores."""
        if not capital_str:
            return None
        # MCA21 shows capital in rupees
        num_match = re.search(r"[\d,]+", capital_str.replace(",", ""))
        if num_match:
            rupees = float(num_match.group().replace(",", ""))
            return rupees / 10_000_000  # Convert to crores
        return None

    # ── Database persistence ──────────────────────────────────────────────────

    def save_record(self, record: dict) -> bool:
        """Save company + directors to PostgreSQL."""
        with self.db_connection() as conn:
            with conn.cursor() as cur:
                # Upsert company
                cur.execute("""
                    INSERT INTO companies (
                        cin, name, name_normalized, company_type, status,
                        registration_date, state_of_reg, registered_address,
                        authorized_capital, paid_up_capital
                    ) VALUES (
                        %(cin)s, %(name)s, %(name_normalized)s, %(company_type)s, %(status)s,
                        %(registration_date)s, %(state_of_reg)s, %(registered_address)s,
                        %(authorized_capital)s, %(paid_up_capital)s
                    )
                    ON CONFLICT (cin) DO UPDATE SET
                        name_normalized = EXCLUDED.name_normalized,
                        status = EXCLUDED.status,
                        updated_at = NOW()
                    RETURNING id
                """, {k: record.get(k) for k in [
                    "cin", "name", "name_normalized", "company_type", "status",
                    "registration_date", "state_of_reg", "registered_address",
                    "authorized_capital", "paid_up_capital"
                ]})
                company_id = cur.fetchone()["id"]

                # Insert directors
                for director in record.get("directors", []):
                    cur.execute("""
                        INSERT INTO company_persons (
                            company_id, din, pan, name_raw, name_normalized,
                            role, appointed_date, ceased_date, is_active
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        str(company_id),
                        director.get("din"), director.get("pan"),
                        director["name_raw"], director["name_normalized"],
                        director.get("role", "director"),
                        director.get("appointed_date"), director.get("ceased_date"),
                        director.get("is_active", True)
                    ))

        self.logger.info(f"  ✓ Saved company: {record['name_normalized']} ({record['cin']})")
        return True


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MCA21 Company Fetcher")
    parser.add_argument("--from-affidavits", action="store_true", default=True,
                        help="Load PANs from EC affidavits in DB")
    parser.add_argument("--pan", help="Single PAN to look up")
    parser.add_argument("--cin", help="Single CIN to fetch")
    args = parser.parse_args()

    fetcher = MCA21Fetcher()
    if args.pan:
        fetcher.run(from_affidavits=False, pan_list=[args.pan])
    else:
        fetcher.run(from_affidavits=True)
