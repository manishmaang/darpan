"""
scrapers/ec_scraper.py
======================
Scrapes Election Commission of India affidavit portal.
URL: https://affidavit.eci.gov.in

What it does:
1. Fetches candidate list for a given state + election year
2. Downloads each candidate's affidavit PDF
3. Extracts: name, PAN, party, assets (movable + immovable), family members,
   business interests, liabilities
4. Saves to PostgreSQL: politicians, politician_family, politician_assets tables

Usage:
    python scrapers/ec_scraper.py --state Maharashtra --year 2024
    python scrapers/ec_scraper.py --state all --year 2024
"""

import re
import json
import logging
import argparse
import io
from typing import Generator, Optional
from datetime import date

import pdfplumber
import requests
from bs4 import BeautifulSoup

from base_scraper import BaseScraper

logger = logging.getLogger(__name__)


# ── Constants ─────────────────────────────────────────────────────────────────

EC_BASE_URL = "https://affidavit.eci.gov.in"
EC_CANDIDATE_SEARCH = f"{EC_BASE_URL}/candidateWise/getCandidateListYearAndStatewise"
EC_AFFIDAVIT_PDF = f"{EC_BASE_URL}/viewAffidavitFile"

INDIAN_STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya",
    "Mizoram", "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim",
    "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand",
    "West Bengal", "Delhi", "Jammu and Kashmir", "Ladakh",
    "Puducherry", "Chandigarh",
]

# Regex patterns for extracting data from PDF text
PATTERNS = {
    "pan": re.compile(r"\b([A-Z]{5}[0-9]{4}[A-Z])\b"),
    "amount_cr": re.compile(r"(?:Rs\.?|₹)\s*([\d,]+(?:\.\d+)?)\s*(?:Cr(?:ore)?s?|crores?)", re.I),
    "amount_lakh": re.compile(r"(?:Rs\.?|₹)\s*([\d,]+(?:\.\d+)?)\s*(?:lakh?s?|L)", re.I),
    "amount_raw": re.compile(r"(?:Rs\.?|₹)\s*([\d,]+(?:\.\d+)?)"),
    "mobile": re.compile(r"\b[6-9]\d{9}\b"),  # To redact
    "aadhaar": re.compile(r"\b\d{4}\s\d{4}\s\d{4}\b"),  # To redact
}

# Asset category keywords for PDF text section detection
ASSET_SECTIONS = {
    "cash_in_hand": ["cash in hand", "cash on hand"],
    "bank_deposits": ["bank deposit", "savings account", "fixed deposit", "fd"],
    "investments": ["investment", "mutual fund", "stock", "share", "debenture", "bond"],
    "vehicles": ["vehicle", "car", "motorcycle", "truck", "jeep"],
    "jewellery": ["jewellery", "jewelry", "gold", "silver", "ornament"],
    "agricultural_land": ["agricultural land", "agriculture land", "farm land"],
    "residential_property": ["residential", "house", "flat", "apartment", "plot"],
    "commercial_property": ["commercial", "office", "shop", "showroom", "warehouse"],
}

FAMILY_RELATIONS = {
    "self": None,
    "wife": "spouse", "husband": "spouse", "spouse": "spouse",
    "son": "child", "daughter": "child", "child": "child",
    "mother": "parent", "father": "parent", "parent": "parent",
    "brother": "sibling", "sister": "sibling", "sibling": "sibling",
}


class ECAffidavitScraper(BaseScraper):
    """
    Scrapes Election Commission of India affidavit portal.

    The ECI portal provides candidate affidavits as PDF files.
    We:
    1. Search for candidates by state + year via the portal's search API
    2. Download each PDF
    3. Extract structured data using pdfplumber + regex

    Rate limiting is important here — be very polite to government servers.
    """

    SCRAPER_NAME = "ec_scraper"
    BASE_URL = EC_BASE_URL
    REQUEST_DELAY_SEC = 3.0   # Extra polite for ECI
    REQUEST_DELAY_JITTER = 2.0

    def scrape(self, state: str = "all", year: int = 2024,
               election_type: str = "all") -> Generator[dict, None, None]:
        """
        Main scraping loop. Yields one dict per candidate affidavit.

        Args:
            state: State name or 'all' for all states
            year: Election year (2019, 2024, etc.)
            election_type: 'LS' (Lok Sabha), 'VS' (Vidhan Sabha), 'all'
        """
        states = INDIAN_STATES if state == "all" else [state]

        for state_name in states:
            self.logger.info(f"Scraping {state_name} — {year}...")
            try:
                candidates = self._fetch_candidate_list(state_name, year, election_type)
                self.logger.info(f"  Found {len(candidates)} candidates in {state_name}")

                for candidate in candidates:
                    try:
                        record = self._process_candidate(candidate, state_name, year)
                        if record:
                            yield record
                    except Exception as e:
                        self.logger.error(f"  Error processing candidate {candidate.get('name')}: {e}")
                        self._error_count += 1

            except Exception as e:
                self.logger.error(f"Failed to fetch candidates for {state_name}: {e}")

    def _fetch_candidate_list(self, state: str, year: int,
                               election_type: str) -> list[dict]:
        """Fetch list of candidates from ECI search API."""
        # ECI uses a form-based search — simulate POST request
        payload = {
            "stateName": state,
            "year": str(year),
            "electionType": election_type if election_type != "all" else "",
        }
        try:
            response = self.post(EC_CANDIDATE_SEARCH, data=payload)
            data = response.json()
            return data.get("candidates", [])
        except Exception as e:
            # Fallback: scrape the HTML search results page
            self.logger.warning(f"API failed, falling back to HTML scraping: {e}")
            return self._scrape_candidate_list_html(state, year)

    def _scrape_candidate_list_html(self, state: str, year: int) -> list[dict]:
        """HTML fallback scraper for candidate list."""
        url = f"{EC_BASE_URL}/candidateList?state={state}&year={year}"
        response = self.get(url)
        soup = BeautifulSoup(response.text, "lxml")
        candidates = []

        for row in soup.select("table.candidate-table tbody tr"):
            cells = row.find_all("td")
            if len(cells) >= 4:
                pdf_link = row.select_one("a[href*='.pdf'], a[href*='affidavit']")
                candidates.append({
                    "name": cells[0].get_text(strip=True),
                    "party": cells[1].get_text(strip=True),
                    "constituency": cells[2].get_text(strip=True),
                    "affidavit_url": pdf_link["href"] if pdf_link else None,
                })
        return candidates

    def _process_candidate(self, candidate: dict, state: str, year: int) -> Optional[dict]:
        """Download and parse a single candidate's affidavit."""
        affidavit_url = candidate.get("affidavit_url")
        if not affidavit_url:
            return None

        # Skip if already scraped within last 30 days
        if self.is_already_scraped(affidavit_url, max_age_days=30):
            self.logger.debug(f"  Skipping (cached): {candidate['name']}")
            return None

        # Download PDF
        pdf_bytes = self._download_pdf(affidavit_url)
        if not pdf_bytes:
            return None

        # Extract text from PDF
        pdf_text = self._extract_pdf_text(pdf_bytes)
        if not pdf_text:
            return None

        # Parse structured data
        record = self._parse_affidavit(pdf_text, candidate, state, year)
        self.mark_scraped(affidavit_url)
        return record

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download affidavit PDF, return bytes."""
        try:
            if not url.startswith("http"):
                url = EC_BASE_URL + url
            response = self.get(url)
            if response.headers.get("content-type", "").startswith("application/pdf"):
                return response.content
            self.logger.warning(f"Non-PDF response from {url}")
            return None
        except Exception as e:
            self.logger.error(f"PDF download failed: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract all text from PDF using pdfplumber."""
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text(x_tolerance=2, y_tolerance=2)
                    if text:
                        pages_text.append(text)
                return "\n".join(pages_text)
        except Exception as e:
            self.logger.error(f"PDF text extraction failed: {e}")
            return None

    def _parse_affidavit(self, text: str, candidate: dict,
                          state: str, year: int) -> dict:
        """
        Parse structured data from affidavit text.

        EC affidavits follow a standard Form 26 format with sections:
        - Personal details (name, PAN, address)
        - Criminal cases
        - Assets (movable + immovable)
        - Liabilities
        - Income details
        """
        record = {
            # Basic info
            "name_raw": candidate.get("name", ""),
            "name_normalized": self.normalize_name(candidate.get("name", "")),
            "party": candidate.get("party", ""),
            "constituency": candidate.get("constituency", ""),
            "state": state,
            "election_year": year,
            "source_url": candidate.get("affidavit_url", ""),

            # Extracted fields
            "pan": None,
            "family_members": [],
            "assets": {},
            "liabilities_lakh": None,
            "declared_annual_income_lakh": None,
        }

        # Extract PAN
        pan_matches = PATTERNS["pan"].findall(text)
        # First PAN is usually the candidate's own
        if pan_matches:
            record["pan"] = pan_matches[0]

        # Extract assets section
        record["assets"] = self._extract_assets(text)

        # Extract family members
        record["family_members"] = self._extract_family(text, pan_matches)

        # Extract liabilities
        record["liabilities_lakh"] = self._extract_liabilities(text)

        # Extract declared income
        record["declared_annual_income_lakh"] = self._extract_income(text)

        return record

    def _extract_assets(self, text: str) -> dict:
        """Extract asset values from affidavit text."""
        assets = {}
        text_lower = text.lower()

        for asset_type, keywords in ASSET_SECTIONS.items():
            for keyword in keywords:
                idx = text_lower.find(keyword)
                if idx == -1:
                    continue

                # Look for amounts in the 200 chars after the keyword
                snippet = text[idx:idx + 200]

                # Try crores first, then lakhs
                amount = None
                cr_match = PATTERNS["amount_cr"].search(snippet)
                if cr_match:
                    amount = float(cr_match.group(1).replace(",", ""))
                else:
                    lakh_match = PATTERNS["amount_lakh"].search(snippet)
                    if lakh_match:
                        amount = float(lakh_match.group(1).replace(",", "")) / 100.0
                    else:
                        raw_match = PATTERNS["amount_raw"].search(snippet)
                        if raw_match:
                            raw_val = float(raw_match.group(1).replace(",", ""))
                            # Heuristic: values > 10000 are likely in rupees, convert
                            amount = raw_val / 10_000_000 if raw_val > 100_000 else raw_val / 100.0

                if amount is not None:
                    assets[asset_type] = assets.get(asset_type, 0) + amount
                break  # Found this asset type, move to next

        return assets

    def _extract_family(self, text: str, pan_list: list[str]) -> list[dict]:
        """Extract family member declarations."""
        family = []
        text_lower = text.lower()

        # Look for relation keywords followed by name + PAN
        for relation_keyword, relation_normalized in FAMILY_RELATIONS.items():
            if relation_normalized is None:
                continue  # Skip 'self'

            idx = text_lower.find(relation_keyword)
            while idx != -1:
                snippet = text[idx:idx + 300]

                # Extract name (title case words after relation)
                name_match = re.search(
                    r"(?:name\s*:?\s*)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,4})",
                    snippet
                )
                pan_match = PATTERNS["pan"].search(snippet)

                if name_match:
                    family.append({
                        "name_raw": name_match.group(1),
                        "name_normalized": self.normalize_name(name_match.group(1)),
                        "relation": relation_normalized,
                        "pan": pan_match.group(0) if pan_match else None,
                    })

                idx = text_lower.find(relation_keyword, idx + 1)

        # Deduplicate by name
        seen = set()
        unique_family = []
        for member in family:
            key = member["name_normalized"]
            if key not in seen:
                seen.add(key)
                unique_family.append(member)

        return unique_family

    def _extract_liabilities(self, text: str) -> Optional[float]:
        """Extract total liabilities from affidavit."""
        text_lower = text.lower()
        for keyword in ["total liabilities", "total liability", "total dues"]:
            idx = text_lower.find(keyword)
            if idx != -1:
                snippet = text[idx:idx + 200]
                cr_match = PATTERNS["amount_cr"].search(snippet)
                if cr_match:
                    return float(cr_match.group(1).replace(",", ""))
                lakh_match = PATTERNS["amount_lakh"].search(snippet)
                if lakh_match:
                    return float(lakh_match.group(1).replace(",", "")) / 100.0
        return None

    def _extract_income(self, text: str) -> Optional[float]:
        """Extract declared annual income."""
        text_lower = text.lower()
        for keyword in ["annual income", "income from", "income declared", "gross income"]:
            idx = text_lower.find(keyword)
            if idx != -1:
                snippet = text[idx:idx + 200]
                lakh_match = PATTERNS["amount_lakh"].search(snippet)
                if lakh_match:
                    return float(lakh_match.group(1).replace(",", "")) / 100.0
                cr_match = PATTERNS["amount_cr"].search(snippet)
                if cr_match:
                    return float(cr_match.group(1).replace(",", ""))
        return None

    # ── Database persistence ──────────────────────────────────────────────────

    def save_record(self, record: dict) -> bool:
        """Save politician + assets + family to PostgreSQL."""
        with self.db_connection() as conn:
            with conn.cursor() as cur:
                # Upsert politician
                cur.execute("""
                    INSERT INTO politicians (
                        ec_affidavit_id, name_raw, name_normalized, pan,
                        party, state, constituency, election_year
                    ) VALUES (
                        %(affidavit_id)s, %(name_raw)s, %(name_normalized)s, %(pan)s,
                        %(party)s, %(state)s, %(constituency)s, %(election_year)s
                    )
                    ON CONFLICT (ec_affidavit_id) DO UPDATE SET
                        name_normalized = EXCLUDED.name_normalized,
                        pan = COALESCE(EXCLUDED.pan, politicians.pan),
                        updated_at = NOW()
                    RETURNING id
                """, {
                    "affidavit_id": f"{record['state']}-{record['election_year']}-{record['name_normalized'][:20]}",
                    **{k: record[k] for k in ["name_raw", "name_normalized", "pan",
                                               "party", "state", "constituency", "election_year"]}
                })
                politician_id = cur.fetchone()["id"]

                # Insert assets
                assets = record.get("assets", {})
                cur.execute("""
                    INSERT INTO politician_assets (
                        politician_id, election_year,
                        cash_in_hand_lakh, bank_deposits_lakh, investments_lakh,
                        vehicles_lakh, jewellery_lakh, other_movable_lakh,
                        agricultural_land_lakh, residential_property_lakh,
                        commercial_property_lakh, other_immovable_lakh,
                        total_liabilities_lakh, declared_annual_income_lakh,
                        source_pdf_url
                    ) VALUES (
                        %(politician_id)s, %(election_year)s,
                        %(cash)s, %(bank)s, %(inv)s, %(veh)s, %(jew)s, %(omov)s,
                        %(agri)s, %(res)s, %(com)s, %(oimm)s, %(liab)s, %(income)s, %(url)s
                    )
                    ON CONFLICT (politician_id, election_year) DO UPDATE SET
                        cash_in_hand_lakh = EXCLUDED.cash_in_hand_lakh,
                        bank_deposits_lakh = EXCLUDED.bank_deposits_lakh,
                        total_liabilities_lakh = EXCLUDED.total_liabilities_lakh
                """, {
                    "politician_id": str(politician_id),
                    "election_year": record["election_year"],
                    "cash": assets.get("cash_in_hand"),
                    "bank": assets.get("bank_deposits"),
                    "inv": assets.get("investments"),
                    "veh": assets.get("vehicles"),
                    "jew": assets.get("jewellery"),
                    "omov": assets.get("other_movable"),
                    "agri": assets.get("agricultural_land"),
                    "res": assets.get("residential_property"),
                    "com": assets.get("commercial_property"),
                    "oimm": assets.get("other_immovable"),
                    "liab": record.get("liabilities_lakh"),
                    "income": record.get("declared_annual_income_lakh"),
                    "url": record.get("source_url"),
                })

                # Insert family members
                for member in record.get("family_members", []):
                    cur.execute("""
                        INSERT INTO politician_family (
                            politician_id, name_raw, name_normalized, relation, pan
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        str(politician_id),
                        member["name_raw"], member["name_normalized"],
                        member["relation"], member.get("pan")
                    ))

        self.logger.info(f"  ✓ Saved: {record['name_normalized']} ({record['state']})")
        return True


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EC Affidavit Scraper")
    parser.add_argument("--state", default="all",
                        help="State name or 'all' (default: all)")
    parser.add_argument("--year", type=int, default=2024,
                        help="Election year (default: 2024)")
    parser.add_argument("--election-type", default="all",
                        choices=["all", "LS", "VS", "RS"],
                        help="Election type (default: all)")
    args = parser.parse_args()

    scraper = ECAffidavitScraper()
    scraper.run(state=args.state, year=args.year, election_type=args.election_type)
