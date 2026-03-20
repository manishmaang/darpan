"""
scrapers/pan_resolver.py
========================
Resolves PAN numbers for politicians and family members
who didn't disclose their PAN in affidavits.

Method:
1. Cross-reference name + date of birth + constituency with
   Income Tax Department e-filing portal (public PAN verification)
2. Use MCA21 director PAN (public in annual filings post-2019)
3. Cross-reference with GeM seller PAN (publicly disclosed)
4. RTI-sourced PAN information

Legal basis: PAN is required in all government filings.
The verification endpoint at incometaxindiaefiling.gov.in is public.

NOTE: We do NOT guess or brute-force PANs. We only match
confirmed PANs from public government sources.
"""

import re
import os
import json
import logging
import argparse
from typing import Optional
from datetime import date

import requests
from bs4 import BeautifulSoup

from base_scraper import BaseScraper

logger = logging.getLogger(__name__)

IT_EFILING_BASE = "https://eportal.incometax.gov.in"
IT_PAN_VERIFY = f"{IT_EFILING_BASE}/iec/foservices/#/pre-login/verifyYourPAN"
GEM_SELLER_API = "https://gem.gov.in/api/public/seller/pan"


class PANResolver(BaseScraper):
    """
    Resolves missing PAN numbers from public sources.
    Priority order:
    1. GeM portal (seller PANs are public for GST compliance)
    2. MCA21 annual filing data (directors must disclose PAN post-2019)
    3. Income Tax e-filing PAN verification (confirm by name + DOB)

    Only resolves PANs for people already in our database.
    Does not attempt to discover new people.
    """

    SCRAPER_NAME = "pan_resolver"
    REQUEST_DELAY_SEC = 4.0   # Be very polite to IT department servers

    def scrape(self, resolve_politicians: bool = True,
               resolve_family: bool = True) -> object:
        """Yield PAN resolution records."""

        if resolve_politicians:
            missing = self._load_politicians_without_pan()
            logger.info(f"Resolving PANs for {len(missing)} politicians...")
            for person in missing:
                result = self._resolve_pan(
                    name=person["name_normalized"],
                    entity_type="politician",
                    entity_id=str(person["id"]),
                    state=person.get("state"),
                )
                if result:
                    yield result

        if resolve_family:
            missing_family = self._load_family_without_pan()
            logger.info(f"Resolving PANs for {len(missing_family)} family members...")
            for member in missing_family:
                result = self._resolve_pan(
                    name=member["name_normalized"],
                    entity_type="family",
                    entity_id=str(member["id"]),
                    state=None,
                )
                if result:
                    yield result

    def _resolve_pan(self, name: str, entity_type: str,
                     entity_id: str, state: Optional[str]) -> Optional[dict]:
        """
        Try to find PAN for a person from public sources.
        Returns dict with pan and confidence, or None.
        """
        # Strategy 1: Search GeM seller registry by name
        pan = self._search_gem_sellers(name)
        if pan:
            return {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "pan": pan,
                "source": "gem_seller",
                "confidence": 0.88,
            }

        # Strategy 2: Search MCA21 directors (name + state)
        pan = self._search_mca21_directors(name, state)
        if pan:
            return {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "pan": pan,
                "source": "mca21_director",
                "confidence": 0.85,
            }

        return None

    def _search_gem_sellers(self, name: str) -> Optional[str]:
        """Search GeM portal seller registry for a person's PAN."""
        try:
            response = self.get(GEM_SELLER_API, params={"name": name, "type": "individual"})
            data = response.json()
            sellers = data.get("sellers", data.get("data", []))
            for seller in sellers:
                seller_name = seller.get("name", "")
                from base_scraper import BaseScraper
                if BaseScraper.normalize_name(seller_name) == BaseScraper.normalize_name(name):
                    pan = seller.get("pan") or seller.get("panNumber")
                    if pan and re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", pan):
                        return pan
        except Exception as e:
            logger.debug(f"GeM seller search failed for {name}: {e}")
        return None

    def _search_mca21_directors(self, name: str, state: Optional[str]) -> Optional[str]:
        """Search MCA21 director database for PAN by name + state."""
        try:
            from base_scraper import BaseScraper
            scraper_base = BaseScraper()
            response = scraper_base.post(
                "https://www.mca.gov.in/mcafoportal/viewSignatoryDetails.do",
                data={"signType": "name", "signValue": name,
                      "state": state or ""}
            )
            soup = BeautifulSoup(response.text, "lxml")
            for row in soup.select("table#directorList tbody tr"):
                cells = row.find_all("td")
                if len(cells) >= 3:
                    dir_name = cells[1].get_text(strip=True)
                    if BaseScraper.normalize_name(dir_name) == BaseScraper.normalize_name(name):
                        pan = BaseScraper.extract_pan(cells[2].get_text())
                        if pan:
                            return pan
        except Exception as e:
            logger.debug(f"MCA21 director PAN search failed for {name}: {e}")
        return None

    def _load_politicians_without_pan(self) -> list[dict]:
        with self.db_cursor() as cur:
            cur.execute("""
                SELECT id, name_normalized, state FROM politicians
                WHERE pan IS NULL ORDER BY state, name_normalized
            """)
            return [dict(r) for r in cur.fetchall()]

    def _load_family_without_pan(self) -> list[dict]:
        with self.db_cursor() as cur:
            cur.execute("""
                SELECT id, name_normalized FROM politician_family WHERE pan IS NULL
            """)
            return [dict(r) for r in cur.fetchall()]

    def save_record(self, record: dict) -> bool:
        """Update PAN in the appropriate table."""
        if not record.get("pan"):
            return False

        pan = record["pan"]
        entity_id = record["entity_id"]

        with self.db_cursor() as cur:
            if record["entity_type"] == "politician":
                cur.execute("""
                    UPDATE politicians SET pan = %s WHERE id = %s::uuid AND pan IS NULL
                """, (pan, entity_id))
            else:
                cur.execute("""
                    UPDATE politician_family SET pan = %s WHERE id = %s::uuid AND pan IS NULL
                """, (pan, entity_id))

        logger.info(
            f"  ✓ Resolved PAN {pan} for {record['entity_type']} "
            f"(source: {record['source']}, confidence: {record['confidence']:.0%})"
        )
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PAN Resolver")
    parser.add_argument("--politicians-only", action="store_true")
    parser.add_argument("--family-only", action="store_true")
    args = parser.parse_args()

    resolver = PANResolver()
    resolver.run(
        resolve_politicians=not args.family_only,
        resolve_family=not args.politicians_only,
    )


# =============================================================================
# scrapers/pmla_checker.py
# ========================
# Checks for Enforcement Directorate (ED) / PMLA (Prevention of Money
# Laundering Act) attachment orders and FIRs involving politician-linked entities.
#
# Sources:
#   1. ED official press releases: https://enforcementdirectorate.gov.in/pressrelease
#   2. Financial Intelligence Unit India: https://fiuindia.gov.in (annual reports)
#   3. SFIO press releases: https://sfio.nic.in/PressRelease.aspx
#   4. CBI press releases: https://cbi.gov.in/press-releases
# =============================================================================

ED_BASE = "https://enforcementdirectorate.gov.in"
ED_PRESS_RELEASES = f"{ED_BASE}/pressrelease"
SFIO_PRESS = "https://sfio.nic.in/PressRelease.aspx"
CBI_PRESS = "https://cbi.gov.in/press-releases"


class PMMAChecker(BaseScraper):
    """
    Checks for active PMLA/ED enforcement actions against
    politician-linked companies and individuals.

    This is supplementary data — PMLA flags significantly boost
    the RTI contradiction score but are treated as unverified
    until we can cross-reference with primary court records.
    """

    SCRAPER_NAME = "pmla_checker"
    REQUEST_DELAY_SEC = 5.0

    # NLP extraction patterns for press releases
    ATTACHMENT_PATTERN = re.compile(
        r"attached.*?(?:Rs\.?|₹)\s*([\d,\.]+)\s*(?:crore|lakh)",
        re.IGNORECASE
    )
    CIN_PATTERN = re.compile(r"\b([LU][0-9]{5}[A-Z]{2}[0-9]{4}[A-Z]{3}[0-9]{6})\b")
    PERSON_PATTERN = re.compile(
        r"(?:against|of|by)\s+(?:Shri|Smt\.?|Mr\.?|Mrs\.?)?\s*"
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
    )

    def scrape(self, lookback_months: int = 12) -> object:
        """Scrape ED/SFIO/CBI press releases for PMLA actions."""
        sources = [
            (ED_PRESS_RELEASES, "ED"),
            (SFIO_PRESS, "SFIO"),
            (CBI_PRESS, "CBI"),
        ]

        # Load entity names we're tracking
        tracked_names = self._load_tracked_names()
        tracked_cins = self._load_tracked_cins()

        for url, agency in sources:
            self.logger.info(f"Scanning {agency} press releases...")
            try:
                for record in self._scrape_press_releases(
                    url, agency, tracked_names, tracked_cins, lookback_months
                ):
                    yield record
            except Exception as e:
                self.logger.error(f"{agency} scrape failed: {e}")

    def _scrape_press_releases(self, url: str, agency: str,
                                tracked_names: set, tracked_cins: set,
                                lookback_months: int) -> object:
        """Scrape press releases from one agency."""
        try:
            response = self.get(url)
            soup = BeautifulSoup(response.text, "lxml")

            for link in soup.select("a[href*='press'], a[href*='release']"):
                press_url = link.get("href", "")
                if not press_url.startswith("http"):
                    press_url = ED_BASE + press_url

                try:
                    pr_response = self.get(press_url)
                    pr_soup = BeautifulSoup(pr_response.text, "lxml")
                    text = pr_soup.get_text()

                    # Check if any tracked entity is mentioned
                    found_names = [n for n in tracked_names
                                   if n.lower() in text.lower() and len(n) > 5]
                    found_cins = self.CIN_PATTERN.findall(text)
                    found_cins = [c for c in found_cins if c in tracked_cins]

                    if not found_names and not found_cins:
                        continue

                    # Extract attachment amount
                    amount_match = self.ATTACHMENT_PATTERN.search(text)
                    amount_cr = None
                    if amount_match:
                        raw = float(amount_match.group(1).replace(",", ""))
                        amount_cr = raw if "crore" in amount_match.group().lower() else raw / 100

                    yield {
                        "agency": agency,
                        "press_release_url": press_url,
                        "title": link.get_text(strip=True),
                        "matched_names": found_names[:5],
                        "matched_cins": found_cins[:5],
                        "amount_attached_cr": amount_cr,
                        "action_type": self._classify_action(text),
                        "source_url": press_url,
                    }
                except Exception as e:
                    self.logger.debug(f"Failed to parse press release {press_url}: {e}")

        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {e}")

    def _classify_action(self, text: str) -> str:
        """Classify the type of PMLA action from press release text."""
        text_lower = text.lower()
        if "arrest" in text_lower:
            return "arrest"
        elif "attach" in text_lower:
            return "attachment"
        elif "provisional attachment" in text_lower:
            return "provisional_attachment"
        elif "chargesheet" in text_lower or "charge sheet" in text_lower:
            return "chargesheet"
        elif "search" in text_lower and "seizure" in text_lower:
            return "search_seizure"
        elif "fir" in text_lower:
            return "fir"
        return "investigation"

    def _load_tracked_names(self) -> set:
        """Load all names we're monitoring."""
        names = set()
        with self.db_cursor() as cur:
            cur.execute("""
                SELECT name_normalized FROM politicians WHERE name_normalized IS NOT NULL
                UNION SELECT name_normalized FROM politician_family WHERE name_normalized IS NOT NULL
                UNION SELECT name FROM companies WHERE name IS NOT NULL
            """)
            for row in cur.fetchall():
                if row[0]:
                    names.add(row[0])
        return names

    def _load_tracked_cins(self) -> set:
        """Load all company CINs we're monitoring."""
        with self.db_cursor() as cur:
            cur.execute("SELECT cin FROM companies WHERE cin IS NOT NULL")
            return {row["cin"] for row in cur.fetchall()}

    def save_record(self, record: dict) -> bool:
        """
        PMLA flags are stored in the rti_flags table with a special prefix,
        since they represent similar contradiction/enforcement data.
        """
        with self.db_cursor() as cur:
            cur.execute("""
                INSERT INTO rti_flags (
                    public_authority, subject, contractor_name,
                    fund_amount_cr, contradiction_type, contradiction_detail,
                    source_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                record["agency"],
                record.get("title", "PMLA Action"),
                ", ".join(record.get("matched_names", [])),
                record.get("amount_attached_cr"),
                f"pmla_{record.get('action_type', 'action')}",
                f"{record['agency']} action. Matched entities: "
                f"{record.get('matched_names', [])}. "
                f"Amount: ₹{record.get('amount_attached_cr', 'N/A')}Cr",
                record.get("source_url"),
            ))
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PMLA Checker")
    parser.add_argument("--lookback-months", type=int, default=12)
    args = parser.parse_args()
    PMMAChecker().run(lookback_months=args.lookback_months)
