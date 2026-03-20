"""
scrapers/pfms_watcher.py
========================
Monitors PFMS (Public Financial Management System) for district-level
government fund disbursements. URL: https://pfms.nic.in

Tracks:
- NREGA, PM Awas Yojana, Smart City, PMGSY, and 20+ other major schemes
- District-wise, state-wise disbursements with dates and amounts
"""

import re
import json
import argparse
import logging
from typing import Generator, Optional
from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup

from base_scraper import BaseScraper

logger = logging.getLogger(__name__)

PFMS_BASE = "https://pfms.nic.in"
PFMS_SCHEME_REPORT = f"{PFMS_BASE}/new/site_content/report/fts/ftsreport.aspx"
PFMS_DISTRICT_REPORT = f"{PFMS_BASE}/new/site_content/report/DistrictReport.aspx"

# Major central schemes to monitor
TARGET_SCHEMES = [
    {"code": "MGNREGS", "name": "Mahatma Gandhi National Rural Employment Guarantee Scheme"},
    {"code": "PMAY-G", "name": "Pradhan Mantri Awaas Yojana - Gramin"},
    {"code": "PMAY-U", "name": "Pradhan Mantri Awaas Yojana - Urban"},
    {"code": "SCM", "name": "Smart City Mission"},
    {"code": "AMRUT", "name": "Atal Mission for Rejuvenation and Urban Transformation"},
    {"code": "PMGSY", "name": "Pradhan Mantri Gram Sadak Yojana"},
    {"code": "JJM", "name": "Jal Jeevan Mission"},
    {"code": "PMKSY", "name": "Pradhan Mantri Krishi Sinchayee Yojana"},
    {"code": "SBMG", "name": "Swachh Bharat Mission - Gramin"},
    {"code": "NHM", "name": "National Health Mission"},
    {"code": "SSA", "name": "Samagra Shiksha Abhiyan"},
    {"code": "PMFBY", "name": "Pradhan Mantri Fasal Bima Yojana"},
]


class PFMSWatcher(BaseScraper):
    """
    Scrapes PFMS public dashboard for fund release data.

    PFMS is a GoI platform that tracks real-time fund flows
    from Central government → State → District → Implementing Agency.

    We capture the District level releases because that's where
    the correlation with tenders becomes geographically meaningful.
    """

    SCRAPER_NAME = "pfms_watcher"
    BASE_URL = PFMS_BASE
    REQUEST_DELAY_SEC = 3.0

    def scrape(self, financial_year: str = "2023-24",
               states: list = None, lookback_days: int = 90) -> Generator[dict, None, None]:
        """
        Scrape PFMS for district-level disbursements.

        Args:
            financial_year: e.g. '2023-24'
            states: List of state names to filter (None = all states)
            lookback_days: Only fetch releases from last N days
        """
        cutoff_date = date.today() - timedelta(days=lookback_days)

        for scheme in TARGET_SCHEMES:
            self.logger.info(f"Fetching PFMS data: {scheme['name']}...")
            try:
                for record in self._fetch_scheme_releases(
                    scheme, financial_year, states, cutoff_date
                ):
                    yield record
            except Exception as e:
                self.logger.error(f"Failed to fetch {scheme['name']}: {e}")

    def _fetch_scheme_releases(self, scheme: dict, financial_year: str,
                                states: Optional[list], cutoff_date: date) -> Generator:
        """Fetch all district-level releases for one scheme."""
        try:
            # PFMS uses ASP.NET with ViewState — need to simulate form POSTs
            # First GET to capture ViewState tokens
            response = self.get(PFMS_DISTRICT_REPORT)
            soup = BeautifulSoup(response.text, "lxml")

            viewstate = soup.find("input", {"id": "__VIEWSTATE"})
            eventval = soup.find("input", {"id": "__EVENTVALIDATION"})

            # POST to filter by scheme and year
            form_data = {
                "__VIEWSTATE": viewstate["value"] if viewstate else "",
                "__EVENTVALIDATION": eventval["value"] if eventval else "",
                "ctl00$ContentPlaceHolder1$ddlScheme": scheme["code"],
                "ctl00$ContentPlaceHolder1$ddlYear": financial_year,
                "ctl00$ContentPlaceHolder1$btnSearch": "Search",
            }

            response = self.post(PFMS_DISTRICT_REPORT, data=form_data)
            soup = BeautifulSoup(response.text, "lxml")

            # Parse results table
            for row in soup.select("table#gvDistrictWise tbody tr"):
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue
                try:
                    release_date = self._parse_date(cells[4].get_text(strip=True))
                    if release_date and release_date < cutoff_date:
                        continue  # Skip old records

                    amount_text = cells[3].get_text(strip=True)
                    amount_cr = self.parse_amount_cr(amount_text)

                    if not amount_cr or amount_cr < 0.1:
                        continue  # Skip tiny amounts

                    # Filter by state if requested
                    state_name = cells[0].get_text(strip=True)
                    if states and state_name not in states:
                        continue

                    yield {
                        "scheme_code": scheme["code"],
                        "scheme_name": scheme["name"],
                        "state": state_name,
                        "district": cells[1].get_text(strip=True),
                        "implementing_agency": cells[2].get_text(strip=True),
                        "amount_cr": amount_cr,
                        "release_date": release_date,
                        "financial_year": financial_year,
                    }
                except (IndexError, ValueError) as e:
                    continue

        except Exception as e:
            self.logger.error(f"PFMS scrape failed for {scheme['name']}: {e}")

    def _parse_date(self, date_str: str) -> Optional[date]:
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except (ValueError, AttributeError):
                continue
        return None

    def save_record(self, record: dict) -> bool:
        with self.db_cursor() as cur:
            cur.execute("""
                INSERT INTO fund_releases (
                    scheme_code, scheme_name, state, district, implementing_agency,
                    amount_cr, release_date, financial_year
                ) VALUES (%(scheme_code)s, %(scheme_name)s, %(state)s, %(district)s,
                          %(implementing_agency)s, %(amount_cr)s, %(release_date)s,
                          %(financial_year)s)
                ON CONFLICT (pfms_ref_id) DO NOTHING
            """, record)
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PFMS Fund Watcher")
    parser.add_argument("--year", default="2023-24")
    parser.add_argument("--lookback-days", type=int, default=90)
    args = parser.parse_args()
    PFMSWatcher().run(financial_year=args.year, lookback_days=args.lookback_days)


# =============================================================================
# scrapers/gem_crawler.py — GeM Tender Portal Crawler
# =============================================================================
"""
Crawls Government e-Marketplace for awarded tender data.
URL: https://gem.gov.in

Extracts: tender ID, department, district, value, winner CIN/name/GST, award date.
"""

GEM_BASE = "https://gem.gov.in"
GEM_ORDERS_API = f"{GEM_BASE}/api/public/orders/awarded"


class GeMAwardCrawler(BaseScraper):
    SCRAPER_NAME = "gem_crawler"
    BASE_URL = GEM_BASE
    REQUEST_DELAY_SEC = 2.0

    def scrape(self, lookback_days: int = 90,
               states: list = None) -> Generator[dict, None, None]:
        """Scrape awarded orders from GeM portal."""
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        page = 1
        per_page = 100

        while True:
            try:
                response = self.get(GEM_ORDERS_API, params={
                    "fromDate": cutoff,
                    "pageNo": page,
                    "pageSize": per_page,
                    "orderBy": "awardDate",
                    "sortOrder": "DESC",
                })
                data = response.json()
                orders = data.get("data", data.get("orders", []))

                if not orders:
                    break

                for order in orders:
                    record = self._parse_order(order)
                    if record:
                        if states and record.get("state") not in states:
                            continue
                        yield record

                # Pagination
                if len(orders) < per_page:
                    break
                page += 1

            except Exception as e:
                self.logger.error(f"GeM page {page} failed: {e}")
                # Fallback to HTML scraping
                yield from self._scrape_html_fallback(cutoff)
                break

    def _parse_order(self, order: dict) -> Optional[dict]:
        """Parse one GeM order JSON into our schema."""
        try:
            # GeM API field names vary — handle multiple formats
            cin = (order.get("sellerCIN") or order.get("cin") or
                   order.get("seller", {}).get("cin") or
                   self.extract_cin(order.get("sellerDetails", "")))

            return {
                "tender_ref_id": order.get("orderId") or order.get("order_id"),
                "source_portal": "gem",
                "department": order.get("buyerOrg") or order.get("department"),
                "category": order.get("productCategory") or order.get("category"),
                "state": order.get("deliveryState") or order.get("state"),
                "district": order.get("deliveryDistrict") or order.get("district"),
                "tender_description": order.get("productDescription"),
                "award_date": self._parse_iso_date(order.get("orderDate") or order.get("awardDate")),
                "contract_value_cr": self._crore_from_rupees(
                    order.get("orderValue") or order.get("totalValue", 0)
                ),
                "winner_name": order.get("sellerName") or order.get("seller", {}).get("name", ""),
                "winner_cin": cin,
                "winner_gst": order.get("sellerGST") or order.get("gstNumber"),
            }
        except Exception as e:
            self.logger.debug(f"Failed to parse order: {e}")
            return None

    def _scrape_html_fallback(self, cutoff_date: str) -> Generator[dict, None, None]:
        """HTML fallback for GeM tender scraping."""
        url = f"{GEM_BASE}/search/bid?orderBy=orderDate&sortOrder=DESC"
        try:
            response = self.get(url)
            soup = BeautifulSoup(response.text, "lxml")
            for row in soup.select("table.orders-table tbody tr"):
                cells = row.find_all("td")
                if len(cells) >= 6:
                    cin_text = cells[4].get_text() if len(cells) > 4 else ""
                    yield {
                        "tender_ref_id": cells[0].get_text(strip=True),
                        "source_portal": "gem",
                        "department": cells[1].get_text(strip=True),
                        "state": cells[2].get_text(strip=True),
                        "award_date": self._parse_iso_date(cells[5].get_text(strip=True)),
                        "contract_value_cr": self.parse_amount_cr(cells[3].get_text()),
                        "winner_name": cells[4].get_text(strip=True),
                        "winner_cin": self.extract_cin(cin_text),
                    }
        except Exception as e:
            self.logger.error(f"HTML fallback also failed: {e}")

    def _parse_iso_date(self, date_str) -> Optional[date]:
        if not date_str:
            return None
        for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
            try:
                return datetime.strptime(str(date_str)[:19], fmt).date()
            except ValueError:
                continue
        return None

    def _crore_from_rupees(self, rupees) -> Optional[float]:
        try:
            return float(rupees) / 10_000_000
        except (TypeError, ValueError):
            return None

    def save_record(self, record: dict) -> bool:
        if not record.get("tender_ref_id") or not record.get("award_date"):
            return False
        with self.db_cursor() as cur:
            cur.execute("""
                INSERT INTO tenders (
                    tender_ref_id, source_portal, department, category,
                    state, district, tender_description, award_date,
                    contract_value_cr, winner_name, winner_cin, winner_gst
                ) VALUES (%(tender_ref_id)s, %(source_portal)s, %(department)s, %(category)s,
                          %(state)s, %(district)s, %(tender_description)s, %(award_date)s,
                          %(contract_value_cr)s, %(winner_name)s, %(winner_cin)s, %(winner_gst)s)
                ON CONFLICT (tender_ref_id) DO UPDATE SET
                    completion_status = EXCLUDED.completion_status,
                    winner_cin = COALESCE(EXCLUDED.winner_cin, tenders.winner_cin)
            """, record)
        return True


# =============================================================================
# scrapers/rera_scraper.py — RERA Land Registry Scraper
# =============================================================================

RERA_PORTALS = {
    "Maharashtra": "https://maharera.mahaonline.gov.in",
    "Delhi": "https://rera.delhi.gov.in",
    "Karnataka": "https://rera.karnataka.gov.in",
    "Uttar Pradesh": "https://www.up-rera.in",
    "Tamil Nadu": "https://www.tnrera.in",
    "Gujarat": "https://gujrera.gujarat.gov.in",
    "Telangana": "https://rera.telangana.gov.in",
    "Rajasthan": "https://rera.rajasthan.gov.in",
    "West Bengal": "https://www.wbhidco.in",
    "Punjab": "https://www.rera.punjab.gov.in",
}


class RERAScraperScraper(BaseScraper):
    """
    Scrapes RERA portals across Indian states for property registrations.
    Each state has its own portal with different HTML structure.
    """

    SCRAPER_NAME = "rera_scraper"
    REQUEST_DELAY_SEC = 4.0   # State servers are slow

    def scrape(self, states: list = None,
               registered_after: date = None) -> Generator[dict, None, None]:
        target_states = states or list(RERA_PORTALS.keys())
        cutoff = registered_after or (date.today() - timedelta(days=365))

        # Load CINs and PANs we're tracking
        tracked_entities = self._load_tracked_entities()

        for state in target_states:
            portal_url = RERA_PORTALS.get(state)
            if not portal_url:
                continue
            self.logger.info(f"Scraping RERA: {state}...")
            try:
                for record in self._scrape_state_portal(
                    state, portal_url, tracked_entities, cutoff
                ):
                    yield record
            except Exception as e:
                self.logger.error(f"RERA scrape failed for {state}: {e}")

    def _load_tracked_entities(self) -> set:
        """Load all CINs and PANs we care about."""
        entities = set()
        with self.db_cursor() as cur:
            cur.execute("SELECT cin FROM companies")
            for row in cur.fetchall():
                entities.add(row["cin"])
            cur.execute("SELECT pan FROM politicians WHERE pan IS NOT NULL")
            for row in cur.fetchall():
                entities.add(row["pan"])
            cur.execute("SELECT pan FROM politician_family WHERE pan IS NOT NULL")
            for row in cur.fetchall():
                entities.add(row["pan"])
        return entities

    def _scrape_state_portal(self, state: str, url: str,
                              tracked: set, cutoff: date) -> Generator[dict, None, None]:
        """State-specific scraping. Each state has its own HTML structure."""
        response = self.get(f"{url}/search/project")
        soup = BeautifulSoup(response.text, "lxml")

        # Generic table parser — adjust selectors per state
        for row in soup.select("table.project-list tbody tr, table#projectTable tbody tr"):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            try:
                promoter_pan = self.extract_pan(cells[2].get_text())
                promoter_cin = self.extract_cin(cells[2].get_text())

                # Only track entities we know about
                if (promoter_pan not in tracked and
                        promoter_cin not in tracked):
                    continue

                reg_date = self._parse_date_str(cells[4].get_text(strip=True))
                if reg_date and reg_date < cutoff:
                    continue

                yield {
                    "rera_reg_no": cells[0].get_text(strip=True),
                    "project_name": cells[1].get_text(strip=True),
                    "promoter_name": cells[2].get_text(strip=True),
                    "promoter_cin": promoter_cin,
                    "promoter_pan": promoter_pan,
                    "state": state,
                    "district": cells[3].get_text(strip=True) if len(cells) > 3 else None,
                    "declared_value_cr": self.parse_amount_cr(
                        cells[5].get_text() if len(cells) > 5 else ""
                    ),
                    "registration_date": reg_date,
                }
            except Exception:
                continue

    def _parse_date_str(self, s: str) -> Optional[date]:
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]:
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except (ValueError, AttributeError):
                continue
        return None

    def save_record(self, record: dict) -> bool:
        if not record.get("rera_reg_no"):
            return False
        with self.db_cursor() as cur:
            cur.execute("""
                INSERT INTO rera_properties (
                    rera_reg_no, project_name, promoter_name, promoter_cin,
                    promoter_pan, state, district, declared_value_cr, registration_date
                ) VALUES (%(rera_reg_no)s, %(project_name)s, %(promoter_name)s, %(promoter_cin)s,
                          %(promoter_pan)s, %(state)s, %(district)s, %(declared_value_cr)s,
                          %(registration_date)s)
                ON CONFLICT (rera_reg_no) DO NOTHING
            """, record)
        return True


# =============================================================================
# scrapers/rti_indexer.py — RTI Response Indexer
# =============================================================================

RTI_BASE = "https://www.rtionline.gov.in"
RTI_RESPONSES = f"{RTI_BASE}/download.php"


class RTIIndexer(BaseScraper):
    """
    Indexes RTI (Right to Information) responses from RTIOnline.
    Uses NLP to extract contractor names, fund amounts, and detect contradictions.
    """

    SCRAPER_NAME = "rti_indexer"
    REQUEST_DELAY_SEC = 5.0   # RTI portal is especially sensitive

    # Keywords that suggest corruption-relevant RTI responses
    CORRUPTION_KEYWORDS = [
        "contractor", "tender", "award", "scheme", "disbursement",
        "fund release", "work order", "completion certificate",
        "utilization certificate", "bogus", "fictitious", "diversion",
    ]

    def scrape(self, keywords: list = None) -> Generator[dict, None, None]:
        search_terms = keywords or self.CORRUPTION_KEYWORDS[:5]

        for term in search_terms:
            try:
                for record in self._search_rti_responses(term):
                    yield record
            except Exception as e:
                self.logger.error(f"RTI search failed for '{term}': {e}")

    def _search_rti_responses(self, keyword: str) -> Generator[dict, None, None]:
        """Search RTIOnline for responses matching a keyword."""
        response = self.get(RTI_RESPONSES, params={
            "keyword": keyword,
            "doc_type": "reply",
            "from_date": (date.today() - timedelta(days=365)).isoformat(),
        })
        soup = BeautifulSoup(response.text, "lxml")

        for row in soup.select("table.rti-responses tbody tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            pdf_link = row.select_one("a[href*='.pdf']")
            try:
                doc_url = pdf_link["href"] if pdf_link else None
                if not doc_url:
                    continue

                # Download and extract text from RTI response PDF
                pdf_response = self.get(doc_url if doc_url.startswith("http") else RTI_BASE + doc_url)
                text = None
                if pdf_response.headers.get("content-type", "").startswith("application/pdf"):
                    import pdfplumber
                    import io
                    with pdfplumber.open(io.BytesIO(pdf_response.content)) as pdf:
                        text = "\n".join(p.extract_text() or "" for p in pdf.pages[:5])

                if not text:
                    continue

                # Extract structured data from text
                contractor = self._extract_contractor_name(text)
                amount = self.parse_amount_cr(text)
                contradiction = self._detect_contradiction(text)

                if contradiction:
                    yield {
                        "rti_application_no": cells[0].get_text(strip=True),
                        "response_date": None,
                        "public_authority": cells[1].get_text(strip=True),
                        "subject": cells[2].get_text(strip=True),
                        "contractor_name": contractor,
                        "fund_amount_cr": amount,
                        "contradiction_type": contradiction["type"],
                        "contradiction_detail": contradiction["detail"],
                        "source_url": doc_url,
                    }
            except Exception as e:
                self.logger.debug(f"RTI row parse failed: {e}")

    def _extract_contractor_name(self, text: str) -> Optional[str]:
        """Extract company/contractor name from RTI text using NLP patterns."""
        patterns = [
            r"(?:contractor|firm|company|vendor)\s*[:is]\s*([A-Z][A-Za-z\s&\.]+(?:Ltd|Pvt|LLP|Limited|Constructions|Infra|Builders))",
            r"([A-Z][A-Za-z\s]+(?:Pvt\.?\s*Ltd\.?|Limited|LLP|Constructions|Infrastructure|Builders|Works))",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()[:255]
        return None

    def _detect_contradiction(self, text: str) -> Optional[dict]:
        """
        Detect contradiction patterns in RTI response text.
        Returns dict with type and detail, or None if no contradiction found.
        """
        text_lower = text.lower()

        # Pattern: work awarded but not completed
        if any(k in text_lower for k in ["not completed", "incomplete work", "partial work"]):
            return {
                "type": "incomplete_work",
                "detail": "RTI reveals work awarded to contractor is incomplete despite full fund release."
            }

        # Pattern: contractor list hidden/withheld
        if any(k in text_lower for k in ["third party disclosure", "withheld", "exempted under section 8"]):
            if any(k in text_lower for k in ["contractor", "tender", "award"]):
                return {
                    "type": "hidden_contractor",
                    "detail": "RTI authority withheld contractor/tender information under exemption."
                }

        # Pattern: amount mismatch
        amounts = re.findall(r"(?:Rs\.?|₹)\s*([\d,]+(?:\.\d+)?)\s*(?:cr|lakh)", text_lower)
        if len(amounts) >= 2:
            vals = [float(a.replace(",", "")) for a in amounts]
            if max(vals) > 0 and abs(vals[0] - vals[1]) / max(vals) > 0.3:
                return {
                    "type": "amount_mismatch",
                    "detail": f"RTI shows discrepancy between sanctioned and disbursed amounts."
                }

        return None

    def save_record(self, record: dict) -> bool:
        with self.db_cursor() as cur:
            cur.execute("""
                INSERT INTO rti_flags (
                    rti_application_no, public_authority, subject,
                    contractor_name, fund_amount_cr, contradiction_type,
                    contradiction_detail, source_url
                ) VALUES (%(rti_application_no)s, %(public_authority)s, %(subject)s,
                          %(contractor_name)s, %(fund_amount_cr)s, %(contradiction_type)s,
                          %(contradiction_detail)s, %(source_url)s)
                ON CONFLICT DO NOTHING
            """, record)
        return True
