"""
scrapers/base_scraper.py
========================
Base class for all VIGILANT.IN scrapers.
Handles: retries, rate limiting, logging, DB connection, error recording.
"""

import os
import time
import hashlib
import logging
import json
import random
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional, Generator, Any
from contextlib import contextmanager

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class BaseScraper(ABC):
    """
    Abstract base class for all VIGILANT.IN scrapers.

    Each scraper must implement:
        - scrape(): main entry point, yields records
        - parse_record(): transforms raw HTML/JSON into a clean dict
        - save_record(): persists one record to PostgreSQL

    Built-in features:
        - Automatic retry with exponential backoff
        - Polite rate limiting (configurable delay between requests)
        - Request fingerprinting (skip already-scraped URLs)
        - Full audit logging to PostgreSQL
        - User-agent rotation to avoid blocks
    """

    # Subclasses override these
    SCRAPER_NAME: str = "base"
    BASE_URL: str = ""
    REQUEST_DELAY_SEC: float = 2.0      # Polite delay between requests
    REQUEST_DELAY_JITTER: float = 1.0   # Random extra delay (±jitter)
    MAX_RETRIES: int = 3
    TIMEOUT_SEC: int = 30

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    ]

    def __init__(self):
        self.logger = logging.getLogger(f"vigilant.{self.SCRAPER_NAME}")
        self._db_conn = None
        self._session = None
        self._scraped_count = 0
        self._error_count = 0

    # ── Database ──────────────────────────────────────────────────────────────

    @contextmanager
    def db_connection(self):
        """Context manager for PostgreSQL connection with auto-commit."""
        conn = psycopg2.connect(
            os.getenv("DATABASE_URL"),
            cursor_factory=RealDictCursor
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def db_cursor(self):
        """Context manager that yields a cursor within a managed connection."""
        with self.db_connection() as conn:
            with conn.cursor() as cur:
                yield cur

    def log_audit(self, action: str, entity_type: str = None,
                  entity_id: str = None, details: dict = None,
                  status: str = "success", error_msg: str = None):
        """Write an audit log entry."""
        try:
            with self.db_cursor() as cur:
                cur.execute("""
                    INSERT INTO audit_log (module, action, entity_type, entity_id,
                                          details, status, error_msg)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.SCRAPER_NAME, action, entity_type,
                    entity_id, json.dumps(details) if details else None,
                    status, error_msg
                ))
        except Exception as e:
            self.logger.warning(f"Audit log write failed: {e}")

    # ── HTTP Session ──────────────────────────────────────────────────────────

    @property
    def session(self) -> requests.Session:
        """Lazy-initialized HTTP session with retry logic."""
        if self._session is None:
            self._session = requests.Session()
            retry_strategy = Retry(
                total=self.MAX_RETRIES,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)
        return self._session

    def get(self, url: str, params: dict = None, headers: dict = None) -> requests.Response:
        """
        Polite GET request with:
        - Random user-agent rotation
        - Configurable rate limiting
        - Automatic retry on failure
        """
        # Rate limiting — be polite to government servers
        delay = self.REQUEST_DELAY_SEC + random.uniform(0, self.REQUEST_DELAY_JITTER)
        time.sleep(delay)

        _headers = {
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }
        if headers:
            _headers.update(headers)

        self.logger.debug(f"GET {url}")
        response = self.session.get(
            url, params=params, headers=_headers, timeout=self.TIMEOUT_SEC
        )
        response.raise_for_status()
        return response

    def post(self, url: str, data: dict = None, json_data: dict = None,
             headers: dict = None) -> requests.Response:
        """POST request with same polite behavior."""
        time.sleep(self.REQUEST_DELAY_SEC + random.uniform(0, self.REQUEST_DELAY_JITTER))
        _headers = {"User-Agent": random.choice(self.USER_AGENTS)}
        if headers:
            _headers.update(headers)
        response = self.session.post(
            url, data=data, json=json_data, headers=_headers, timeout=self.TIMEOUT_SEC
        )
        response.raise_for_status()
        return response

    # ── Fingerprinting ────────────────────────────────────────────────────────

    def url_fingerprint(self, url: str) -> str:
        """SHA256 fingerprint of a URL for deduplication."""
        return hashlib.sha256(url.encode()).hexdigest()

    def is_already_scraped(self, url: str, max_age_days: int = 7) -> bool:
        """
        Check if a URL was already scraped recently.
        Avoids re-scraping the same page within max_age_days.
        """
        fingerprint = self.url_fingerprint(url)
        try:
            with self.db_cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM audit_log
                    WHERE module = %s
                      AND action = 'scraped_url'
                      AND details->>'fingerprint' = %s
                      AND created_at > NOW() - INTERVAL '%s days'
                    LIMIT 1
                """, (self.SCRAPER_NAME, fingerprint, max_age_days))
                return cur.fetchone() is not None
        except Exception:
            return False  # If check fails, proceed with scraping

    def mark_scraped(self, url: str):
        """Record that a URL has been scraped."""
        self.log_audit(
            action="scraped_url",
            details={"url": url, "fingerprint": self.url_fingerprint(url)}
        )

    # ── Text Utilities ────────────────────────────────────────────────────────

    @staticmethod
    def normalize_name(name: str) -> str:
        """
        Normalize a person/company name for consistent matching.
        - Uppercase
        - Remove extra spaces
        - Remove common suffixes (PVT LTD, LIMITED, etc.)
        """
        if not name:
            return ""
        name = name.upper().strip()
        # Remove extra whitespace
        name = " ".join(name.split())
        # Remove common corporate suffixes for comparison
        for suffix in ["PRIVATE LIMITED", "PVT LTD", "PVT. LTD.", "LIMITED",
                       "LTD.", "LTD", "LLP", "& ASSOCIATES", "AND ASSOCIATES"]:
            if name.endswith(suffix):
                name = name[:-len(suffix)].strip()
        return name

    @staticmethod
    def parse_amount_cr(text: str) -> Optional[float]:
        """
        Parse Indian currency amounts from affidavit text.
        Handles: '1,23,456', 'Rs. 1.23 Cr', '₹45 lakhs', '2.5 crore'
        Returns amount in crores (float) or None.
        """
        import re
        if not text:
            return None
        text = text.replace(",", "").replace("₹", "").replace("Rs.", "").strip().lower()

        # Already in crores
        cr_match = re.search(r"([\d.]+)\s*cr(?:ore)?s?", text)
        if cr_match:
            return float(cr_match.group(1))

        # In lakhs → convert to crores
        lakh_match = re.search(r"([\d.]+)\s*lakh?s?", text)
        if lakh_match:
            return float(lakh_match.group(1)) / 100.0

        # Raw number (assume lakhs for small, crores for large)
        num_match = re.search(r"([\d.]+)", text)
        if num_match:
            val = float(num_match.group(1))
            return val / 100.0 if val > 1000 else val

        return None

    @staticmethod
    def extract_pan(text: str) -> Optional[str]:
        """Extract PAN card number from text. Format: AAAAA9999A"""
        import re
        pan_match = re.search(r"\b([A-Z]{5}[0-9]{4}[A-Z])\b", text)
        return pan_match.group(1) if pan_match else None

    @staticmethod
    def extract_cin(text: str) -> Optional[str]:
        """Extract Company Identification Number from text."""
        import re
        cin_match = re.search(r"\b([A-Z]{1}[0-9]{5}[A-Z]{2}[0-9]{4}[A-Z]{3}[0-9]{6})\b", text)
        return cin_match.group(1) if cin_match else None

    # ── Abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    def scrape(self, **kwargs) -> Generator[dict, None, None]:
        """
        Main scraping method. Yields one record dict per scraped item.
        Each subclass implements this for their specific portal.
        """
        pass

    @abstractmethod
    def save_record(self, record: dict) -> bool:
        """
        Persist one parsed record to PostgreSQL.
        Returns True on success, False on duplicate/error.
        """
        pass

    # ── Run method ────────────────────────────────────────────────────────────

    def run(self, **kwargs):
        """
        Main entry point. Calls scrape() and saves each record.
        Handles errors gracefully and logs summary.
        """
        self.logger.info(f"Starting {self.SCRAPER_NAME} scraper...")
        start_time = time.time()

        for record in self.scrape(**kwargs):
            try:
                saved = self.save_record(record)
                if saved:
                    self._scraped_count += 1
            except Exception as e:
                self._error_count += 1
                self.logger.error(f"Failed to save record: {e} | Record: {record}")
                self.log_audit(
                    action="save_failed",
                    details={"record_keys": list(record.keys())},
                    status="error",
                    error_msg=str(e)
                )

        elapsed = time.time() - start_time
        self.logger.info(
            f"✓ {self.SCRAPER_NAME} complete: "
            f"{self._scraped_count} saved, {self._error_count} errors, "
            f"{elapsed:.1f}s"
        )
        self.log_audit(
            action="scrape_complete",
            details={
                "scraped": self._scraped_count,
                "errors": self._error_count,
                "elapsed_sec": round(elapsed, 1)
            }
        )
