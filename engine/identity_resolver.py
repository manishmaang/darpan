"""
engine/identity_resolver.py
============================
Cross-dataset identity resolution engine.

The hardest problem in VIGILANT.IN:
    "Rajendra Patil" in EC affidavit
    "R. Patil" in MCA21 director list
    "Rajendra B. Patil" in GeM tender winner
    "RAJENDRA BHIMRAO PATIL" in RERA registration

Are these the same person? This module answers that.

Approach:
1. PAN as primary anchor (certain when present)
2. DIN for directors (near-certain)
3. Name + address blocking (Soundex/phonetic similarity)
4. Name + phone/Aadhaar last-4 (high confidence)
5. Learned deduplication model (dedupe library — trains on human-labeled examples)

Output: resolved entity clusters written back to PostgreSQL,
with confidence scores on every link.
"""

import os
import re
import json
import hashlib
import logging
import unicodedata
from typing import Optional
from collections import defaultdict
from difflib import SequenceMatcher

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Phonetic utilities ────────────────────────────────────────────────────────

# Indian name prefix/suffix noise words to strip before matching
NAME_NOISE = {
    "shri", "smt", "dr", "prof", "adv", "advocate", "late", "mr", "mrs",
    "ms", "ji", "saheb", "kumar", "kumari", "devi", "singh", "bai",
}

# Common Indian surname spelling variants
# Maps alternate spellings to canonical form
SPELLING_VARIANTS = {
    "patel": ["patel", "patil", "patidar", "patell"],
    "sharma": ["sharma", "sharme", "sharmaa"],
    "verma": ["verma", "varma", "verman"],
    "singh": ["singh", "sing", "shing"],
    "gupta": ["gupta", "guptha", "gupt"],
    "reddy": ["reddy", "redy", "reddi"],
    "nair": ["nair", "naiar", "nayar"],
    "iyer": ["iyer", "iyyer", "aiyar", "aiyyer"],
    "krishna": ["krishna", "krisna", "krishnan"],
    "mukherjee": ["mukherjee", "mukerjee", "mukherji", "mukhurji"],
}

VARIANT_MAP = {}
for canonical, variants in SPELLING_VARIANTS.items():
    for v in variants:
        VARIANT_MAP[v] = canonical


def normalize_name_for_matching(name: str) -> str:
    """
    Deep normalization of a person name for fuzzy matching.
    Strips titles, normalizes Unicode, canonicalizes spelling variants.
    """
    if not name:
        return ""

    # Unicode normalize (ñ → n, etc.)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()

    # Remove punctuation
    name = re.sub(r"[^\w\s]", " ", name)

    # Remove noise words
    tokens = [t for t in name.split() if t not in NAME_NOISE]

    # Apply spelling variant canonicalization
    tokens = [VARIANT_MAP.get(t, t) for t in tokens]

    # Sort tokens so "Patil Rajendra" == "Rajendra Patil"
    return " ".join(sorted(tokens))


def soundex_indian(name: str) -> str:
    """
    Modified Soundex for Indian names.
    Standard Soundex works poorly for Indian names because of:
    - Retroflex consonants (ṭ, ḍ, ṇ)
    - Aspirated consonants (kh, gh, ch, etc.)
    - Vowel-heavy names
    """
    if not name:
        return "0000"

    name = normalize_name_for_matching(name).upper()
    if not name:
        return "0000"

    # Map for Indian name sounds
    mapping = {
        "A": "0", "E": "0", "I": "0", "O": "0", "U": "0", "H": "0",
        "W": "0", "Y": "0",
        "B": "1", "F": "1", "P": "1", "V": "1",
        "C": "2", "G": "2", "J": "2", "K": "2", "Q": "2",
        "S": "2", "X": "2", "Z": "2",
        "D": "3", "T": "3",
        "L": "4",
        "M": "5", "N": "5",
        "R": "6",
    }

    first = name[0]
    encoded = first

    prev_code = mapping.get(first, "0")
    for char in name[1:]:
        code = mapping.get(char, "0")
        if code != "0" and code != prev_code:
            encoded += code
        prev_code = code

    encoded = encoded + "000"
    return encoded[:4]


# ── Confidence scoring ────────────────────────────────────────────────────────

def compute_name_similarity(name1: str, name2: str) -> float:
    """
    Compute similarity between two names using multiple strategies.
    Returns float 0.0–1.0.
    """
    n1 = normalize_name_for_matching(name1)
    n2 = normalize_name_for_matching(name2)

    if not n1 or not n2:
        return 0.0

    # Exact match after normalization
    if n1 == n2:
        return 1.0

    # Token-based overlap (handles word reordering)
    t1 = set(n1.split())
    t2 = set(n2.split())
    if t1 and t2:
        jaccard = len(t1 & t2) / len(t1 | t2)
        if jaccard >= 0.8:
            return 0.95

    # Character-level similarity
    seq_score = SequenceMatcher(None, n1, n2).ratio()

    # Soundex match bonus
    s1 = soundex_indian(name1)
    s2 = soundex_indian(name2)
    soundex_bonus = 0.1 if s1 == s2 else 0.0

    return min(1.0, seq_score + soundex_bonus)


# ── Main resolver class ───────────────────────────────────────────────────────

class IdentityResolver:
    """
    Resolves the same real-world entity across multiple datasets.

    The resolution hierarchy:
    1. PAN match → confidence 1.0
    2. DIN match → confidence 0.97
    3. Name + state + birth year → confidence 0.85–0.95
    4. Name similarity ≥ 0.90 → confidence 0.70–0.85
    5. Soundex match + same surname → confidence 0.55–0.70

    For each politician, we:
    a) Resolve politician ↔ politician_family member ↔ company_persons
    b) Write links to entity_links with confidence scores
    c) Flag ambiguous cases for human review (confidence 0.50–0.70)
    """

    def __init__(self):
        self.conn = psycopg2.connect(
            os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor
        )
        self._resolved_count = 0
        self._ambiguous_count = 0

    def resolve_all(self):
        """Run full identity resolution pass across all politicians."""
        politicians = self._load_politicians()
        logger.info(f"Resolving identities for {len(politicians)} politicians...")

        for pol in politicians:
            try:
                self._resolve_politician(pol)
            except Exception as e:
                logger.error(f"Resolution failed for {pol['name_normalized']}: {e}")

        logger.info(
            f"Identity resolution complete: {self._resolved_count} links, "
            f"{self._ambiguous_count} ambiguous"
        )

    def _resolve_politician(self, pol: dict):
        """Resolve all entities for one politician."""
        # Step 1: Direct PAN lookups in company_persons
        if pol.get("pan"):
            self._resolve_by_pan(pol)

        # Step 2: Resolve family members → company_persons
        family = self._load_family(pol["id"])
        for member in family:
            if member.get("pan"):
                self._resolve_family_by_pan(pol, member)
            else:
                # Fallback: fuzzy name match
                self._resolve_family_by_name(pol, member)

        # Step 3: Surname-based sweep for undeclared family in same state
        self._surname_sweep(pol)

    def _resolve_by_pan(self, pol: dict):
        """Find all company_persons entries matching the politician's PAN."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT cp.id, cp.company_id, cp.din, cp.name_normalized,
                       cp.role, cp.appointed_date, cp.is_active,
                       c.cin, c.name AS company_name, c.id AS company_uuid
                FROM company_persons cp
                JOIN companies c ON c.id = cp.company_id
                WHERE cp.pan = %s
            """, (pol["pan"],))
            matches = cur.fetchall()

        for match in matches:
            self._write_entity_link(
                politician_id=str(pol["id"]),
                company_id=str(match["company_uuid"]),
                link_type="direct",
                confidence=1.0,
                evidence=["pan_exact_match"],
                relation_via=None,
                graph_depth=1,
            )
            self._resolved_count += 1

    def _resolve_family_by_pan(self, pol: dict, member: dict):
        """Find company_persons entries for a family member's PAN."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT cp.company_id, c.id AS company_uuid, c.cin, c.name
                FROM company_persons cp
                JOIN companies c ON c.id = cp.company_id
                WHERE cp.pan = %s
            """, (member["pan"],))
            matches = cur.fetchall()

        for match in matches:
            self._write_entity_link(
                politician_id=str(pol["id"]),
                company_id=str(match["company_uuid"]),
                link_type="family",
                confidence=0.95,
                evidence=["family_pan_match"],
                relation_via=member.get("relation"),
                graph_depth=1,
            )
            self._resolved_count += 1

    def _resolve_family_by_name(self, pol: dict, member: dict):
        """Fuzzy name match for family members without PAN."""
        member_name = member.get("name_normalized", "")
        if not member_name or len(member_name) < 4:
            return

        # Only search companies registered in the same state
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT cp.id, cp.name_normalized, cp.company_id,
                       c.id AS company_uuid, c.name AS company_name, c.state_of_reg
                FROM company_persons cp
                JOIN companies c ON c.id = cp.company_id
                WHERE c.state_of_reg = %s
                  AND cp.is_active = TRUE
                  AND cp.name_normalized IS NOT NULL
                  AND LENGTH(cp.name_normalized) > 3
            """, (pol.get("state"),))
            candidates = cur.fetchall()

        for candidate in candidates:
            sim = compute_name_similarity(member_name, candidate["name_normalized"])

            if sim >= 0.90:
                confidence = 0.85 if sim >= 0.95 else 0.72
                self._write_entity_link(
                    politician_id=str(pol["id"]),
                    company_id=str(candidate["company_uuid"]),
                    link_type="family",
                    confidence=confidence,
                    evidence=[f"fuzzy_name_{int(sim*100)}pct"],
                    relation_via=member.get("relation"),
                    graph_depth=1,
                )
                if confidence < 0.80:
                    self._ambiguous_count += 1
                else:
                    self._resolved_count += 1

    def _surname_sweep(self, pol: dict):
        """
        Extract politician's surname and search for matching directors
        in companies in the same state. Flags undisclosed family links.

        Example: Politician "Rajendra Patil" → search for all "Patil" directors
        in Maharashtra companies that won government tenders.
        """
        name_tokens = (pol.get("name_normalized") or "").split()
        if len(name_tokens) < 2:
            return

        # Use the last word as surname (heuristic)
        surname = name_tokens[-1]
        if len(surname) < 4 or surname.lower() in NAME_NOISE:
            return

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT cp.name_normalized, cp.company_id,
                       c.id AS company_uuid, c.name AS company_name
                FROM company_persons cp
                JOIN companies c ON c.id = cp.company_id
                -- Only companies that won tenders (high relevance filter)
                JOIN tenders t ON t.winner_cin = c.cin
                WHERE c.state_of_reg = %s
                  AND cp.name_normalized ILIKE %s
                  AND cp.is_active = TRUE
                  AND cp.pan != %s  -- Exclude politician themselves
                LIMIT 50
            """, (pol.get("state"), f"%{surname}%", pol.get("pan") or ""))

            surname_matches = cur.fetchall()

        for match in surname_matches:
            sim = compute_name_similarity(surname, match["name_normalized"])
            if sim >= 0.70:
                # Low confidence — same surname in same state ≠ family
                # But it's worth investigating
                self._write_entity_link(
                    politician_id=str(pol["id"]),
                    company_id=str(match["company_uuid"]),
                    link_type="associate",
                    confidence=0.45,
                    evidence=[f"surname_sweep_{surname}"],
                    relation_via="possible_associate",
                    graph_depth=2,
                )
                self._ambiguous_count += 1

    def _write_entity_link(self, politician_id: str, company_id: str,
                            link_type: str, confidence: float,
                            evidence: list, relation_via: Optional[str],
                            graph_depth: int):
        """Upsert entity_links record. Higher confidence always wins."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO entity_links (
                        politician_id, company_id, link_type, confidence,
                        evidence_sources, relation_via, graph_depth
                    ) VALUES (
                        %s::uuid, %s::uuid, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (politician_id, company_id) DO UPDATE SET
                        confidence = GREATEST(EXCLUDED.confidence, entity_links.confidence),
                        link_type = CASE
                            WHEN EXCLUDED.confidence > entity_links.confidence
                            THEN EXCLUDED.link_type
                            ELSE entity_links.link_type
                        END,
                        evidence_sources = array_cat(
                            entity_links.evidence_sources, EXCLUDED.evidence_sources
                        )
                """, (
                    politician_id, company_id, link_type, confidence,
                    evidence, relation_via, graph_depth
                ))
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to write entity link: {e}")

    # ── PAN disambiguation ────────────────────────────────────────────────────

    def find_pan_conflicts(self) -> list[dict]:
        """
        Find cases where the same PAN appears linked to multiple different
        real-world entities (data entry errors, fake PANs, etc.)
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT pan, COUNT(DISTINCT name_normalized) AS name_count,
                       array_agg(DISTINCT name_normalized) AS names
                FROM (
                    SELECT pan, name_normalized FROM politicians WHERE pan IS NOT NULL
                    UNION ALL
                    SELECT pan, name_normalized FROM politician_family WHERE pan IS NOT NULL
                    UNION ALL
                    SELECT pan, name_normalized FROM company_persons WHERE pan IS NOT NULL
                ) all_entities
                WHERE pan IS NOT NULL
                GROUP BY pan
                HAVING COUNT(DISTINCT name_normalized) > 1
                ORDER BY name_count DESC
            """)
            return [dict(r) for r in cur.fetchall()]

    def find_duplicate_politicians(self) -> list[dict]:
        """
        Find politician records that might be duplicates (same person filed
        affidavits for multiple constituencies or parties).
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT p1.id AS id1, p2.id AS id2,
                       p1.name_normalized AS name1, p2.name_normalized AS name2,
                       p1.state AS state1, p2.state AS state2,
                       p1.pan AS pan1, p2.pan AS pan2
                FROM politicians p1
                JOIN politicians p2 ON (
                    p1.id < p2.id
                    AND (
                        (p1.pan IS NOT NULL AND p1.pan = p2.pan)
                        OR (p1.name_normalized = p2.name_normalized
                            AND p1.state = p2.state)
                    )
                )
                ORDER BY p1.name_normalized
            """)
            return [dict(r) for r in cur.fetchall()]

    # ── Data loaders ──────────────────────────────────────────────────────────

    def _load_politicians(self) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, name_normalized, pan, state
                FROM politicians ORDER BY state, name_normalized
            """)
            return [dict(r) for r in cur.fetchall()]

    def _load_family(self, politician_id: str) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, name_normalized, relation, pan
                FROM politician_family WHERE politician_id = %s
            """, (str(politician_id),))
            return [dict(r) for r in cur.fetchall()]

    def close(self):
        self.conn.close()


# ── Name comparison utilities (exposed for use in other modules) ──────────────

def names_likely_same_person(name1: str, name2: str,
                               pan1: str = None, pan2: str = None,
                               state1: str = None, state2: str = None) -> tuple[bool, float]:
    """
    High-level convenience function.
    Returns (is_same, confidence_score).
    Used by entity_graph.py for quick inline checks.
    """
    # PAN is definitive
    if pan1 and pan2:
        if pan1 == pan2:
            return True, 1.0
        else:
            return False, 0.0  # Different PANs = definitely different people

    # Name similarity
    sim = compute_name_similarity(name1, name2)

    # State context bonus
    if state1 and state2 and state1 == state2:
        sim = min(1.0, sim + 0.05)

    if sim >= 0.92:
        return True, sim
    elif sim >= 0.80:
        return True, sim * 0.9  # Penalize uncertainty
    else:
        return False, sim


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Identity Resolver")
    parser.add_argument("--check-conflicts", action="store_true",
                        help="Find PAN conflicts in dataset")
    parser.add_argument("--find-duplicates", action="store_true",
                        help="Find duplicate politician records")
    args = parser.parse_args()

    resolver = IdentityResolver()
    try:
        if args.check_conflicts:
            conflicts = resolver.find_pan_conflicts()
            print(f"\nPAN Conflicts Found: {len(conflicts)}")
            for c in conflicts[:20]:
                print(f"  PAN {c['pan']}: {c['names']}")
        elif args.find_duplicates:
            dupes = resolver.find_duplicate_politicians()
            print(f"\nPossible Duplicate Politicians: {len(dupes)}")
            for d in dupes[:20]:
                print(f"  {d['name1']} vs {d['name2']} ({d['state1']}/{d['state2']})")
        else:
            resolver.resolve_all()
    finally:
        resolver.close()
