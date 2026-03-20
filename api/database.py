"""
api/database.py
===============
Database connection helpers for the FastAPI application.
Provides connection pooling for PostgreSQL and Neo4j.
"""

import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from neo4j import GraphDatabase
import redis
from dotenv import load_dotenv

load_dotenv()

# ── PostgreSQL connection pool ─────────────────────────────────────────────────

_pg_pool: pool.ThreadedConnectionPool = None


def get_pg_pool() -> pool.ThreadedConnectionPool:
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=os.getenv("DATABASE_URL"),
            cursor_factory=RealDictCursor,
        )
    return _pg_pool


def get_db() -> Generator:
    """FastAPI dependency: yields a pooled PostgreSQL connection per request."""
    pg_pool = get_pg_pool()
    conn = pg_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pg_pool.putconn(conn)


@contextmanager
def db_cursor():
    """Context manager for direct use outside FastAPI dependency injection."""
    pg_pool = get_pg_pool()
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pg_pool.putconn(conn)


# ── Neo4j driver ───────────────────────────────────────────────────────────────

_neo4j_driver = None


def get_neo4j() -> GraphDatabase:
    global _neo4j_driver
    if _neo4j_driver is None:
        _neo4j_driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
            max_connection_pool_size=50,
        )
    return _neo4j_driver


def neo4j_session():
    return get_neo4j().session()


# ── Redis client ───────────────────────────────────────────────────────────────

_redis_client = None


def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
    return _redis_client


def cache_get(key: str):
    try:
        return get_redis().get(f"vigilant:{key}")
    except Exception:
        return None


def cache_set(key: str, value: str, ttl_seconds: int = 300):
    try:
        get_redis().setex(f"vigilant:{key}", ttl_seconds, value)
    except Exception:
        pass


def cache_invalidate_pattern(pattern: str):
    try:
        r = get_redis()
        keys = r.keys(f"vigilant:{pattern}*")
        if keys:
            r.delete(*keys)
    except Exception:
        pass


# ── Startup / shutdown ─────────────────────────────────────────────────────────

def startup():
    """Call on app startup to warm up connections."""
    get_pg_pool()
    get_neo4j().verify_connectivity()
    get_redis().ping()


def shutdown():
    """Call on app shutdown to clean up connections."""
    global _pg_pool, _neo4j_driver, _redis_client
    if _pg_pool:
        _pg_pool.closeall()
    if _neo4j_driver:
        _neo4j_driver.close()
    if _redis_client:
        _redis_client.close()
