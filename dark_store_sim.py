"""
Dark Store Simulation — Streamlit Dashboard with SQLite Persistence
Run with: streamlit run dark_store_sim.py

Dependencies: streamlit, pyyaml, pandas, sqlalchemy
  pip install streamlit pyyaml pandas sqlalchemy
"""

import streamlit as st
import yaml
import pandas as pd
import os
import time
import random
import threading
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from collections import deque
import copy
import json
import logging
from datetime import datetime
from contextlib import contextmanager

# ── SQLAlchemy ────────────────────────────────────────────────────────────────
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Text,
    Boolean, BigInteger, event, text
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger("dark_store")

# ─────────────────────────────────────────────
# RANKING POLICY  (loaded from ranking_policy.json)
# ─────────────────────────────────────────────

def load_ranking_policy(path: str = "ranking_policy.json") -> dict:
    """Load heuristic weights from ranking_policy.json; fall back to defaults."""
    defaults = {
        "sku_process_time": 8.5,
        "row_change_penalty": 12.0,
        "restock_delay_multiplier": 2.8,
        "cooler_row_constant": 15.0,
        "backlog_threshold_seconds": 180,
        "cooler_rows": ["C", "D"],
    }
    try:
        with open(path, "r") as f:
            data = json.load(f)
        defaults.update({k: data[k] for k in defaults if k in data})
        log.info("Ranking policy loaded from %s", path)
    except FileNotFoundError:
        log.warning("ranking_policy.json not found — using built-in defaults.")
    return defaults


# Module-level policy (also stored in st.session_state for live editing)
_RANKING_POLICY: dict = load_ranking_policy()


# ─────────────────────────────────────────────
# ETC ENGINE  (Estimated Time to Complete)
# ─────────────────────────────────────────────

def calc_etc(skus: list, policy: dict, restocking_row_indices: set,
             cooler_row_index: int, sku_position: dict,
             skus_per_row: int) -> float:
    """
    Compute the Estimated Time to Complete an order using the policy weights.

    Formula:
        ETC = (N × sku_process_time)
            + max(0, unique_rows-1) × row_change_penalty
            + cooler_rows_touched × cooler_row_constant
        IF any row in the order is currently restocking:
            ETC ×= restock_delay_multiplier  (one-end access penalty)

    Parameters
    ----------
    skus                  : list of SKU name strings in the order
    policy                : loaded ranking_policy dict
    restocking_row_indices: set of integer row indices currently restocking
    cooler_row_index      : integer index of the cooler/special row
    sku_position          : {sku_name: (row_idx, col_idx)} lookup
    skus_per_row          : grid width (used for row-label mapping)
    """
    n = len(skus)
    if n == 0:
        return 0.0

    row_indices_in_order: set = set()
    for sku in skus:
        pos = sku_position.get(sku)
        if pos:
            row_indices_in_order.add(pos[0])

    base     = n * policy["sku_process_time"]
    row_pen  = max(0, len(row_indices_in_order) - 1) * policy["row_change_penalty"]
    cool_pen = (1 if cooler_row_index in row_indices_in_order else 0) * policy["cooler_row_constant"]

    etc = base + row_pen + cool_pen

    # One-end access penalty: any overlap with actively restocking rows
    if row_indices_in_order & restocking_row_indices:
        etc *= policy["restock_delay_multiplier"]

    return round(etc, 1)


def pending_pool_etc_total(orders: list) -> float:
    """Sum the pre-computed ETC values across all pending orders."""
    return sum(getattr(o, "etc_seconds", 0.0) for o in orders)

# ─────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────
# ── SQLAlchemy & DB Connection ──────────────────────────────────────────────

DB_PATH = "dark_store.db"
    
# 1. Detection: Use Supabase if secrets exist, else fall back to local SQLite
if "postgres" in st.secrets:
    p = st.secrets["postgres"]
    # Supabase Connection URI
    DB_URL = f"postgresql://{p.user}:{p.password}@{p.host}:{p.port}/{p.dbname}"
    
    # 2. Configure engine for PostgreSQL (Supabase)
    engine = create_engine(
        DB_URL,
        pool_size=10,        # Keeps 10 connections open
        max_overflow=20,     # Allows up to 20 extra if busy
        pool_recycle=300,    # Reconnect every 5 mins to prevent "idle" timeouts
        pool_pre_ping=True,  # Checks if connection is alive before using it
        connect_args={'sslmode': 'require'}  # MANDATORY for Supabase
    )
else:
    # Local: SQLite Fallback
    DB_URL = f"sqlite:///{DB_PATH}"
    engine = create_engine(
        DB_URL, 
        connect_args={"check_same_thread": False, "timeout": 30}
    )

    # SQLite Specific Events (only needed for local)
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_conn, _rec):
        if "sqlite" in DB_URL:
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute("PRAGMA busy_timeout=10000")
            cur.close()

Base = declarative_base()
SessionFactory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

@contextmanager
def get_db() -> Session:
    """Session context manager (Works for both SQLite and Postgres)."""
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        log.error("DB error (rolled back): %s", exc)
        raise
    finally:
        session.close()
# ── ORM Models ────────────────────────────────────────────────────────────────

class DBOrder(Base):
    __tablename__ = "orders"
    order_id         = Column(BigInteger, primary_key=True)
    skus             = Column(Text,    nullable=False)   # JSON list
    assigned_picker  = Column(Integer, nullable=True)
    status           = Column(String(20), nullable=False, default="Pending")
    pick_time        = Column(Float,   default=0.0)
    start_time       = Column(Float,   nullable=True)
    end_time         = Column(Float,   nullable=True)
    duration         = Column(Float,   nullable=True)
    paused_at        = Column(Float,   nullable=True)
    pause_reason     = Column(Text,    default="")
    delay_reason     = Column(Text,    default="")
    delay_count      = Column(Integer, default=0)
    created_at       = Column(Float,   nullable=False)   # epoch for ordering

class DBRestockEvent(Base):
    __tablename__ = "restock_events"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    row_index        = Column(Integer, nullable=False)
    picker_id        = Column(Integer, nullable=False)
    start_time       = Column(Float,   nullable=False)
    end_time         = Column(Float,   nullable=True)
    paused_order_id  = Column(BigInteger, nullable=True)

class DBInventorySnapshot(Base):
    __tablename__ = "inventory_snapshots"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    sku       = Column(String(100), nullable=False)
    row_index = Column(Integer,    nullable=False)
    col_index = Column(Integer,    nullable=False)
    units     = Column(Integer,    nullable=False)
    saved_at  = Column(Float,      nullable=False)

class DBMetrics(Base):
    __tablename__ = "metrics"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    total_processed  = Column(Integer, default=0)
    total_pick_time  = Column(Float,   default=0.0)
    total_restocks   = Column(Integer, default=0)
    saved_at         = Column(Float,   nullable=False)

Base.metadata.create_all(engine)


def _migrate_schema():
    """
    Run lightweight ALTER TABLE migrations so an existing dark_store.db
    from a previous version gets the new columns without losing data.
    Each statement is wrapped independently — if a column already exists
    SQLite raises OperationalError which we silently swallow.
    """
    migrations = [
        # v2: added delay tracking columns to orders
        "ALTER TABLE orders ADD COLUMN delay_reason TEXT DEFAULT ''",
        "ALTER TABLE orders ADD COLUMN delay_count  INTEGER DEFAULT 0",
    ]
    with engine.connect() as conn:
        for stmt in migrations:
            try:
                conn.execute(text(stmt))
                conn.commit()
                log.info("Migration applied: %s", stmt[:60])
            except Exception:
                # Column already exists → safe to ignore
                pass

_migrate_schema()


# ── DB helper functions ───────────────────────────────────────────────────────

def db_upsert_order(order: "Order"):
    """Write or update a single order row. Safe to call from any thread."""
    try:
        with get_db() as sess:
            row = sess.get(DBOrder, order.order_id)
            if row is None:
                row = DBOrder(order_id=order.order_id, created_at=time.time())
                sess.add(row)
            row.skus            = json.dumps(order.skus)
            row.assigned_picker = order.assigned_picker_id
            row.status          = order.status
            row.pick_time       = order.pick_time
            row.start_time      = order.start_time
            row.end_time        = order.end_time
            row.duration        = order.duration
            row.paused_at       = order.paused_at
            row.pause_reason    = order.pause_reason
            row.delay_reason    = order.delay_reason
            row.delay_count     = order.delay_count
    except SQLAlchemyError as exc:
        log.error("db_upsert_order failed for %s: %s", order.order_id, exc)

def db_insert_restock(evt: "RestockEvent") -> int:
    """Insert a new restock event; returns its DB id."""
    try:
        with get_db() as sess:
            row = DBRestockEvent(
                row_index       = evt.row_index,
                picker_id       = evt.picker_id,
                start_time      = evt.start_time,
                end_time        = evt.end_time,
                paused_order_id = evt.paused_order_id,
            )
            sess.add(row)
            sess.flush()
            return row.id
    except SQLAlchemyError as exc:
        log.error("db_insert_restock failed: %s", exc)
        return -1

def db_close_restock(db_id: int, end_time: float):
    """Mark a restock event as finished."""
    try:
        with get_db() as sess:
            row = sess.get(DBRestockEvent, db_id)
            if row:
                row.end_time = end_time
    except SQLAlchemyError as exc:
        log.error("db_close_restock failed: %s", exc)

def db_save_inventory(store: "DarkStore"):
    """Persist current inventory levels (overwrite previous snapshot)."""
    try:
        with get_db() as sess:
            sess.execute(text("DELETE FROM inventory_snapshots"))
            now = time.time()
            for row in store.rows:
                for c_idx, cell in enumerate(row.cells):
                    if cell.name == "(empty)":
                        continue
                    sess.add(DBInventorySnapshot(
                        sku       = cell.name,
                        row_index = row.index,
                        col_index = c_idx,
                        units     = cell.units,
                        saved_at  = now,
                    ))
    except SQLAlchemyError as exc:
        log.error("db_save_inventory failed: %s", exc)

def db_save_metrics(store: "DarkStore"):
    """Persist running totals (append row — easy to query latest)."""
    try:
        with get_db() as sess:
            sess.add(DBMetrics(
                total_processed = store.total_processed,
                total_pick_time = store.total_pick_time,
                total_restocks  = store.total_restocks,
                saved_at        = time.time(),
            ))
    except SQLAlchemyError as exc:
        log.error("db_save_metrics failed: %s", exc)

def db_load_orders() -> List[dict]:
    """Load all orders from DB ordered by creation time."""
    try:
        with get_db() as sess:
            rows = sess.query(DBOrder).order_by(DBOrder.created_at).all()
            return [
                {
                    "order_id":         r.order_id,
                    "skus":             json.loads(r.skus),
                    "assigned_picker":  r.assigned_picker,
                    "status":           r.status,
                    "pick_time":        r.pick_time,
                    "start_time":       r.start_time,
                    "end_time":         r.end_time,
                    "duration":         r.duration,
                    "paused_at":        r.paused_at,
                    "pause_reason":     r.pause_reason or "",
                    "created_at":       r.created_at,
                }
                for r in rows
            ]
    except SQLAlchemyError as exc:
        log.error("db_load_orders failed: %s", exc)
        return []

def db_load_restock_log() -> List[dict]:
    """Load last 50 restock events."""
    try:
        with get_db() as sess:
            rows = (sess.query(DBRestockEvent)
                    .order_by(DBRestockEvent.start_time.desc())
                    .limit(50).all())
            return [
                {
                    "db_id":           r.id,
                    "row_index":       r.row_index,
                    "picker_id":       r.picker_id,
                    "start_time":      r.start_time,
                    "end_time":        r.end_time,
                    "paused_order_id": r.paused_order_id,
                }
                for r in rows
            ]
    except SQLAlchemyError as exc:
        log.error("db_load_restock_log failed: %s", exc)
        return []

def db_load_metrics() -> dict:
    """Load the most recent metrics row."""
    try:
        with get_db() as sess:
            row = (sess.query(DBMetrics)
                   .order_by(DBMetrics.saved_at.desc())
                   .first())
            if row:
                return {
                    "total_processed": row.total_processed,
                    "total_pick_time": row.total_pick_time,
                    "total_restocks":  row.total_restocks,
                }
    except SQLAlchemyError as exc:
        log.error("db_load_metrics failed: %s", exc)
    return {"total_processed": 0, "total_pick_time": 0.0, "total_restocks": 0}

def db_load_inventory() -> List[dict]:
    """Load most recent inventory snapshot."""
    try:
        with get_db() as sess:
            rows = (sess.query(DBInventorySnapshot)
                    .order_by(DBInventorySnapshot.saved_at.desc())
                    .all())
            if not rows:
                return []
            # Only keep the latest save batch (all rows with same saved_at)
            latest_ts = rows[0].saved_at
            return [
                {"sku": r.sku, "row_index": r.row_index,
                 "col_index": r.col_index, "units": r.units}
                for r in rows if abs(r.saved_at - latest_ts) < 1.0
            ]
    except SQLAlchemyError as exc:
        log.error("db_load_inventory failed: %s", exc)
        return []

def db_reset_all():
    """Drop and recreate all tables — full wipe."""
    try:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        log.info("Database reset complete.")
    except SQLAlchemyError as exc:
        log.error("db_reset_all failed: %s", exc)
        raise


def db_load_recent_skus(n: int) -> Dict[str, int]:
    """
    Query the last *n* orders from the DB and return a frequency map
    {sku_name: count} — how many times each SKU appeared across those orders.
    Only Completed orders are counted so frequency reflects actual fulfilment.
    """
    try:
        with get_db() as sess:
            rows = (
                sess.query(DBOrder.skus)
                .filter(DBOrder.status == "Completed")
                .order_by(DBOrder.created_at.desc())
                .limit(n)
                .all()
            )
        freq: Dict[str, int] = {}
        for (skus_json,) in rows:
            for sku in json.loads(skus_json):
                freq[sku] = freq.get(sku, 0) + 1
        return freq
    except SQLAlchemyError as exc:
        log.error("db_load_recent_skus failed: %s", exc)
        return {}


def db_count_completed_orders() -> int:
    """Return total number of Completed orders stored in the DB."""
    try:
        with get_db() as sess:
            return sess.query(DBOrder).filter(DBOrder.status == "Completed").count()
    except SQLAlchemyError as exc:
        log.error("db_count_completed_orders failed: %s", exc)
        return 0


# ─────────────────────────────────────────────
# CONFIG LOADER  (reads store_config.yaml)
# ─────────────────────────────────────────────

_CONFIG_FILE = "store_config.yaml"

def load_store_config(path: str = _CONFIG_FILE) -> dict:
    """Load store configuration from *path* (default: store_config.yaml).
    Raises a clear RuntimeError if the file is missing so the user knows
    exactly what to fix rather than seeing a cryptic KeyError later.
    """
    try:
        with open(path, "r") as f:
            cfg = yaml.safe_load(f)
        log.info("Store config loaded from %s", path)
        return cfg
    except FileNotFoundError:
        raise RuntimeError(
            f"Configuration file '{path}' not found. "
            "Please place store_config.yaml in the same directory as dark_store_sim.py."
        )

# ─────────────────────────────────────────────
# IN-MEMORY DATA STRUCTURES
# ─────────────────────────────────────────────

@dataclass
class SKUCell:
    name: str
    units: int

@dataclass
class RowState:
    index: int
    cells: List[SKUCell]
    is_restocking: bool = False
    restock_open_end: int = 0
    restock_end_time: float = 0.0
    is_cooler: bool = False
    assigned_picker_id: Optional[int] = None

@dataclass
class Order:
    order_id: int
    skus: List[str]
    assigned_picker_id: Optional[int] = None
    status: str = "Pending"          # Pending/Ongoing/Paused/Completed/Failed
    pick_time: float = 0.0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration: Optional[float] = None
    paused_at: Optional[float] = None
    pause_reason: str = ""
    delay_reason: str = ""          # set when status == "Delayed"
    delay_count: int  = 0           # number of times this order was delayed
    etc_seconds: float = 0.0        # Heuristic ETC (populated at queue time)

@dataclass
class RestockEvent:
    row_index: int
    picker_id: int
    start_time: float
    end_time: Optional[float] = None
    paused_order_id: Optional[int] = None
    db_id: int = -1                  # FK to DBRestockEvent.id

@dataclass
class Picker:
    picker_id: int
    status: str = "Idle"             # Idle/Picking/Restocking
    current_row: int = 0
    current_sku_idx: int = 0
    current_order_id: Optional[int] = None
    restocking_row: Optional[int] = None
    paused_order_id: Optional[int] = None


# ─────────────────────────────────────────────
# MISC HELPERS
# ─────────────────────────────────────────────

def _rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()      # type: ignore[attr-defined]

def _divider():
    if hasattr(st, "divider"):
        st.divider()
    else:
        st.markdown("---")

def _make_order_id() -> int:
    return random.randint(100_000_000_000, 999_999_999_999)

def _fmt_time(epoch) -> str:
    """Convert epoch float to HH:MM:SS. Safely handles None, NaN, and bad values."""
    try:
        if epoch is None:
            return "—"
        f = float(epoch)
        if f != f:          # NaN check (NaN != NaN is always True)
            return "—"
        return datetime.fromtimestamp(f).strftime("%H:%M:%S")
    except Exception:
        return "—"

def _status_badge(status: str) -> str:
    colours = {
        "Pending":   ("#fff3cd", "#856404", "🕐"),
        "Ongoing":   ("#cce5ff", "#004085", "⚙️"),
        "Paused":    ("#f0e6ff", "#5b2d8e", "⏸️"),
        "Delayed":   ("#fde8d8", "#7b3f00", "⏳"),
        "Completed": ("#d4edda", "#155724", "✅"),
        "Failed":    ("#f8d7da", "#721c24", "❌"),
    }
    bg, fg, icon = colours.get(status, ("#f8f9fa", "#333", "❓"))
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;'
        f'border-radius:12px;font-size:11px;font-weight:600;">{icon} {status}</span>'
    )


# ─────────────────────────────────────────────
# DARK STORE ENGINE
# ─────────────────────────────────────────────

def _row_sku_needs_restock(row: "RowState", sku: str) -> bool:
    """Return True if the SKU's cell is on a restocking row AND stock is critically low."""
    for cell in row.cells:
        if cell.name == sku:
            return cell.units <= 0   # genuinely empty — restock will fix it
    return False

# How often (in pick-completions) to flush inventory + metrics to DB
_DB_FLUSH_EVERY = 3

class DarkStore:
    def __init__(self, config: dict):
        self.cfg         = config
        self.lock        = threading.Lock()
        self._db_lock    = threading.Lock()   # serialise DB writes from threads
        self._flush_ctr  = 0                  # counts completed picks → triggers flush

        layout = config["layout"]
        self.num_rows:    int = layout["rows"]
        self.skus_per_row: int = layout["skus_per_row"]

        self.timing      = config["timing"]
        self.restock_cfg = config["restocking"]
        self.special_skus: List[str] = config.get("special_skus", [])
        self.cooler_row_index: int = self.num_rows - 1

        self.sku_position: Dict[str, Tuple[int, int]] = {}
        self.rows: List[RowState] = []
        self._build_grid(config["inventory"])

        self.pickers: Dict[int, Picker] = {
            i: Picker(picker_id=i)
            for i in range(config["pickers"]["initial_count"])
        }

        self.order_queue: deque   = deque()
        self.active_orders: Dict[int, Order] = {}
        self.completed_orders: List[Order]   = []
        self.failed_orders: List[Order]      = []

        # ── Heuristic Ranking Engine state ──────────────────────────────────
        # pending_pool: orders waiting while all pickers are busy
        # system_mode : "FCFS" | "HEURISTIC" — updated on every dispatch tick
        # pool_etc_total: running sum of ETC for all orders in pending_pool
        # ranking_policy: plain dict — threads read this, never st.session_state
        self.pending_pool: List[Order] = []
        self.system_mode: str  = "FCFS"
        self.pool_etc_total: float = 0.0
        self.ranking_policy: dict = _RANKING_POLICY.copy()

        # In-memory history (always current; DB is the persistent source)
        self.order_history: Dict[int, Order] = {}
        self._order_insert_seq: List[int]    = []

        self.restock_log: List[RestockEvent] = []
        self._restock_queue: deque           = deque()

        # Metrics — bootstrapped from DB on first load
        self.total_processed = 0
        self.total_pick_time = 0.0
        self.total_restocks  = 0

        # Restore persisted inventory levels
        self._restore_from_db()

    # ── Grid ─────────────────────────────────────────────────────────────────

    def _build_grid(self, inventory_cfg: list):
        all_skus  = [item["name"] for item in inventory_cfg]
        units_map = {item["name"]: item["units"] for item in inventory_cfg}
        special   = [s for s in all_skus if s in self.special_skus]
        regular   = [s for s in all_skus if s not in self.special_skus]

        for r in range(self.num_rows):
            is_cooler = (r == self.cooler_row_index)
            pool = special if is_cooler else regular
            cells: List[SKUCell] = []
            for c in range(self.skus_per_row):
                if not pool:
                    cells.append(SKUCell(name="(empty)", units=0))
                else:
                    flat_idx = r * self.skus_per_row + c
                    sku_name = pool[flat_idx % len(pool)]
                    cells.append(SKUCell(
                        name=sku_name,
                        units=units_map.get(sku_name, 0),
                    ))
                    if sku_name not in self.sku_position:
                        self.sku_position[sku_name] = (r, c)
            self.rows.append(RowState(index=r, cells=cells, is_cooler=is_cooler))

    # ── DB restore ────────────────────────────────────────────────────────────

    def _restore_from_db(self):
        """
        On startup, pull the last saved inventory levels and metrics
        from the DB so a server restart picks up where it left off.
        """
        # Inventory levels
        snap = db_load_inventory()
        if snap:
            for entry in snap:
                pos = self.sku_position.get(entry["sku"])
                if pos:
                    r, c = pos
                    self.rows[r].cells[c].units = entry["units"]
            log.info("Restored %d inventory records from DB.", len(snap))

        # Metrics totals
        m = db_load_metrics()
        self.total_processed = m["total_processed"]
        self.total_pick_time = m["total_pick_time"]
        self.total_restocks  = m["total_restocks"]

        # Order history (all statuses — displayed in Order Management tab)
        for row in db_load_orders():
            o = Order(
                order_id         = row["order_id"],
                skus             = row["skus"],
                assigned_picker_id = row["assigned_picker"],
                status           = row["status"],
                pick_time        = row["pick_time"],
                start_time       = row["start_time"],
                end_time         = row["end_time"],
                duration         = row["duration"],
                paused_at        = row["paused_at"],
                pause_reason     = row["pause_reason"],
            )
            # Treat any in-flight/paused orders as Failed on resume
            # (pickers aren't running — we can't safely continue mid-pick)
            if o.status in ("Ongoing", "Paused", "Delayed"):
                o.status   = "Failed"
                o.end_time = time.time()
                o.duration = (o.end_time - o.start_time) if o.start_time else 0.0
                db_upsert_order(o)   # persist the status correction
            self.order_history[o.order_id] = o
            self._order_insert_seq.append(o.order_id)

    # ── Inventory ─────────────────────────────────────────────────────────────

    def _update_stock_unsafe(self, sku: str, amount: int):
        pos = self.sku_position.get(sku)
        if pos is None:
            return False, f"SKU '{sku}' not found"
        r, c = pos
        cell = self.rows[r].cells[c]
        if amount < 0 and cell.units + amount < 0:
            return False, f"Out of stock: {sku}"
        cell.units += amount
        return True, "OK"

    def get_stock(self, sku: str) -> int:
        pos = self.sku_position.get(sku)
        if pos is None:
            return 0
        r, c = pos
        return self.rows[r].cells[c].units

    # ── Periodic DB flush ─────────────────────────────────────────────────────

    def _maybe_flush_to_db(self):
        """
        Called after each pick/restock completion (under self.lock).
        Flushes inventory + metrics every _DB_FLUSH_EVERY completions.
        """
        self._flush_ctr += 1
        if self._flush_ctr % _DB_FLUSH_EVERY == 0:
            # Do the DB write outside the main lock in a tiny thread
            # to avoid blocking the simulation loop.
            snap_rows = copy.deepcopy(self.rows)
            metrics   = (self.total_processed, self.total_pick_time, self.total_restocks)
            threading.Thread(
                target=self._flush_worker,
                args=(snap_rows, metrics),
                daemon=True,
            ).start()

    def _flush_worker(self, snap_rows: List[RowState], metrics: tuple):
        with self._db_lock:
            # Inventory
            try:
                with get_db() as sess:
                    sess.execute(text("DELETE FROM inventory_snapshots"))
                    now = time.time()
                    for row in snap_rows:
                        for c_idx, cell in enumerate(row.cells):
                            if cell.name == "(empty)":
                                continue
                            sess.add(DBInventorySnapshot(
                                sku=cell.name, row_index=row.index,
                                col_index=c_idx, units=cell.units, saved_at=now,
                            ))
            except SQLAlchemyError as exc:
                log.error("flush inventory failed: %s", exc)
            # Metrics
            try:
                with get_db() as sess:
                    tp, tpt, tr = metrics
                    sess.add(DBMetrics(
                        total_processed=tp, total_pick_time=tpt,
                        total_restocks=tr, saved_at=time.time(),
                    ))
            except SQLAlchemyError as exc:
                log.error("flush metrics failed: %s", exc)

    # ── Restock ───────────────────────────────────────────────────────────────

    def _check_restock_triggers(self):
        threshold = self.restock_cfg["threshold"]
        with self.lock:
            already_queued = set(self._restock_queue)
            for row in self.rows:
                if row.is_restocking or row.index in already_queued:
                    continue
                low_count = sum(
                    1 for c in row.cells
                    if c.units < threshold and c.name != "(empty)"
                )
                if low_count >= max(1, len(row.cells) // 2):
                    self._restock_queue.append(row.index)
        self._assign_restock_pickers()

    def _assign_restock_pickers(self):
        while True:
            with self.lock:
                if not self._restock_queue:
                    break
                row_index = self._restock_queue[0]
                row = self.rows[row_index]
                if row.is_restocking:
                    self._restock_queue.popleft()
                    continue

                picker = next(
                    (p for p in self.pickers.values() if p.status == "Idle"), None
                )
                if picker is None:
                    picker = next(
                        (p for p in self.pickers.values() if p.status == "Picking"), None
                    )
                    if picker is None:
                        break

                self._restock_queue.popleft()
                row.is_restocking      = True
                row.restock_open_end   = random.choice([0, 1])
                row.restock_end_time   = time.time() + self.restock_cfg["restock_duration_seconds"]
                row.assigned_picker_id = picker.picker_id

                paused_order_id = None
                if picker.status == "Picking" and picker.current_order_id is not None:
                    paused_order_id = picker.current_order_id
                    if paused_order_id in self.active_orders:
                        o = self.active_orders[paused_order_id]
                        o.status       = "Paused"
                        o.paused_at    = time.time()
                        o.pause_reason = (
                            f"Picker P{picker.picker_id} reassigned to restock Row {row_index}"
                        )
                        self._record_order(o)

                picker.status           = "Restocking"
                picker.paused_order_id  = paused_order_id
                picker.restocking_row   = row_index
                picker.current_order_id = None

                evt = RestockEvent(
                    row_index       = row_index,
                    picker_id       = picker.picker_id,
                    start_time      = time.time(),
                    paused_order_id = paused_order_id,
                )
                self.restock_log.append(evt)
                evt_ref = self.restock_log[-1]

            # Persist restock event start (off-lock)
            threading.Thread(
                target=self._start_restock_db_and_worker,
                args=(row_index, picker, evt_ref),
                daemon=True,
            ).start()

    def _start_restock_db_and_worker(
        self, row_index: int, picker: Picker, evt: RestockEvent
    ):
        # Write event to DB and capture the DB id
        with self._db_lock:
            db_id = db_insert_restock(evt)
        evt.db_id = db_id
        self._restock_worker(row_index, picker, evt)

    def _restock_worker(self, row_index: int, picker: Picker, evt: RestockEvent):
        duration     = self.restock_cfg["restock_duration_seconds"]
        units_to_add = self.restock_cfg["units_to_add"]
        time.sleep(duration)

        with self.lock:
            row = self.rows[row_index]
            for cell in row.cells:
                if cell.name != "(empty)":
                    cell.units += units_to_add
            row.is_restocking      = False
            row.assigned_picker_id = None
            evt.end_time           = time.time()
            self.total_restocks   += 1
            self._maybe_flush_to_db()

            paused_order_id       = picker.paused_order_id
            picker.status         = "Idle"
            picker.restocking_row  = None
            picker.paused_order_id = None

            if paused_order_id and paused_order_id in self.active_orders:
                po = self.active_orders[paused_order_id]
                po.status       = "Ongoing"
                po.paused_at    = None
                po.pause_reason = ""
                _now = time.time()
                _elapsed = (_now - po.start_time) if po.start_time is not None else 0.0
                remaining = max(po.pick_time - _elapsed, 0.5)
                po.pick_time  = remaining
                po.start_time = time.time()
                picker.status           = "Picking"
                picker.current_order_id = paused_order_id
                self._record_order(po)
                threading.Thread(
                    target=self._execute_order,
                    args=(po, picker),
                    daemon=True,
                ).start()

        # Close restock event in DB
        if evt.db_id > 0:
            threading.Thread(
                target=db_close_restock,
                args=(evt.db_id, evt.end_time),
                daemon=True,
            ).start()

        self._drain_order_queue()
        self._check_restock_triggers()

    def _restocking_row_indices(self) -> set:
        """Return the set of row indices currently under restock (caller holds self.lock)."""
        return {r.index for r in self.rows if r.is_restocking}

    def _recompute_etc_for_pool(self, policy: dict):
        """
        Refresh the ETC value for every order in pending_pool.
        Called under self.lock so restock state is consistent.
        """
        restocking = self._restocking_row_indices()
        for order in self.pending_pool:
            order.etc_seconds = calc_etc(
                skus=order.skus,
                policy=policy,
                restocking_row_indices=restocking,
                cooler_row_index=self.cooler_row_index,
                sku_position=self.sku_position,
                skus_per_row=self.skus_per_row,
            )

    def _drain_order_queue(self):
        """
        Assign queued orders to free pickers.

        Dispatch logic (Conditional Heuristic Ranking Engine):
        1. If a picker is free AND the incoming queue has an order → instant
           FCFS assignment (order never touches pending_pool).
        2. When all pickers are busy, orders accumulate in pending_pool.
        3. On each drain call, recompute pool ETC totals.
           - Pool ETC < threshold → serve pending_pool in arrival order (FCFS).
           - Pool ETC ≥ threshold → sort pending_pool ascending by ETC before
             popping (Heuristic / Quick-Win-First).
        """
        policy = self.ranking_policy   # thread-safe: plain dict on store object
        threshold = policy["backlog_threshold_seconds"]

        while True:
            with self.lock:
                picker = next(
                    (p for p in self.pickers.values() if p.status == "Idle"), None
                )

                # Also drain the legacy order_queue (FCFS instant-assign path)
                if picker and self.order_queue:
                    order = self.order_queue.popleft()
                    # Stamp ETC so the Order Management tab can show it
                    restocking = self._restocking_row_indices()
                    order.etc_seconds = calc_etc(
                        skus=order.skus,
                        policy=policy,
                        restocking_row_indices=restocking,
                        cooler_row_index=self.cooler_row_index,
                        sku_position=self.sku_position,
                        skus_per_row=self.skus_per_row,
                    )
                    # Assign immediately — no backlog threshold check needed
                    self.assign_order(order, picker)
                    continue

                if picker and self.pending_pool:
                    # Recompute ETC with current restock state
                    self._recompute_etc_for_pool(policy)
                    pool_total = pending_pool_etc_total(self.pending_pool)
                    self.pool_etc_total = pool_total

                    if pool_total >= threshold:
                        self.system_mode = "HEURISTIC"
                        self.pending_pool.sort(key=lambda o: o.etc_seconds)
                    else:
                        self.system_mode = "FCFS"

                    order = self.pending_pool.pop(0)
                    # Update running total
                    self.pool_etc_total = pending_pool_etc_total(self.pending_pool)
                else:
                    # Nothing to do — update pool total and mode then exit
                    if self.pending_pool:
                        self._recompute_etc_for_pool(policy)
                        pool_total = pending_pool_etc_total(self.pending_pool)
                        self.pool_etc_total = pool_total
                        threshold = policy["backlog_threshold_seconds"]
                        self.system_mode = "HEURISTIC" if pool_total >= threshold else "FCFS"
                    else:
                        self.pool_etc_total = 0.0
                        self.system_mode = "FCFS"
                    break

            self.assign_order(order, picker)

    # ── Route calculation ─────────────────────────────────────────────────────

    def calculate_pick_time(self, picker: Picker, sku_list: List[str]) -> float:
        t = 0.0
        cur_row, cur_col = picker.current_row, picker.current_sku_idx
        for sku in sku_list:
            pos = self.sku_position.get(sku)
            if pos is None:
                continue
            target_row, target_col = pos
            row_obj = self.rows[target_row]
            t += abs(target_row - cur_row) * self.timing["switch_row_seconds"]
            if target_row == cur_row:
                col_sw = abs(target_col - cur_col)
            elif row_obj.is_restocking:
                col_sw = (target_col if row_obj.restock_open_end == 0
                          else self.skus_per_row - 1 - target_col)
            else:
                col_sw = abs(target_col - cur_col)
            t += col_sw * self.timing["switch_sku_seconds"]
            cur_row, cur_col = target_row, target_col
        return max(t, 0.5)

    # ── Order lifecycle ───────────────────────────────────────────────────────

    def get_idle_picker(self) -> Optional[Picker]:
        for p in self.pickers.values():
            if p.status == "Idle":
                return p
        return None

    def assign_order(self, order: Order, picker: Picker):
        pick_time               = self.calculate_pick_time(picker, order.skus)
        order.pick_time         = pick_time
        order.status            = "Ongoing"
        order.assigned_picker_id = picker.picker_id
        order.start_time        = time.time()
        picker.status           = "Picking"
        picker.current_order_id = order.order_id
        with self.lock:
            self.active_orders[order.order_id] = order
            self._record_order(order)
        threading.Thread(
            target=self._execute_order, args=(order, picker), daemon=True
        ).start()

    def _execute_order(self, order: Order, picker: Picker):
        """
        Pick lifecycle with Delayed protection:
        1. Sleep for pick_time (simulates travel + picking).
        2. If picker was pulled to restock while sleeping → bail (Paused guard).
        3. Pre-flight: check every SKU has stock >= 1 BEFORE deducting anything.
           - Any SKU with 0 units → mark Delayed, wait for active restocks to
             finish (poll every 2 s), then retry — no limit on waits caused by
             restocking.
           - If units > 0 for all SKUs but deduction still fails (race condition
             where a concurrent pick drained the last unit between check and
             deduct) → delay up to MAX_STOCK_RETRIES times, then fail for real.
        4. Deduct all SKUs atomically (under lock) only when everything is
           confirmed available.
        """
        MAX_STOCK_RETRIES = 8   # safety valve for genuine concurrent depletion

        time.sleep(order.pick_time)

        # ── Paused guard (picker was reassigned to restock mid-sleep) ──────────
        with self.lock:
            if order.status == "Paused":
                return   # restock worker will re-fire us after it finishes

        # ── Main execution loop ────────────────────────────────────────────────
        while True:
            with self.lock:
                # Re-check Paused inside lock (avoids TOCTOU race)
                if order.status == "Paused":
                    return

                # Step 1: pre-flight availability check — no mutations yet
                blocked: List[str] = []   # SKUs with 0 stock right now
                restocking_rows: set = set()

                for sku in order.skus:
                    pos = self.sku_position.get(sku)
                    if not pos:
                        continue
                    r, c = pos
                    row_obj  = self.rows[r]
                    cell     = row_obj.cells[c]
                    if cell.units <= 0:
                        blocked.append(sku)
                        if row_obj.is_restocking:
                            restocking_rows.add(r)

                # Step 2: if any SKU is at 0 units, decide how to handle
                if blocked:
                    order.delay_count += 1
                    if restocking_rows:
                        # Rows are already being restocked — just wait
                        reason = (
                            f"⏳ Stock depleted for {', '.join(blocked)} — "
                            f"waiting for Row(s) {', '.join(str(r) for r in restocking_rows)} "
                            f"to finish restocking (hold #{order.delay_count})"
                        )
                    else:
                        # Stock is 0 but no restock is running yet —
                        # trigger one and wait
                        for sku in blocked:
                            pos = self.sku_position.get(sku)
                            if pos and pos[0] not in {row.index for row in self.rows if row.is_restocking}:
                                if pos[0] not in set(self._restock_queue):
                                    self._restock_queue.append(pos[0])
                        reason = (
                            f"⏳ Stock depleted for {', '.join(blocked)} — "
                            f"restock triggered, holding order (hold #{order.delay_count})"
                        )

                    order.status       = "Delayed"
                    order.delay_reason = reason
                    self._record_order(order)

                # trigger any newly queued restocks (outside lock below)
                needs_restock_assign = bool(blocked)

            # Outside lock: trigger restock assignment if needed, then wait
            if needs_restock_assign:
                self._assign_restock_pickers()
                # Poll every 2 s until stock is replenished or picker is freed
                time.sleep(2.0)
                # Loop back to re-check
                continue

            # Step 3: all SKUs have stock — attempt atomic deduction
            with self.lock:
                if order.status == "Paused":
                    return

                failed_skus: List[str] = []
                for sku in order.skus:
                    ok, _ = self._update_stock_unsafe(sku, -1)
                    if not ok:
                        failed_skus.append(sku)

                if failed_skus:
                    # Race condition: someone else just took the last unit
                    if order.delay_count < MAX_STOCK_RETRIES:
                        order.delay_count += 1
                        order.status       = "Delayed"
                        order.delay_reason = (
                            f"⏳ Stock race for {', '.join(failed_skus)} — "
                            f"retry #{order.delay_count}"
                        )
                        self._record_order(order)
                        # Fall through to sleep + retry
                    else:
                        # Genuinely exhausted all retries → fail for real
                        order.end_time = time.time()
                        order.duration = max(
                            order.end_time - order.start_time, 0.0
                        ) if order.start_time is not None else 0.0
                        order.status   = "Failed"
                        self.failed_orders.append(order)
                        self.active_orders.pop(order.order_id, None)
                        self._record_order(order)
                        self._maybe_flush_to_db()
                        picker.status           = "Idle"
                        picker.current_order_id = None
                        break   # exit while loop → fall through to restock check

                else:
                    # ── SUCCESS ─────────────────────────────────────────────
                    order.end_time = time.time()
                    order.duration = max(
                        order.end_time - order.start_time, 0.0
                    ) if order.start_time is not None else 0.0
                    order.status   = "Completed"
                    if order.delay_count > 0:
                        order.delay_reason = (
                            f"Completed after {order.delay_count} hold(s)"
                        )
                    self.completed_orders.append(order)
                    self.total_processed += 1
                    self.total_pick_time += order.pick_time
                    self.active_orders.pop(order.order_id, None)
                    self._record_order(order)
                    self._maybe_flush_to_db()

                    last_sku = order.skus[-1] if order.skus else None
                    if last_sku and last_sku in self.sku_position:
                        picker.current_row, picker.current_sku_idx = self.sku_position[last_sku]
                    picker.status           = "Idle"
                    picker.current_order_id = None
                    break   # exit while loop

            # Short back-off before retrying after a stock race
            if order.status in ("Delayed",):
                time.sleep(1.5)

        self._check_restock_triggers()
        self._drain_order_queue()

    # ── In-memory + DB history ────────────────────────────────────────────────

    def _record_order(self, order: Order):
        """
        Snapshot order in-memory AND persist to DB.
        Must be called under self.lock for the in-memory part;
        the DB write is dispatched to a daemon thread.
        """
        snap      = copy.copy(order)
        snap.skus = list(order.skus)
        if order.order_id not in self.order_history:
            self._order_insert_seq.append(order.order_id)
        self.order_history[order.order_id] = snap

        # Fire-and-forget DB write (serialised via _db_lock)
        order_copy = copy.copy(order)
        order_copy.skus = list(order.skus)
        threading.Thread(
            target=self._write_order_to_db,
            args=(order_copy,),
            daemon=True,
        ).start()

    def _write_order_to_db(self, order: Order):
        with self._db_lock:
            db_upsert_order(order)

    # ── Warehouse Slotting Optimisation ──────────────────────────────────────

    def reorder_warehouse(self, n_orders: int) -> dict:
        """
        Popularity-Based Slotting Optimisation.

        Algorithm
        ---------
        1. Pull SKU frequency from the last *n_orders* Completed DB records.
        2. Sort non-cooler SKUs by frequency descending.
        3. Score every non-cooler grid coordinate by real travel cost from (0,0):
               score(r, c) = r × switch_row_s + c × switch_sku_s
           Lower score ⟹ closer to the picker dispatch point ⟹ "Golden Zone".
        4. Assign highest-frequency SKUs to lowest-score coordinates.
        5. Cooler SKUs (self.special_skus) are NEVER relocated.
        6. Preserve current unit counts by SKU name across the remap.
        7. Persist new layout to DB (background thread).
        8. Return a result dict with efficiency gain, heatmap data, etc.

        Must be called from the main (Streamlit) thread — acquires self.lock
        internally so background picker threads see the new positions atomically.

        Returns
        -------
        dict with keys:
          ok              bool   — False if insufficient data
          reason          str    — human-readable status message
          orders_analysed int    — how many completed orders were used
          skus_moved      int    — how many non-cooler SKUs changed position
          efficiency_gain float  — % reduction in simulated avg travel time
          freq_map        dict   — {sku: count}
          before_grid     list   — [[sku, ...], ...] row×col before remap
          after_grid      list   — [[sku, ...], ...] row×col after remap
          before_avg_s    float  — avg simulated pick time before (seconds)
          after_avg_s     float  — avg simulated pick time after (seconds)
        """
        # ── Step 1: frequency data (outside lock — DB call) ───────────────────
        freq_map = db_load_recent_skus(n_orders)
        orders_analysed = min(n_orders, sum(freq_map.values()) or 0)

        if not freq_map:
            return {"ok": False, "reason": "No completed orders found in the database."}

        sw_row = self.timing["switch_row_seconds"]
        sw_col = self.timing["switch_sku_seconds"]

        # ── Step 2: snapshot current state for efficiency calc (outside lock) ─
        # We need last-100 completed orders' SKU lists to benchmark before/after
        benchmark_orders = db_load_recent_skus(100)  # freq map, good enough proxy
        # We'll do a proper benchmark below using actual SKU lists

        try:
            with get_db() as sess:
                bench_rows = (
                    sess.query(DBOrder.skus)
                    .filter(DBOrder.status == "Completed")
                    .order_by(DBOrder.created_at.desc())
                    .limit(100)
                    .all()
                )
            bench_sku_lists = [json.loads(r[0]) for r in bench_rows]
        except Exception:
            bench_sku_lists = []

        with self.lock:
            # ── Step 3: capture before-state ────────────────────────────────
            before_grid = [
                [cell.name for cell in row.cells]
                for row in self.rows
            ]
            old_sku_position = dict(self.sku_position)

            # Carry forward current unit counts by SKU name
            units_by_sku: Dict[str, int] = {}
            for row in self.rows:
                for cell in row.cells:
                    if cell.name != "(empty)":
                        units_by_sku[cell.name] = cell.units

            # ── Step 4: build coordinate pool (non-cooler only) ──────────────
            non_cooler_coords: List[Tuple[int, int]] = []
            for r in range(self.num_rows):
                if r == self.cooler_row_index:
                    continue
                for c in range(self.skus_per_row):
                    non_cooler_coords.append((r, c))

            # Sort coordinates by real travel cost from dispatch point (0, 0)
            non_cooler_coords.sort(
                key=lambda rc: rc[0] * sw_row + rc[1] * sw_col
            )

            # ── Step 5: sort non-cooler SKUs by frequency ─────────────────────
            all_non_cooler_skus = [
                s for s in self.sku_position
                if s not in self.special_skus
            ]
            # SKUs not in freq_map (never ordered) get frequency = 0
            all_non_cooler_skus.sort(
                key=lambda s: freq_map.get(s, 0), reverse=True
            )

            # ── Step 6: assign SKUs to coordinates ───────────────────────────
            # Map: coord → sku  (some coords may be "(empty)" if fewer SKUs than slots)
            coord_to_sku: Dict[Tuple[int, int], str] = {}
            for i, coord in enumerate(non_cooler_coords):
                if i < len(all_non_cooler_skus):
                    coord_to_sku[coord] = all_non_cooler_skus[i]
                else:
                    coord_to_sku[coord] = "(empty)"

            # ── Step 7: rebuild rows and sku_position ─────────────────────────
            new_sku_position: Dict[str, Tuple[int, int]] = {}

            # Non-cooler rows
            for r in range(self.num_rows):
                if r == self.cooler_row_index:
                    continue
                for c in range(self.skus_per_row):
                    sku_name  = coord_to_sku.get((r, c), "(empty)")
                    new_units = units_by_sku.get(sku_name, 0)
                    self.rows[r].cells[c] = SKUCell(name=sku_name, units=new_units)
                    if sku_name != "(empty)":
                        new_sku_position[sku_name] = (r, c)

            # Cooler row — positions unchanged, just copy back
            cooler_row = self.rows[self.cooler_row_index]
            for c, cell in enumerate(cooler_row.cells):
                if cell.name != "(empty)":
                    new_sku_position[cell.name] = (self.cooler_row_index, c)

            # Atomically replace the position map
            self.sku_position = new_sku_position

            # ── Step 8: capture after-state ───────────────────────────────────
            after_grid = [
                [cell.name for cell in row.cells]
                for row in self.rows
            ]

            new_sku_position_snap = dict(self.sku_position)  # for benchmark

        # ── Step 9: count moved SKUs ─────────────────────────────────────────
        skus_moved = sum(
            1 for sku, old_pos in old_sku_position.items()
            if sku not in self.special_skus and new_sku_position_snap.get(sku) != old_pos
        )

        # ── Step 10: efficiency gain — simulate pick time before and after ────
        def _sim_pick(sku_list, pos_map):
            """Simulate travel time from (0,0) through sku_list using pos_map."""
            t = 0.0
            cur_r, cur_c = 0, 0
            for sku in sku_list:
                pos = pos_map.get(sku)
                if pos is None:
                    continue
                tr, tc = pos
                t += abs(tr - cur_r) * sw_row + abs(tc - cur_c) * sw_col
                cur_r, cur_c = tr, tc
            return max(t, 0.5)

        before_times = [_sim_pick(sl, old_sku_position) for sl in bench_sku_lists if sl]
        after_times  = [_sim_pick(sl, new_sku_position_snap) for sl in bench_sku_lists if sl]

        before_avg = sum(before_times) / len(before_times) if before_times else 0.0
        after_avg  = sum(after_times)  / len(after_times)  if after_times  else 0.0
        gain_pct   = ((before_avg - after_avg) / before_avg * 100.0) if before_avg > 0 else 0.0

        # ── Step 11: persist new layout to DB ────────────────────────────────
        threading.Thread(target=db_save_inventory, args=(self,), daemon=True).start()

        log.info(
            "Warehouse reordered: %d SKUs moved, efficiency gain %.1f%% "
            "(%d orders analysed, n_lookback=%d)",
            skus_moved, gain_pct, orders_analysed, n_orders,
        )

        return {
            "ok":               True,
            "reason":           "Optimisation complete.",
            "orders_analysed":  orders_analysed,
            "skus_moved":       skus_moved,
            "efficiency_gain":  round(gain_pct, 2),
            "freq_map":         freq_map,
            "before_grid":      before_grid,
            "after_grid":       after_grid,
            "before_avg_s":     round(before_avg, 2),
            "after_avg_s":      round(after_avg, 2),
        }


# ─────────────────────────────────────────────
# ORDER GENERATOR  (background thread)
# ─────────────────────────────────────────────

class OrderGenerator:
    def __init__(self, store: DarkStore):
        self.store       = store
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def _loop(self):
        all_skus = list(self.store.sku_position.keys())
        while not self._stop_event.is_set():
            # 1.8–2.2 s randomised jitter as specified
            self._stop_event.wait(timeout=random.uniform(1.8, 2.2))
            if self._stop_event.is_set():
                break

            num_skus = random.randint(1, min(5, len(all_skus)))
            selected = random.sample(all_skus, num_skus)
            order    = Order(order_id=_make_order_id(), skus=selected, status="Pending")

            policy = self.store.ranking_policy   # thread-safe: plain dict on store

            with self.store.lock:
                # Stamp ETC immediately so the pool always has a value
                restocking = self.store._restocking_row_indices()
                order.etc_seconds = calc_etc(
                    skus=order.skus,
                    policy=policy,
                    restocking_row_indices=restocking,
                    cooler_row_index=self.store.cooler_row_index,
                    sku_position=self.store.sku_position,
                    skus_per_row=self.store.skus_per_row,
                )
                self.store._record_order(order)
                picker = self.store.get_idle_picker()

            if picker:
                # Instant FCFS assignment — picker was free
                self.store.assign_order(order, picker)
            else:
                # All pickers busy — enter pending_pool for heuristic evaluation
                with self.store.lock:
                    self.store.pending_pool.append(order)


# ─────────────────────────────────────────────
# RENDER : LIVE SIMULATION
# ─────────────────────────────────────────────

def render_grid(store: DarkStore):
    with store.lock:
        rows_snap    = copy.deepcopy(store.rows)
        pickers_snap = copy.deepcopy(store.pickers)

    picker_locs: Dict[Tuple[int, int], List[str]] = {}
    for p in pickers_snap.values():
        if p.status == "Idle":
            continue
        if p.status == "Restocking" and p.restocking_row is not None:
            key  = (p.restocking_row, 0)
            icon = f"📦 P{p.picker_id}"
        else:
            key  = (p.current_row, p.current_sku_idx)
            icon = f"🚶 P{p.picker_id}"
        picker_locs.setdefault(key, []).append(icon)

    for row in rows_snap:
        label = "🧊 Cooler Row" if row.is_cooler else f"Row {row.index}"
        if row.is_restocking:
            who   = f" (P{row.assigned_picker_id})" if row.assigned_picker_id is not None else ""
            label += f"  📦 RESTOCKING{who}"
            hdr   = "#e67e22"
        else:
            hdr = "#1e3a5f" if row.is_cooler else "#2c3e50"

        st.markdown(
            f'<div style="background:{hdr};color:white;padding:4px 10px;'
            f'border-radius:6px 6px 0 0;font-weight:600;font-size:13px;">{label}</div>',
            unsafe_allow_html=True,
        )
        cols = st.columns(store.skus_per_row)
        for c_idx, (cell, col) in enumerate(zip(row.cells, cols)):
            ph = picker_locs.get((row.index, c_idx), [])
            with col:
                if row.is_restocking:
                    bg, border = "#fff4e6", "2px solid #e67e22"
                elif row.is_cooler:
                    bg, border = "#f0f8ff", "1px solid #aed6f1"
                else:
                    bg, border = "#f8f9fa", "1px solid #dee2e6"

                badge = "".join(
                    f'<div style="font-size:10px;color:#e67e22;text-align:center">{ico}</div>'
                    for ico in ph
                )
                sc = "#e74c3c" if cell.units < 5 else "#27ae60" if cell.units > 10 else "#f39c12"
                st.markdown(
                    f'<div style="background:{bg};border:{border};border-radius:6px;'
                    f'padding:8px;text-align:center;min-height:80px;">'
                    f'{badge}'
                    f'<div style="font-size:11px;font-weight:600;color:#2c3e50">{cell.name}</div>'
                    f'<div style="font-size:16px;font-weight:700;color:{sc}">{cell.units}</div>'
                    f'<div style="font-size:9px;color:#7f8c8d">units</div></div>',
                    unsafe_allow_html=True,
                )
        st.markdown("<div style='margin-bottom:6px'></div>", unsafe_allow_html=True)


def render_inventory_table(store: DarkStore):
    with store.lock:
        rows_snap = copy.deepcopy(store.rows)
    records, seen = [], set()
    for row in rows_snap:
        for cell in row.cells:
            if cell.name in ("(empty)", ) or cell.name in seen:
                continue
            seen.add(cell.name)
            threshold = store.restock_cfg["threshold"]
            status_str = (
                "📦 Restocking" if row.is_restocking else
                "🔴 Low"        if cell.units < threshold else
                "🟢 OK"
            )
            records.append({
                "SKU": cell.name,
                "Row": "Cooler" if row.is_cooler else str(row.index),
                "Stock": cell.units,
                "Status": status_str,
            })
    if records:
        st.dataframe(
            pd.DataFrame(records).sort_values("SKU").set_index("SKU"),
            width='stretch',
        )


def render_picker_status(store: DarkStore):
    with store.lock:
        pickers_snap = copy.deepcopy(store.pickers)
        active_snap  = copy.deepcopy(store.active_orders)

    st.markdown("**👥 Picker Status**")
    for p in pickers_snap.values():
        if p.status == "Idle":
            colour, icon, detail = "#d4edda", "💤", "Idle"
        elif p.status == "Restocking":
            colour, icon = "#fff4e6", "📦"
            detail = f"Restocking Row {p.restocking_row}"
            if p.paused_order_id:
                detail += f" — Order #{p.paused_order_id} paused"
        else:
            colour, icon = "#cce5ff", "🚶"
            o = active_snap.get(p.current_order_id)
            if o:
                _now = time.time()
                _elapsed = (_now - o.start_time) if o.start_time is not None else 0.0
                remaining = max(o.pick_time - _elapsed, 0.0)
                detail    = f"Picking #{p.current_order_id} (~{remaining:.0f}s left)"
            else:
                detail = "Picking"
        st.markdown(
            f'<div style="background:{colour};border-radius:6px;padding:6px 10px;'
            f'margin-bottom:4px;font-size:12px;">'
            f'<b>{icon} Picker P{p.picker_id}</b> — {detail}</div>',
            unsafe_allow_html=True,
        )


def render_restock_log(store: DarkStore):
    with store.lock:
        log_snap = list(reversed(store.restock_log[-8:]))

    if not log_snap:
        # Fall back to DB records (useful after a server restart)
        db_records = db_load_restock_log()[:8]
        if not db_records:
            st.caption("No restocks yet.")
            return
        for r in db_records:
            done   = r["end_time"] is not None
            dur    = f"{r['end_time'] - r['start_time']:.1f}s" if done else "In progress…"
            colour = "#d4edda" if done else "#fff4e6"
            border = "#28a745"  if done else "#e67e22"
            paused = f" | Paused order #{r['paused_order_id']}" if r["paused_order_id"] else ""
            st.markdown(
                f'<div style="background:{colour};border-left:4px solid {border};'
                f'padding:5px 10px;border-radius:4px;margin-bottom:3px;font-size:11px;">'
                f'{"✅" if done else "📦"} Row {r["row_index"]} — '
                f'Picker P{r["picker_id"]} — {dur}{paused}</div>',
                unsafe_allow_html=True,
            )
        return

    for evt in log_snap:
        done   = evt.end_time is not None
        dur    = f"{evt.end_time - evt.start_time:.1f}s" if done else "In progress…"
        colour = "#d4edda" if done else "#fff4e6"
        border = "#28a745"  if done else "#e67e22"
        paused = f" | Paused order #{evt.paused_order_id}" if evt.paused_order_id else ""
        st.markdown(
            f'<div style="background:{colour};border-left:4px solid {border};'
            f'padding:5px 10px;border-radius:4px;margin-bottom:3px;font-size:11px;">'
            f'{"✅" if done else "📦"} Row {evt.row_index} — '
            f'Picker P{evt.picker_id} — {dur}{paused}</div>',
            unsafe_allow_html=True,
        )


def render_pending_pool(store: DarkStore):
    """Display the heuristic pending pool with ETC values and current mode."""
    with store.lock:
        policy    = store.ranking_policy   # always consistent with what threads use
        threshold = policy["backlog_threshold_seconds"]
        pool_snap  = list(store.pending_pool)
        mode       = store.system_mode
        pool_total = store.pool_etc_total

    if not pool_snap:
        st.caption("Pending pool is empty — all orders assigned instantly (FCFS).")
        return

    mode_colour = "#e67e22" if mode == "HEURISTIC" else "#27ae60"
    mode_label  = "⚡ Heuristic (sorted by ETC)" if mode == "HEURISTIC" else "✅ FCFS (arrival order)"
    st.markdown(
        f'<div style="background:#f8f9fa;border-left:4px solid {mode_colour};'
        f'padding:6px 12px;border-radius:4px;margin-bottom:6px;font-size:12px;">'
        f'<b>Mode:</b> <span style="color:{mode_colour}">{mode_label}</span> &nbsp;|&nbsp; '
        f'<b>Pool ETC:</b> {pool_total:.0f}s / {threshold}s threshold</div>',
        unsafe_allow_html=True,
    )

    rows = [
        {
            "Order ID":      str(o.order_id),
            "# SKUs":        len(o.skus),
            "Calc. ETC (s)": o.etc_seconds,
            "SKUs":          ", ".join(o.skus),
        }
        for o in pool_snap
    ]
    st.dataframe(pd.DataFrame(rows).set_index("Order ID"), width='stretch')


def render_live_orders(store: DarkStore):
    with store.lock:
        active    = list(store.active_orders.values())
        completed = list(store.completed_orders[-5:])
        failed    = list(store.failed_orders[-3:])
        queue_len = len(store.order_queue)

    def card(bg, border, text):
        st.markdown(
            f'<div style="background:{bg};border-left:4px solid {border};'
            f'padding:6px 10px;border-radius:4px;margin-bottom:4px;font-size:12px;">{text}</div>',
            unsafe_allow_html=True,
        )

    ongoing  = [o for o in active if o.status == "Ongoing"]
    paused   = [o for o in active if o.status == "Paused"]
    delayed  = [o for o in active if o.status == "Delayed"]

    if ongoing:
        st.markdown("**⚙️ Ongoing**")
        for o in ongoing:
            card("#cce5ff", "#004085",
                 f"#{o.order_id} &nbsp;|&nbsp; Picker P{o.assigned_picker_id} &nbsp;|&nbsp; "
                 f"{', '.join(o.skus)} &nbsp;|&nbsp; Est. {o.pick_time:.1f}s")
    if delayed:
        st.markdown("**⏳ Delayed (waiting for restock)**")
        for o in delayed:
            card("#fde8d8", "#7b3f00",
                 f"#{o.order_id} &nbsp;|&nbsp; {o.delay_reason}")
    if paused:
        st.markdown("**⏸️ Paused (picker restocking)**")
        for o in paused:
            card("#f0e6ff", "#8e44ad", f"#{o.order_id} &nbsp;|&nbsp; {o.pause_reason}")
    if queue_len:
        st.info(f"📋 {queue_len} order(s) queued")
    if completed:
        st.markdown("**✅ Completed**")
        for o in reversed(completed):
            dur_str = f" ({o.duration:.1f}s)" if o.duration is not None else ""
            card("#d4edda", "#28a745", f"#{o.order_id} — {', '.join(o.skus)}{dur_str}")
    if failed:
        st.markdown("**❌ Failed**")
        for o in reversed(failed):
            card("#f8d7da", "#dc3545", f"#{o.order_id} — {', '.join(o.skus)}")


# ─────────────────────────────────────────────
# RENDER : ORDER MANAGEMENT TAB
# ─────────────────────────────────────────────

def render_order_management(store: DarkStore):
    with store.lock:
        seq     = list(store._order_insert_seq)
        history = [copy.copy(store.order_history[oid])
                   for oid in seq if oid in store.order_history]

    if not history:
        st.info("No orders yet. Press **▶️ Start** in the sidebar to begin generating orders.")
        return

    fc1, fc2, fc3 = st.columns([2, 2, 1])
    with fc1:
        status_filter = st.selectbox(
            "Filter by Status",
            ["All", "Pending", "Ongoing", "Paused", "Delayed", "Completed", "Failed"],
            key="om_status_filter",
        )
    with fc2:
        search_id = st.text_input("Search by Order ID (partial)", key="om_search_id")
    with fc3:
        sort_newest = st.checkbox("Newest first", value=True, key="om_sort")

    filtered = list(reversed(history)) if sort_newest else list(history)
    if status_filter != "All":
        filtered = [o for o in filtered if o.status == status_filter]
    if search_id.strip():
        filtered = [o for o in filtered if search_id.strip() in str(o.order_id)]

    total   = len(history)
    ongoing = sum(1 for o in history if o.status == "Ongoing")
    paused  = sum(1 for o in history if o.status == "Paused")
    delayed = sum(1 for o in history if o.status == "Delayed")
    done    = sum(1 for o in history if o.status == "Completed")
    failed  = sum(1 for o in history if o.status == "Failed")
    durs    = [o.duration for o in history if o.duration is not None]
    avg_dur = sum(durs) / len(durs) if durs else 0.0

    m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
    m1.metric("📋 Total",    total)
    m2.metric("⚙️ Ongoing",  ongoing)
    m3.metric("⏸️ Paused",   paused)
    m4.metric("⏳ Delayed",   delayed)
    m5.metric("✅ Completed", done)
    m6.metric("❌ Failed",    failed)
    m7.metric("⏱ Avg Dur",  f"{avg_dur:.1f}s")

    _divider()
    st.subheader(f"📋 Order History  ({len(filtered)} shown)")

    if filtered:
        rows_data = [
            {
                "Order ID":     str(o.order_id),
                "Status":       o.status,
                "Picker":       f"P{o.assigned_picker_id}" if o.assigned_picker_id is not None else "—",
                "SKUs":         ", ".join(o.skus),
                "# Items":      len(o.skus),
                "Calc. ETC (s)": f"{o.etc_seconds:.1f}" if o.etc_seconds else "—",
                "Start":        _fmt_time(o.start_time),
                "End":          _fmt_time(o.end_time),
                "Duration":     f"{o.duration:.1f}s" if o.duration is not None else "—",
                "Pause Reason": o.pause_reason or "—",
                "Delay Reason": o.delay_reason or "—",
                "Delays":       o.delay_count,
            }
            for o in filtered
        ]
        st.dataframe(pd.DataFrame(rows_data).set_index("Order ID"), width='stretch')
    else:
        st.warning("No orders match the current filter.")

    _divider()
    st.subheader("🔍 Order Detail")

    id_options = [str(o.order_id) for o in filtered]
    if not id_options:
        st.info("No orders to inspect with current filter.")
        return

    sel_id = st.selectbox("Select Order ID to inspect", id_options, key="om_selected_id")
    o = next((x for x in history if str(x.order_id) == sel_id), None)
    if o is None:
        st.warning("Order not found.")
        return

    with st.container():
        st.markdown(
            f'<div style="background:#f8f9fa;border:1px solid #dee2e6;'
            f'border-radius:10px;padding:18px 20px;">'
            f'<h4 style="margin:0 0 14px 0;">Order &nbsp;<code>{o.order_id}</code>'
            f'&nbsp;&nbsp;{_status_badge(o.status)}</h4>',
            unsafe_allow_html=True,
        )
        dc1, dc2 = st.columns(2)
        with dc1:
            st.markdown("**👤 Assigned Picker**")
            st.markdown(
                f"`Picker P{o.assigned_picker_id}`"
                if o.assigned_picker_id is not None else "_Not yet assigned_"
            )
            st.markdown("**🕐 Start**"); st.markdown(f"`{_fmt_time(o.start_time)}`")
            st.markdown("**🏁 End**");   st.markdown(f"`{_fmt_time(o.end_time)}`")
            st.markdown("**⏱ Duration**")
            st.markdown(f"`{f'{o.duration:.1f}s' if o.duration else '—'}`")
            if o.status == "Paused":
                st.markdown("**⏸️ Pause Reason**")
                st.markdown(f"_{o.pause_reason}_")
        with dc2:
            st.markdown(f"**📦 SKUs ({len(o.skus)} items)**")
            for sku in o.skus:
                st.markdown(f"- {sku}")

        st.markdown("**📊 Fulfillment Progress**")
        if o.status == "Completed":
            st.progress(1.0)
            st.caption(f"Completed in {o.duration:.1f}s" if o.duration else "Completed")
        elif o.status == "Failed":
            st.progress(1.0)
            st.caption("❌ Failed — one or more SKUs were out of stock")
        elif o.status == "Paused":
            st.progress(0.5)
            st.caption(f"⏸️ {o.pause_reason} — will resume automatically")
        elif o.status == "Delayed":
            st.progress(0.3)
            st.caption(f"⏳ {o.delay_reason}")
        elif o.status == "Ongoing" and o.start_time is not None and o.pick_time:
            _now     = time.time()
            elapsed  = max(_now - o.start_time, 0.0)
            remaining = max(o.pick_time - elapsed, 0.0)
            progress = min(elapsed / o.pick_time, 0.99) if o.pick_time > 0 else 0.0
            st.progress(progress)
            st.caption(f"~{remaining:.1f}s remaining (est. {o.pick_time:.1f}s total)")
        else:
            st.progress(0.0)
            st.caption("Waiting for a picker…")
        st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# RENDER : DATABASE ANALYTICS TAB
# ─────────────────────────────────────────────

def render_db_analytics():
    st.subheader("🗄️ Database Analytics  *(all-time, cross-run)*")

    # Pull directly from SQLite — reflects every run
    all_orders = db_load_orders()
    all_restocks = db_load_restock_log()

    if not all_orders:
        st.info("No data in the database yet.")
        return

    df_o = pd.DataFrame(all_orders)
    # Coerce all numeric columns — SQLAlchemy may return mixed object dtype
    for _col in ("start_time", "end_time", "duration", "pick_time", "paused_at"):
        if _col in df_o.columns:
            df_o[_col] = pd.to_numeric(df_o[_col], errors="coerce")
        if "start_time" in df_o.columns:
            # 1. Ensure numeric types for comparison
            df_o["start_time_num"] = pd.to_numeric(df_o["start_time"], errors='coerce')
            
            # 2. Heuristic: timestamps > 2e10 are likely milliseconds (13 digits)
            is_ms = df_o["start_time_num"] > 2e10
            
            # 3. Apply conversion based on detected unit
            df_o["start_dt"] = pd.NaT
            df_o.loc[is_ms, "start_dt"] = pd.to_datetime(df_o.loc[is_ms, "start_time_num"], unit="ms", errors="coerce")
            df_o.loc[~is_ms, "start_dt"] = pd.to_datetime(df_o.loc[~is_ms, "start_time_num"], unit="s", errors="coerce")
            
            # 4. Cleanup temporary column
            df_o = df_o.drop(columns=["start_time_num"])
    df_o["start_dt"] = pd.to_datetime(df_o["start_time"], unit="s", errors="coerce")
    df_o["end_dt"]   = pd.to_datetime(df_o["end_time"],   unit="s", errors="coerce")

    # ── Summary cards ──────────────────────────────────────────────────────
    total   = len(df_o)
    done    = int((df_o["status"] == "Completed").sum())
    failed  = int((df_o["status"] == "Failed").sum())
    avg_dur = df_o[df_o["status"] == "Completed"]["duration"].mean()
    avg_dur = round(avg_dur, 2) if pd.notna(avg_dur) else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📋 All-Time Orders",    total)
    c2.metric("✅ Completed",           done)
    c3.metric("❌ Failed",              failed)
    c4.metric("⏱ Avg Duration",       f"{avg_dur}s")
    c5.metric("📦 Total Restocks",     len(all_restocks))

    _divider()

    # ── Orders over time ───────────────────────────────────────────────────
    st.markdown("**📈 Orders Over Time (completed per minute)**")
    completed_df = df_o[df_o["status"] == "Completed"].copy()
    if not completed_df.empty and completed_df["end_dt"].notna().any():
        completed_df = completed_df.dropna(subset=["end_dt"])
        completed_df["minute"] = completed_df["end_dt"].dt.floor("min")
        by_min = completed_df.groupby("minute").size().reset_index(name="count")
        by_min["minute"] = by_min["minute"].astype(str)
        st.bar_chart(by_min.set_index("minute")["count"])
    else:
        st.caption("Not enough data yet.")

    # ── Status breakdown ───────────────────────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**📊 Status Breakdown**")
        status_counts = df_o["status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        st.dataframe(status_counts.set_index("Status"), width='stretch')

    with col_b:
        st.markdown("**🚶 Orders per Picker**")
        picker_counts = (
            df_o[df_o["assigned_picker"].notna()]
            .groupby("assigned_picker")
            .size()
            .reset_index(name="Orders")
        )
        picker_counts["assigned_picker"] = "Picker P" + picker_counts["assigned_picker"].astype(int).astype(str)
        picker_counts.columns = ["Picker", "Orders"]
        st.dataframe(picker_counts.set_index("Picker"), width='stretch')

    _divider()

    # ── Restock table ──────────────────────────────────────────────────────
    st.markdown("**📦 Restock Event Log (last 50)**")
    if all_restocks:
        df_r = pd.DataFrame(all_restocks)
        # Coerce to numeric first — SQLAlchemy may return mixed object dtype
        df_r["start_time"] = pd.to_numeric(df_r["start_time"], errors="coerce")
        df_r["end_time"]   = pd.to_numeric(df_r["end_time"],   errors="coerce")
        df_r["start"] = df_r["start_time"].apply(_fmt_time)
        df_r["end"]   = df_r["end_time"].apply(_fmt_time)
        df_r["dur"]   = df_r.apply(
            lambda x: (
                f"{(x['end_time'] - x['start_time']):.1f}s"
                if pd.notna(x["end_time"]) and pd.notna(x["start_time"])
                else "ongoing"
            ),
            axis=1,
        )
        df_r = df_r[["db_id", "row_index", "picker_id", "start", "end", "dur", "paused_order_id"]]
        df_r.columns = ["ID", "Row", "Picker", "Start", "End", "Duration", "Paused Order"]
        st.dataframe(df_r.set_index("ID"), width='stretch')
    else:
        st.caption("No restock events recorded yet.")

    _divider()

    # ── Raw orders table ───────────────────────────────────────────────────
    st.markdown("**📋 Full Order History (DB)**")
    display_df = df_o[["order_id", "status", "assigned_picker", "duration",
                        "start_time", "end_time", "pause_reason"]].copy()
    display_df["order_id"]        = display_df["order_id"].astype(str)
    display_df["start_time"]      = display_df["start_time"].apply(_fmt_time)
    display_df["end_time"]        = display_df["end_time"].apply(_fmt_time)
    display_df["duration"]        = display_df["duration"].apply(
        lambda x: f"{x:.1f}s" if pd.notna(x) else "—"
    )
    display_df["assigned_picker"] = display_df["assigned_picker"].apply(
        lambda x: f"P{int(x)}" if pd.notna(x) else "—"
    )
    display_df.columns = ["Order ID", "Status", "Picker", "Duration",
                           "Start", "End", "Pause Reason"]
    st.dataframe(display_df.set_index("Order ID"), width='stretch')


# ─────────────────────────────────────────────
# RENDER : DANGER ZONE (DB Reset)
# ─────────────────────────────────────────────

def render_danger_zone():
    st.markdown(
        '<div style="background:#fff5f5;border:2px solid #fc8181;border-radius:10px;'
        'padding:16px 20px;">'
        '<h4 style="color:#c53030;margin:0 0 8px 0;">⚠️ Danger Zone</h4>'
        '<p style="color:#742a2a;font-size:13px;margin:0 0 12px 0;">'
        'Resetting the database is <strong>irreversible</strong>. '
        'All order history, restock events, inventory snapshots, and metrics '
        'will be permanently deleted.</p>',
        unsafe_allow_html=True,
    )

    # Two-step confirm: first click arms the confirm prompt
    if "db_reset_armed" not in st.session_state:
        st.session_state.db_reset_armed = False

    if not st.session_state.db_reset_armed:
        if st.button("🗑️ Reset Database", key="btn_reset_arm"):
            st.session_state.db_reset_armed = True
            _rerun()
    else:
        st.error(
            "**Are you absolutely sure?**  This will wipe every order, restock event, "
            "and inventory snapshot across all runs. This cannot be undone."
        )
        col_confirm, col_cancel = st.columns(2)
        with col_confirm:
            if st.button("✅ Yes, permanently delete everything", key="btn_reset_confirm"):
                try:
                    # Stop the generator first if it's running
                    if st.session_state.get("gen_running"):
                        st.session_state.generator.stop()
                        st.session_state.gen_running = False

                    db_reset_all()

                    # Rebuild session state from scratch
                    for key in list(st.session_state.keys()):
                        del st.session_state[key]

                    st.success("✅ Database reset successfully. Reloading…")
                    time.sleep(1.5)
                    _rerun()
                except Exception as exc:
                    st.error(f"Reset failed: {exc}")
        with col_cancel:
            if st.button("❌ Cancel", key="btn_reset_cancel"):
                st.session_state.db_reset_armed = False
                _rerun()

    st.markdown("</div>", unsafe_allow_html=True)



# ─────────────────────────────────────────────
# RENDER : SLOTTING OPTIMISATION TAB
# ─────────────────────────────────────────────

def _render_frequency_heatmap_from_grid(
    grid,
    freq,
    cooler_row_index,
    title="",
):
    """
    Render a frequency heatmap from a grid snapshot (list-of-rows of SKU names).

    Parameters
    ----------
    grid             : [[sku_name, ...], ...] — row-major, full grid
    freq             : {sku: pick_count} for the analysis window
    cooler_row_index : integer index of the cooler/frozen row
    title            : optional caption above the heatmap
    """
    if title:
        st.caption(title)

    max_freq     = max(freq.values()) if freq else 1
    skus_per_row = max(len(r) for r in grid) if grid else 1

    for r_idx, row_cells in enumerate(grid):
        is_cooler = (r_idx == cooler_row_index)
        label = "🧊 Cooler" if is_cooler else f"Row {r_idx}"
        hdr   = "#1e3a5f" if is_cooler else "#2c3e50"
        st.markdown(
            f'<div style="background:{hdr};color:white;padding:3px 8px;'
            f'border-radius:5px 5px 0 0;font-weight:600;font-size:12px;">{label}</div>',
            unsafe_allow_html=True,
        )
        cols = st.columns(skus_per_row)
        for c_idx, sku in enumerate(row_cells):
            with cols[c_idx]:
                f_count   = freq.get(sku, 0) if sku != "(empty)" else 0
                intensity = (f_count / max_freq) if max_freq and sku != "(empty)" else 0
                g_val     = int(255 - 165 * intensity)
                b_val     = int(255 - 200 * intensity)
                bg        = f"rgb(255,{g_val},{b_val})"
                # Golden Zone: cells whose travel cost from (0,0) is lowest
                is_golden = (r_idx + c_idx) <= 2 and not is_cooler
                border    = "2px solid #f39c12" if is_golden else "1px solid #dee2e6"
                golden_badge = (
                    '<div style="font-size:9px;color:#f39c12;font-weight:700">⭐ GOLDEN</div>'
                    if is_golden else ""
                )
                pick_label = ("×" + str(f_count)) if f_count else "—"
                st.markdown(
                    f'<div style="background:{bg};border:{border};border-radius:5px;'
                    f'padding:6px 4px;text-align:center;min-height:68px;">'
                    + golden_badge
                    + f'<div style="font-size:10px;font-weight:600;color:#2c3e50;'
                    f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{sku}</div>'
                    f'<div style="font-size:14px;font-weight:700;color:#e67e22;">{pick_label}</div>'
                    f'<div style="font-size:9px;color:#7f8c8d;">picks</div></div>',
                    unsafe_allow_html=True,
                )
        st.markdown("<div style='margin-bottom:4px'></div>", unsafe_allow_html=True)


def _render_live_frequency_heatmap(
    rows_snap,
    freq,
    skus_per_row,
    title="",
):
    """Live-layout heatmap using RowState objects (current-state preview)."""
    grid       = [[cell.name for cell in row.cells] for row in rows_snap]
    cooler_idx = next((row.index for row in rows_snap if row.is_cooler), len(rows_snap) - 1)
    _render_frequency_heatmap_from_grid(grid, freq, cooler_idx, title=title)


def render_slotting_tab(store: DarkStore):
    """Full Warehouse Slotting Optimisation UI panel."""

    st.subheader("🗂️ Warehouse Slotting Optimisation")
    st.caption(
        "Reorders SKU positions based on historical pick frequency. "
        "High-frequency SKUs move to the **Golden Zone** — the rows and columns "
        "with the lowest real travel cost from the picker dispatch point (Row 0, Col 0). "
        "Cooler SKUs are **never** relocated."
    )

    _divider()

    # ── Data availability check ───────────────────────────────────────────────
    n_completed = db_count_completed_orders()

    col_info, col_ctrl = st.columns([3, 2])
    with col_info:
        if n_completed < 10:
            st.warning(
                f"⚠️ Only **{n_completed}** completed order(s) found. "
                "At least **10** are needed before optimisation can run. "
                "Start the simulation and let it run for a while, then return here."
            )
            _divider()
            st.markdown("**📊 Current Layout — Pick Frequency**")
            with store.lock:
                rows_snap = copy.deepcopy(store.rows)
            _render_live_frequency_heatmap(rows_snap, {}, store.skus_per_row)
            return
        else:
            st.success(f"✅ **{n_completed}** completed orders available for analysis.")

    with col_ctrl:
        max_window  = 400 #min(n_completed, 400)
        default_win = min(200, max_window)
        lookback = st.slider(
            "Lookback Window (Orders)",
            min_value=10,
            max_value=max_window,
            value=default_win,
            step=10,
            key="slotting_lookback",
            help=(
                "Number of most-recent completed orders used to build the "
                "pick-frequency map. Larger windows give more stable rankings; "
                "smaller windows react faster to recent demand shifts."
            ),
        )
        run_btn = st.button(
            "🚀 Reorder Warehouse SKUs",
            width='stretch',
            key="btn_run_slotting",
            type="primary",
        )

    _divider()

    # ── Current layout frequency preview ─────────────────────────────────────
    st.markdown("**📊 Current Layout — Pick Frequency**")
    preview_freq = db_load_recent_skus(lookback)
    with store.lock:
        rows_snap = copy.deepcopy(store.rows)
    _render_live_frequency_heatmap(
        rows_snap, preview_freq, store.skus_per_row,
        title=f"Frequency over last {lookback} completed orders",
    )

    # ── Run optimisation ──────────────────────────────────────────────────────
    if run_btn:
        with st.spinner("Analysing pick history and computing new slot assignments…"):
            result = store.reorder_warehouse(lookback)
        if not result.get("ok"):
            st.error(f"Optimisation failed: {result.get('reason', 'unknown error')}")
        else:
            st.session_state.slotting_result = result
            # Recompute ETC for pending orders now that positions changed
            with store.lock:
                store._recompute_etc_for_pool(store.ranking_policy)
            gain = result["efficiency_gain"]
            if gain > 0:
                st.toast(f"✅ Reorder complete — {gain:.1f}% efficiency gain!", icon="🚀")
            else:
                st.toast("ℹ️ Reorder complete — layout was already near-optimal.", icon="📦")

    # ── Show last result if one exists ────────────────────────────────────────
    result = st.session_state.get("slotting_result")
    if result is None or not result.get("ok"):
        return

    _divider()

    gain         = result["efficiency_gain"]
    before_avg_s = result["before_avg_s"]
    after_avg_s  = result["after_avg_s"]
    n_analysed   = result["orders_analysed"]
    skus_moved   = result["skus_moved"]
    freq_map     = result["freq_map"]
    before_grid  = result["before_grid"]
    after_grid   = result["after_grid"]

    # ── Result banner ─────────────────────────────────────────────────────────
    if gain > 0:
        st.success(
            f"✅ Reorder complete on **{n_analysed}** orders.  "
            f"**{skus_moved}** SKU(s) relocated.  "
            f"Projected avg pick time: **{before_avg_s:.1f}s → {after_avg_s:.1f}s**  "
            f"(**{gain:.1f}% faster**)"
        )
    elif gain == 0.0:
        st.info(
            f"ℹ️ Reorder complete ({n_analysed} orders analysed, {skus_moved} SKU(s) moved). "
            "Pick time unchanged — layout was already near-optimal for this window."
        )
    else:
        st.warning(
            f"⚠️ Reorder complete ({n_analysed} orders). "
            f"Projected pick time changed: {before_avg_s:.1f}s → {after_avg_s:.1f}s. "
            "This can occur with very small or uniform frequency distributions."
        )

    # ── Metrics row ───────────────────────────────────────────────────────────
    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("📋 Orders Analysed",  n_analysed)
    mc2.metric("📍 SKUs Relocated",   skus_moved)
    mc3.metric("⏱ Avg Time Before",  f"{before_avg_s:.1f}s")
    mc4.metric("⏱ Avg Time After",   f"{after_avg_s:.1f}s")
    delta_str = f"{gain:+.1f}%"
    mc5.metric("⚡ Efficiency Gain",  delta_str, delta=delta_str)

    _divider()

    # ── Before & After heatmap ────────────────────────────────────────────────
    st.markdown("**🗺️ Before & After Layout — Pick Frequency Heatmap**")
    st.caption(
        "Cells outlined in ⭐ gold are the **Golden Zone** "
        "(lowest Manhattan travel cost from the picker dispatch point). "
        "Heat colour: white = never picked → deep orange = most-picked."
    )
    ba_col1, ba_col2 = st.columns(2)
    with ba_col1:
        st.markdown("**Before** *(previous layout)*")
        _render_frequency_heatmap_from_grid(before_grid, freq_map, store.cooler_row_index)
    with ba_col2:
        st.markdown("**After** *(optimised layout)*")
        _render_frequency_heatmap_from_grid(after_grid, freq_map, store.cooler_row_index)

    _divider()

    # ── SKU frequency breakdown table ─────────────────────────────────────────
    st.markdown("**📋 SKU Frequency Breakdown**")

    def _grid_to_pos(grid):
        pos = {}
        for r, row in enumerate(grid):
            for c, sku in enumerate(row):
                if sku and sku != "(empty)":
                    pos[sku] = (r, c)
        return pos

    before_pos_map = _grid_to_pos(before_grid)
    after_pos_map  = _grid_to_pos(after_grid)
    sw_row = store.timing["switch_row_seconds"]
    sw_col = store.timing["switch_sku_seconds"]

    freq_rows = []
    for sku, cnt in sorted(freq_map.items(), key=lambda x: x[1], reverse=True):
        bp = before_pos_map.get(sku)
        ap = after_pos_map.get(sku)
        b_score = round(bp[0] * sw_row + bp[1] * sw_col, 2) if bp else "—"
        a_score = round(ap[0] * sw_row + ap[1] * sw_col, 2) if ap else "—"
        is_cooler_sku = sku in store.special_skus
        moved = "🧊 Cooler (fixed)" if is_cooler_sku else ("✅ Yes" if bp != ap else "— No")
        freq_rows.append({
            "SKU":                     sku,
            "Pick Count":              cnt,
            "Travel Score Before (s)": b_score,
            "Travel Score After (s)":  a_score,
            "Moved?":                  moved,
        })
    if freq_rows:
        st.dataframe(
            pd.DataFrame(freq_rows).set_index("SKU"),
            width='stretch',
        )


# ─────────────────────────────────────────────
# STORE INITIALISATION
# ─────────────────────────────────────────────

def get_store() -> DarkStore:
    if "store" not in st.session_state:
        cfg = load_store_config()
        st.session_state.store       = DarkStore(cfg)   # restores from DB internally
        st.session_state.generator   = OrderGenerator(st.session_state.store)
        st.session_state.gen_running = False
        st.session_state.db_reset_armed = False
        # Load ranking policy into session state so threads can read it
        if "ranking_policy" not in st.session_state:
            st.session_state.ranking_policy = load_ranking_policy()
    return st.session_state.store


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    st.set_page_config(page_title="Dark Store Simulation", layout="wide", page_icon="🏪")
    st.markdown("""
    <style>
    .block-container { padding-top: 1rem; }
    [data-testid="metric-container"] { background:#f8f9fa; border-radius:8px; padding:8px; }
    </style>
    """, unsafe_allow_html=True)

    st.title("🏪 Dark Store Simulation Dashboard")

    store     = get_store()
    generator: OrderGenerator = st.session_state.generator
    # Keep session_state in sync with the authoritative store policy
    with store.lock:
        policy = store.ranking_policy
    st.session_state.ranking_policy = policy

    # ── System Load / Heuristic Mode Banner ──────────────────────────────────
    with store.lock:
        sys_mode      = store.system_mode
        pool_etc      = store.pool_etc_total
        pending_count = len(store.pending_pool)

    threshold = policy["backlog_threshold_seconds"]
    if sys_mode == "HEURISTIC":
        st.markdown(
            f'<div style="background:#fff4e6;border:2px solid #e67e22;border-radius:8px;'
            f'padding:10px 18px;margin-bottom:12px;display:flex;align-items:center;gap:16px;">'
            f'<span style="font-size:22px">⚡</span>'
            f'<div><strong style="color:#b7610a;font-size:15px">Optimised — Heuristic Ranking ACTIVE</strong>'
            f'<br><span style="color:#7b4a00;font-size:12px">'
            f'Pending pool ETC: <code>{pool_etc:.0f}s</code> ≥ threshold '
            f'<code>{threshold}s</code> — pool sorted by Lowest ETC First '
            f'({pending_count} order(s) waiting)</span></div></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div style="background:#f0fff4;border:2px solid #38a169;border-radius:8px;'
            f'padding:10px 18px;margin-bottom:12px;display:flex;align-items:center;gap:16px;">'
            f'<span style="font-size:22px">✅</span>'
            f'<div><strong style="color:#276749;font-size:15px">Normal — FCFS (First-Come, First-Served)</strong>'
            f'<br><span style="color:#2f6b42;font-size:12px">'
            f'Pending pool ETC: <code>{pool_etc:.0f}s</code> &lt; threshold '
            f'<code>{threshold}s</code> — serving in arrival order '
            f'({pending_count} order(s) waiting)</span></div></div>',
            unsafe_allow_html=True,
        )

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ Control Panel")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("▶️ Start", width='stretch',
                         disabled=st.session_state.gen_running):
                generator.start()
                st.session_state.gen_running = True
                _rerun()
        with c2:
            if st.button("⏹ Stop", width='stretch',
                         disabled=not st.session_state.gen_running):
                generator.stop()
                st.session_state.gen_running = False
                # Final DB flush on stop
                threading.Thread(target=db_save_inventory, args=(store,), daemon=True).start()
                threading.Thread(target=db_save_metrics,   args=(store,), daemon=True).start()
                _rerun()

        icon = "🟢" if st.session_state.gen_running else "🔴"
        st.markdown(
            f"**Status:** {icon} {'Running' if st.session_state.gen_running else 'Stopped'}"
        )

        _divider()
        st.markdown("**Config Summary**")
        cfg = load_store_config()
        st.markdown(f"- Rows: `{cfg['layout']['rows']}`")
        st.markdown(f"- SKUs/row: `{cfg['layout']['skus_per_row']}`")
        st.markdown(f"- Pickers: `{cfg['pickers']['initial_count']}`")
        st.markdown(f"- Restock threshold: `{cfg['restocking']['threshold']} units`")
        st.markdown(f"- Restock duration: `{cfg['restocking']['restock_duration_seconds']}s`")
        st.markdown(f"- 🧊 Cooler: `{', '.join(cfg['special_skus'])}`")

        _divider()
        refresh_rate = st.slider("Refresh rate (s)", 0.5, 5.0, 1.5, 0.5)

        _divider()
        st.markdown("**⚖️ Ranking Policy**")
        st.caption("Edit weights live. Reloads from `ranking_policy.json` on reset.")

        new_sku_t    = st.number_input("SKU Process Time (s)", 1.0, 60.0,
                                        float(policy["sku_process_time"]),    0.5, key="pol_sku")
        new_row_pen  = st.number_input("Row Change Penalty (s)", 0.0, 120.0,
                                        float(policy["row_change_penalty"]),  1.0, key="pol_row")
        new_restock  = st.number_input("Restock Multiplier (×)", 1.0, 10.0,
                                        float(policy["restock_delay_multiplier"]), 0.1, key="pol_rst")
        new_cooler   = st.number_input("Cooler Row Constant (s)", 0.0, 120.0,
                                        float(policy["cooler_row_constant"]),  1.0, key="pol_cool")
        new_thresh   = st.number_input("Backlog Threshold (s)", 30, 600,
                                        int(policy["backlog_threshold_seconds"]), 10, key="pol_thr")

        if st.button("Apply Policy Changes", width='stretch'):
            updated = {
                "sku_process_time":          new_sku_t,
                "row_change_penalty":        new_row_pen,
                "restock_delay_multiplier":  new_restock,
                "cooler_row_constant":       new_cooler,
                "backlog_threshold_seconds": int(new_thresh),
            }
            # Update session_state (UI reads this for widget defaults)
            st.session_state.ranking_policy.update(updated)
            # Update store (background threads read this — no st.session_state access)
            with store.lock:
                store.ranking_policy.update(updated)
            policy = st.session_state.ranking_policy
            st.success("Policy updated ✓")

        _divider()
        st.markdown(
            '<p style="font-size:11px;color:#999;margin-bottom:4px;">🗄️ DB: '
            f'<code>{DB_PATH}</code></p>',
            unsafe_allow_html=True,
        )

        # ── Slotting quick-launch (mirrors the full tab control) ──────────────
        _divider()
        st.markdown("**🗂️ Slotting Optimisation**")
        all_orders_sb = db_load_orders()
        n_done_sb     = sum(1 for o in all_orders_sb if o["status"] == "Completed")
        st.caption(f"{n_done_sb} completed orders in DB")

        if n_done_sb < 10:
            st.warning("Need ≥ 10 completed orders to optimise.")
        else:
            sb_lookback = st.slider(
                "Lookback Window (Orders)",
                min_value=10,
                max_value=min(n_done_sb, 400),
                value=min(100, min(n_done_sb, 400)),
                step=10,
                key="sb_slotting_lookback",
            )
            if st.button("🚀 Reorder Warehouse SKUs", width='stretch',
                         key="btn_sb_slotting"):
                with st.spinner("Optimising…"):
                    result = store.reorder_warehouse(sb_lookback)
                st.session_state.slotting_result = result
                with store.lock:
                    store._recompute_etc_for_pool(store.ranking_policy)
                gain = result.efficiency_gain_pct
                if gain > 0:
                    st.success(f"Done! Gain: **{gain:.1f}%**")
                else:
                    st.info("Done — layout already optimal.")

    # ── Global metrics ────────────────────────────────────────────────────────
    with store.lock:
        total_processed = store.total_processed
        total_pick_time = store.total_pick_time
        active_restock  = sum(1 for r in store.rows if r.is_restocking)
        pickers_picking = sum(1 for p in store.pickers.values() if p.status == "Picking")
        pickers_restock = sum(1 for p in store.pickers.values() if p.status == "Restocking")
        history_len     = len(store.order_history)
        failed_count    = len(store.failed_orders)

    avg_time = (total_pick_time / total_processed) if total_processed else 0.0

    m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
    m1.metric("📋 Total Orders",    history_len)
    m2.metric("📦 Completed",       total_processed)
    m3.metric("⏱ Avg Pick Time",   f"{avg_time:.1f}s")
    m4.metric("🔄 Active Restocks", active_restock)
    m5.metric("🚶 Picking",        f"{pickers_picking}/{len(store.pickers)}")
    m6.metric("📦 Restocking",     f"{pickers_restock}/{len(store.pickers)}")
    m7.metric("❌ Failed",          failed_count)

    _divider()

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_live, tab_orders, tab_db, tab_slotting, tab_danger = st.tabs([
        "🏪 Live Simulation",
        "📋 Order Management",
        "🗄️ DB Analytics",
        "🗂️ Slotting Optimisation",
        "☠️ Danger Zone",
    ])

    with tab_live:
        left, right = st.columns([3, 2])
        with left:
            st.subheader("🗺️ Store Grid")
            render_grid(store)
        with right:
            st.subheader("📊 Inventory")
            render_inventory_table(store)
            _divider()
            render_picker_status(store)
            _divider()
            st.subheader("📦 Restock Log")
            render_restock_log(store)
            _divider()
            st.subheader("📋 Live Orders")
            render_live_orders(store)
            _divider()
            st.subheader("⏳ Pending Pool")
            render_pending_pool(store)

    with tab_orders:
        render_order_management(store)

    with tab_db:
        render_db_analytics()

    with tab_slotting:
        render_slotting_tab(store)

    with tab_danger:
        render_danger_zone()

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    if st.session_state.gen_running:
        time.sleep(refresh_rate)
        _rerun()
    else:
        st.caption("▶️ Press **Start** in the sidebar to begin the simulation.")


if __name__ == "__main__":
    main()