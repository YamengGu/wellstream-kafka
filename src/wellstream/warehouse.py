"""PostgreSQL warehouse: schema setup, reference data, and idempotent loaders."""
from __future__ import annotations

import csv
import datetime as dt
import json
from importlib import resources
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

READING_COLS = ["rig_state_code", "hole_depth_m", "bit_depth_m", "hookload_t", "wob_t", "rpm",
                "torque_knm", "spp_bar", "flow_lpm", "rop_mh", "mud_weight_kgm3"]


def connect(dsn: str):
    return psycopg2.connect(dsn)


def _sql(name: str) -> str:
    return resources.files("wellstream.sql").joinpath(name).read_text()


def _ts(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc)


def _date_key(ms: int) -> int:
    d = _ts(ms)
    return d.year * 10000 + d.month * 100 + d.day


def init_db(conn, reference_dir: str | Path = "reference") -> None:
    """Create tables, load reference data, create marts."""
    ref = Path(reference_dir)
    with conn, conn.cursor() as cur:
        cur.execute(_sql("ddl.sql"))
        with open(ref / "rig_states.csv") as f:
            rows = [(int(r["rig_state_code"]), r["rig_state_name"], r["is_drilling"] == "true",
                     r["is_productive"] == "true") for r in csv.DictReader(f)]
        execute_values(cur, """INSERT INTO dim_rig_state VALUES %s ON CONFLICT (rig_state_code) DO UPDATE
            SET rig_state_name = EXCLUDED.rig_state_name, is_drilling = EXCLUDED.is_drilling,
                is_productive = EXCLUDED.is_productive""", rows)
        with open(ref / "wellbores.csv") as f:
            for r in csv.DictReader(f):
                cur.execute("SELECT upsert_wellbore(%s, %s, %s, %s, %s, %s)",
                            (r["wellbore_id"], r["well_name"], r["field_name"], r["wellbore_type"],
                             r["operator"], r["status"] or None))
        with open(ref / "hole_sections.csv") as f:
            rows = [(r["wellbore_id"], float(r["bit_size_in"]), float(r["top_md_m"]), float(r["base_md_m"]))
                    for r in csv.DictReader(f)]
        execute_values(cur, """INSERT INTO dim_hole_section (wellbore_id, bit_size_in, top_md_m, base_md_m)
            VALUES %s ON CONFLICT (wellbore_id, bit_size_in) DO UPDATE
            SET top_md_m = EXCLUDED.top_md_m, base_md_m = EXCLUDED.base_md_m""", rows)
        cur.execute(_sql("marts.sql"))


def refresh_marts(conn) -> None:
    with conn, conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW ml_features_1min")


class Loader:
    """Writes readings and events. Upserts make every write safe to repeat (Kafka redelivery, replays)."""

    def __init__(self, conn):
        self.conn = conn
        self._sk: dict[str, int] = {}

    def wellbore_sk(self, cur, wellbore_id: str) -> int:
        if wellbore_id not in self._sk:
            cur.execute("SELECT wellbore_sk FROM dim_wellbore WHERE wellbore_id = %s AND is_current",
                        (wellbore_id,))
            row = cur.fetchone()
            if row is None:  # unknown wellbore: create a bare dimension row (late-arriving dimension)
                cur.execute("SELECT upsert_wellbore(%s, NULL, NULL, NULL, NULL, NULL)", (wellbore_id,))
                row = cur.fetchone()
            self._sk[wellbore_id] = row[0]
        return self._sk[wellbore_id]

    def write(self, readings: list[dict], events: list[dict]) -> None:
        with self.conn, self.conn.cursor() as cur:
            if readings:
                rows = [(self.wellbore_sk(cur, r["wellbore_id"]), _ts(r["ts"]), _date_key(r["ts"]),
                         *[r.get(c) for c in READING_COLS], list(r.get("quality_flags") or []))
                        for r in readings]
                cols = ", ".join(READING_COLS)
                updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in [*READING_COLS, "quality_flags"])
                execute_values(cur, f"""INSERT INTO fact_drilling_reading
                    (wellbore_sk, ts, date_key, {cols}, quality_flags) VALUES %s
                    ON CONFLICT (wellbore_sk, ts) DO UPDATE SET {updates}, loaded_at = now()""",
                               rows, page_size=2000)
            if events:
                latest: dict[str, dict] = {}
                for e in events:  # keep the last state per event within the batch
                    latest[e["event_id"]] = e
                rows = [(e["event_id"], self.wellbore_sk(cur, e["wellbore_id"]), _date_key(e["start_ts"]),
                         e["event_type"], e["status"], e["severity"], _ts(e["start_ts"]),
                         None if e.get("end_ts") is None else _ts(e["end_ts"]), e.get("depth_m"),
                         json.dumps(json.loads(e.get("detail") or "{}")), e["detector_version"])
                        for e in latest.values()]
                execute_values(cur, """INSERT INTO fact_drilling_event
                    (event_id, wellbore_sk, date_key, event_type, status, severity, start_ts, end_ts,
                     depth_m, detail, detector_version) VALUES %s
                    ON CONFLICT (event_id) DO UPDATE SET status = EXCLUDED.status,
                        severity = EXCLUDED.severity, end_ts = EXCLUDED.end_ts, detail = EXCLUDED.detail,
                        detector_version = EXCLUDED.detector_version, loaded_at = now()
                    WHERE fact_drilling_event.status <> 'CLOSED' OR EXCLUDED.status = 'CLOSED'""", rows)


def data_dictionary(conn) -> str:
    """Render the warehouse data dictionary (from COMMENT ON metadata) as Markdown."""
    q = """
    SELECT c.relname, obj_description(c.oid, 'pg_class'), a.attname,
           format_type(a.atttypid, a.atttypmod), NOT a.attnotnull, col_description(c.oid, a.attnum),
           EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary
                   AND a.attnum = ANY(i.indkey)) AS is_pk,
           (SELECT cf.relname FROM pg_constraint k JOIN pg_class cf ON cf.oid = k.confrelid
             WHERE k.conrelid = c.oid AND k.contype = 'f' AND a.attnum = ANY(k.conkey) LIMIT 1) AS fk
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND (c.relname LIKE 'dim\\_%' OR c.relname LIKE 'fact\\_%')
    ORDER BY c.relname LIKE 'fact%' DESC, c.relname, a.attnum"""
    with conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()
    lines = ["# Data dictionary", "",
             "Generated from the warehouse catalog (`wellstream data-dictionary`). "
             "Do not edit by hand: change the `COMMENT ON` statements in `src/wellstream/sql/ddl.sql`.", ""]
    table = None
    for rel, tdesc, col, typ, nullable, cdesc, pk, fk in rows:
        if rel != table:
            table = rel
            lines += ["", f"## `{rel}`", "", tdesc or "", "",
                      "| Column | Type | Key | Nullable | Description |", "| --- | --- | --- | --- | --- |"]
        key = "PK" if pk else ""
        if fk:
            key = f"{key}, FK → `{fk}`".lstrip(", ")
        lines.append(f"| `{col}` | {typ} | {key} | {'yes' if nullable else 'no'} | {cdesc or ''} |")
    return "\n".join(lines) + "\n"
