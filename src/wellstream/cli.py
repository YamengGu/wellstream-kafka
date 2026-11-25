"""Command-line entry point: `wellstream <command>`."""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
from pathlib import Path

from . import topics
from .config import Settings


def _validator_handler():
    from .validator import Validator
    v = Validator()

    def handle(rec):
        route, out = v.process(rec)
        yield (topics.CLEAN if route == "clean" else topics.DLQ), out["wellbore_id"], out
    return handle


def _detector_handler():
    from .detector import Detector
    d = Detector()

    def handle(rec):
        for ev in d.process(rec):
            yield topics.ALERTS, ev["wellbore_id"], ev
    return handle


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    s = Settings.from_env()
    p = argparse.ArgumentParser(prog="wellstream")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("create-topics", help="create Kafka topics")
    r = sub.add_parser("replay", help="publish a Volve wellbore log to the raw topic")
    r.add_argument("--well", default="F-1C", choices=["F-1C", "F-5"])
    r.add_argument("--speedup", type=float, default=0.0, help="0 = as fast as possible")
    r.add_argument("--limit", type=int, default=None, help="only the first N readings")
    for name in ("validate", "detect", "sink"):
        q = sub.add_parser(name, help=f"run the {name} service")
        q.add_argument("--max-idle", type=float, default=None, help="exit after N idle seconds")
    sub.add_parser("init-db", help="create warehouse tables, reference data and marts")
    sub.add_parser("refresh-marts", help="refresh the ML feature table")
    b = sub.add_parser("backfill", help="batch path: process a whole log without Kafka")
    b.add_argument("--well", default="F-1C", choices=["F-1C", "F-5"])
    b.add_argument("--load", action="store_true", help="also load the warehouse")
    b.add_argument("--report", default=None, help="write a JSON backtest report here")
    sub.add_parser("data-dictionary", help="print the warehouse data dictionary as Markdown")
    a = p.parse_args(argv)

    if a.cmd == "create-topics":
        print("created:", topics.create_topics(s.bootstrap) or "nothing (all exist)")
    elif a.cmd == "replay":
        from .streams import replay
        from .volve import WELLBORES, iter_raw_readings, load_log
        ref = WELLBORES[a.well]
        df = load_log(Path(s.data_dir) / ref["file"])
        if a.limit:
            df = df.head(a.limit)
        n = replay(s.bootstrap, s.schema_registry_url, iter_raw_readings(df, ref["wellbore_id"]), a.speedup)
        print(f"replayed {n} readings for {ref['wellbore_id']}")
    elif a.cmd in ("validate", "detect"):
        from .streams import run_transform
        host = socket.gethostname()
        if a.cmd == "validate":
            n = run_transform(s.bootstrap, s.schema_registry_url, topics.RAW, "wellstream-validator",
                              f"wellstream-validator-{host}", _validator_handler(), [topics.CLEAN, topics.DLQ],
                              max_idle_s=a.max_idle)
        else:
            n = run_transform(s.bootstrap, s.schema_registry_url, topics.CLEAN, "wellstream-detector",
                              f"wellstream-detector-{host}", _detector_handler(), [topics.ALERTS],
                              max_idle_s=a.max_idle)
        print(f"{a.cmd}: processed {n} records")
    elif a.cmd == "sink":
        from .streams import run_sink
        from .warehouse import Loader, connect
        n = run_sink(s.bootstrap, s.schema_registry_url, Loader(connect(s.pg_dsn)), max_idle_s=a.max_idle)
        print(f"sink: wrote {n} records")
    elif a.cmd == "init-db":
        from .warehouse import connect, init_db
        init_db(connect(s.pg_dsn))
        print("warehouse ready")
    elif a.cmd == "refresh-marts":
        from .warehouse import connect, refresh_marts
        refresh_marts(connect(s.pg_dsn))
        print("marts refreshed")
    elif a.cmd == "backfill":
        from .backfill import downlink_agreement, run_well, summarize
        sink = None
        if a.load:
            from .warehouse import Loader, connect
            sink = Loader(connect(s.pg_dsn)).write
        res = run_well(s.data_dir, a.well, sink=sink)
        rep = summarize(res)
        msgs = Path(s.data_dir) / f"{a.well}_messages.json"
        if msgs.exists():
            rep["downlinks_vs_driller_log"] = downlink_agreement(res["events"], msgs)
        text = json.dumps(rep, indent=2)
        if a.report:
            Path(a.report).write_text(text + "\n")
        print(text)
    elif a.cmd == "data-dictionary":
        from .warehouse import connect, data_dictionary
        print(data_dictionary(connect(s.pg_dsn)), end="")


if __name__ == "__main__":
    main()
