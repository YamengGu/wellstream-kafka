# WellStream: real-time drilling data on Kafka, landed in a formal data model

A streaming data platform for rig sensor data, built on **Apache Kafka** and a **formally modeled
warehouse** (conceptual → logical → physical), run end-to-end on Equinor's public **Volve** field data.

* **Real-time path:** a replay producer streams the rig's surface-logger channels into Kafka. A validator
  and an event detector process them with **exactly-once** transactions, and a sink loads a
  PostgreSQL/TimescaleDB star schema that Grafana reads.
* **Batch path:** the same validator and detector code backfills history straight into the warehouse
  and produces a backtest report.
* **Modeling:** documented conceptual, logical (3NF) and physical (Kimball star, SCD2) models, a
  [data dictionary](docs/data_dictionary.md) generated from the database catalog, and an
  [ML feature table](#for-data-scientists-ml_features_1min) with event labels.

## Results on real Volve data

Both wellbores, 250,482 readings, run through the batch path (`make backfill`):

| Wellbore | Readings | Events found | Checked against |
|---|---:|---|---|
| NO 15/9-F-1 C (8½ in section, Aug 2013) | 190,995 | 48 RSS downlinks, 1 frozen sensor | Driller's log: **44 of 48 downlinks (92%)** have a "DLK" entry within 15 min; 46 of 56 logged downlinks found |
| NO 15/9-F-5 (top-hole, Dec 2007) | 59,487 | 3 torque spikes | No driller's log for this well, so these 3 are unverified |

* **A real sensor fault, caught 2 minutes after onset.** On 25 Aug 2013 the standpipe-pressure channel
  repeated exactly 198.56 bar from 05:41:47 to 06:19:04 UTC while torque, hookload and flow kept
  reporting. That makes it the only non-routine event in F-1 C. The detector raised `FROZEN_SENSOR`
  at 05:43:48 and closed it when the signal came back.
* **Downlinks are not alarms.** Rotary-steerable tools are steered by coded pressure drops of 40–70 bar.
  Without recognizing them, every steering command would look like a washout. They are stored as
  severity-1 `DOWNLINK` events, so they are visible for analysis but don't page anyone.
* **Data quality is measured, not assumed.** The validator nulled and flagged 2,359 out-of-range WOB
  values and 958 "hole depth = 0" logger artefacts in F-1 C. Without the depth fix, day-one
  "meters drilled" read 2,604 m instead of 12 m.
* **A guard learned from F-5.** The first version raised 15 `PRESSURE_LOSS` alerts on F-5, where the
  driller often changes pump rate in top-hole drilling, and standpipe pressure follows flow. Pressure rules
  now only compare readings at the same flow rate (±5%) and relearn the baseline after a set-point change.
  All 15 disappeared, and F-1 C's downlink results did not change. A second rule now ignores channels
  stuck at zero, which had produced quantized near-zero torque "freezes" on both wells.

Full reports: [`reports/backtest_F-1C.json`](reports/backtest_F-1C.json),
[`reports/backtest_F-5.json`](reports/backtest_F-5.json). The detection approach builds on my
[volve-drilling-anomaly](https://github.com/YamengGu/volve-drilling-anomaly) study. It is reimplemented
here as a causal, one-reading-at-a-time stream processor.

## Data flow

```mermaid
flowchart LR
    subgraph sources[Source]
        V[(Volve WITSML logs<br/>parquet)]
    end
    subgraph kafka[Apache Kafka + Schema Registry - Avro]
        RAW[[drilling.sensor.raw.v1]]
        CLEAN[[drilling.sensor.clean.v1]]
        EVT[[drilling.event.v1]]
        DLQ[[drilling.sensor.dlq.v1]]
    end
    subgraph wh[PostgreSQL / TimescaleDB]
        F1[(fact_drilling_reading)]
        F2[(fact_drilling_event)]
        D[(dim_wellbore SCD2<br/>dim_date, dim_rig_state,<br/>dim_hole_section)]
        M[(mart_daily_drilling<br/>ml_features_1min)]
    end
    V -- replay producer<br/>idempotent --> RAW
    RAW -- validator<br/>exactly-once --> CLEAN
    RAW -. rejects .-> DLQ
    CLEAN -- detector<br/>exactly-once --> EVT
    CLEAN -- sink, upsert --> F1
    EVT -- sink, upsert --> F2
    F1 & F2 & D --> M
    M --> G[Grafana]
    M --> DS[Data scientists]
    V -- batch backfill<br/>same validator + detector --> F1
```

| Topic | Key | Partitions | Retention | Contents |
|---|---|---:|---|---|
| `drilling.sensor.raw.v1` | wellbore_id | 6 | 7 days | Readings as logged, SI units |
| `drilling.sensor.clean.v1` | wellbore_id | 6 | 30 days | Validated readings in field units, with quality flags |
| `drilling.event.v1` | wellbore_id | 3 | 90 days | Events: sent when they open and again when they close |
| `drilling.sensor.dlq.v1` | wellbore_id | 3 | 30 days | Rejected readings, with reason and original payload |

All topics are keyed by `wellbore_id`, so one wellbore's readings stay ordered on one partition. That is
what lets the detector keep per-wellbore state. Schemas live in
[`src/wellstream/schemas`](src/wellstream/schemas) and are registered with BACKWARD compatibility. New
channels are added as optional fields with defaults, so old consumers keep working.

## Data model

### Conceptual

A **field** has wells. A **well** (surface location) has one or more **wellbores** (main bore and
sidetracks). Each wellbore is drilled in **hole sections** (one bit size each) and produces a
stream of **drilling readings**, each tagged with a **rig activity**. From those readings come
**drilling events**.

### Logical (3NF)

```mermaid
erDiagram
    FIELD ||--o{ WELL : contains
    WELL ||--o{ WELLBORE : "drilled as"
    WELLBORE ||--o{ WELLBORE : "sidetrack of"
    WELLBORE ||--o{ HOLE_SECTION : "drilled in"
    WELLBORE ||--o{ DRILLING_READING : logs
    RIG_STATE ||--o{ DRILLING_READING : classifies
    WELLBORE ||--o{ DRILLING_EVENT : has
    FIELD { string field_name PK }
    WELL { string well_name PK
           string field_name FK }
    WELLBORE { string wellbore_id PK
               string well_name FK
               string parent_wellbore_id FK
               string wellbore_type
               string operator
               string status }
    HOLE_SECTION { string wellbore_id PK,FK
                   decimal bit_size_in PK
                   float top_md_m
                   float base_md_m }
    RIG_STATE { int rig_state_code PK
                string rig_state_name
                bool is_drilling }
    DRILLING_READING { string wellbore_id PK,FK
                       timestamp ts PK
                       int rig_state_code FK
                       float hole_depth_m
                       float bit_depth_m
                       float spp_bar
                       float flow_lpm
                       float torque_knm
                       float rpm
                       float wob_t
                       float hookload_t
                       float rop_mh }
    DRILLING_EVENT { uuid event_id PK
                     string wellbore_id FK
                     string event_type
                     int severity
                     timestamp start_ts
                     timestamp end_ts }
```

Each non-key attribute depends only on its entity's key. For example, the field belongs to the well, not
to the wellbore. That keeps the logical model in third normal form.

### Physical (Kimball star schema, [`ddl.sql`](src/wellstream/sql/ddl.sql))

| Table | Type | Grain / notes |
|---|---|---|
| `fact_drilling_reading` | fact (TimescaleDB hypertable, 1-day chunks, compressed after 7 days) | one sample per wellbore per timestamp; PK `(wellbore_sk, ts)` makes Kafka redelivery an idempotent upsert |
| `fact_drilling_event` | fact (accumulating snapshot) | one row per event: inserted OPEN, updated CLOSED. A late OPEN message can't reopen a closed event |
| `dim_wellbore` | dimension, **SCD type 2** | well and field flattened in for one-join queries; `upsert_wellbore()` versions changes |
| `dim_date`, `dim_rig_state`, `dim_hole_section` | dimensions | conformed across both facts; sections join by depth range |
| `mart_daily_drilling` | view | drilling hours, meters drilled, average ROP, alert count per wellbore-day |
| `ml_features_1min` | materialized view | one wellbore-minute: channel aggregates plus `label_*` event columns |

Every table and column has a `COMMENT ON` in the DDL, and
[`docs/data_dictionary.md`](docs/data_dictionary.md) is generated from them (`make dictionary`). The
dictionary can't drift from the database.

#### For data scientists: `ml_features_1min`

13,617 wellbore-minutes from the two Volve wells. Each row has the mean or standard deviation of SPP,
torque, flow, RPM, WOB, hookload and ROP, the dominant rig state, a data-quality flag, and three labels:
`label_downlink`, `label_drilling_alert` and `label_sensor_fault`.

```sql
SELECT * FROM ml_features_1min WHERE wellbore_id = 'NO 15/9-F-1 C' ORDER BY minute;
```

## Guarantees and design decisions

* **Exactly-once between topics.** The validator and detector use Kafka transactions: their outputs and
  their input offsets commit together (`send_offsets_to_transaction`). Downstream consumers read with
  `isolation.level=read_committed`. A crash mid-batch aborts the transaction, and the service restarts
  from the last commit.
* **Effectively-once into the warehouse.** The sink commits Kafka offsets only after the database
  commit. Every write is an upsert on a natural key, and event ids are deterministic UUIDv5s, so a
  replay re-sends the same ids. Re-running a full backfill leaves row counts unchanged (tested).
* **The same logic in batch and streaming.** The detector is causal and processes one reading at a time,
  so `wellstream backfill` and the Kafka service share one code path, and the backtest measures what
  production would do.
* **Explainable rules over a black box.** Every event carries the numbers that triggered it (`detail`
  JSON) and the `detector_version` that produced it.
* **Known limitations.** Detector state lives in memory per wellbore, so after a consumer-group rebalance
  it needs about 20 minutes of data to rebuild its pressure baseline. No formation-tops or production
  data is loaded yet. The F-5 torque alerts can't be verified without a driller's log.

## Run it

Requirements: Docker, and Python 3.10+ for the batch path and tests.

```bash
make data                 # download the Volve logs (checksummed)
make up                   # Kafka (KRaft) + Schema Registry + TimescaleDB + Grafana + 3 services
make replay               # stream F-1 C into Kafka at 50x real time
open http://localhost:3000   # Grafana → WellStream: drilling monitor
```

Batch path and tests, without Docker:

```bash
pip install -e '.[dev]'
export WAREHOUSE_DSN=postgresql://user:pass@localhost:5432/wellstream
wellstream init-db && make backfill && make dictionary
pytest -q                 # unit tests (real-data fixture included)
WELLSTREAM_TEST_PG_DSN=... pytest -q       # + warehouse tests (disposable database!)
WELLSTREAM_TEST_KAFKA=localhost:9092 pytest -m kafka   # + end-to-end on a real broker
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs lint, the unit and warehouse tests
against TimescaleDB, the end-to-end test against a real Kafka broker, and the Docker build.

## Layout

```
src/wellstream/
  volve.py        reads Volve logs into raw readings
  validator.py    stage 1: units, physical ranges, ordering, DLQ
  detector.py     stage 2: frozen sensors, downlinks, pressure loss, torque spikes
  streams.py      replay producer, exactly-once transform loop, warehouse sink
  backfill.py     batch path and backtest
  warehouse.py    DDL setup, reference data, idempotent loader, data dictionary
  schemas/        Avro schemas, one per topic
  sql/            ddl.sql (star schema), marts.sql (KPIs, ML features)
reference/        wellbores, rig states, hole sections (loaded into dimensions)
grafana/          provisioned datasource and dashboard
tests/            unit, warehouse and Kafka integration tests
```

## Data and licence

Volve data © Equinor and the Volve licence partners, released under the
[Equinor Open Data Licence](https://www.equinor.com/energy/volve-data-sharing). Sources and the file list
are in [`data/README.md`](data/README.md). Code: MIT.
