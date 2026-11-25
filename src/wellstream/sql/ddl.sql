-- WellStream warehouse: physical model (Kimball star schema) on PostgreSQL, TimescaleDB optional.
-- Idempotent: safe to run more than once.

-- ---------------------------------------------------------------- dimensions
CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    day_of_month  SMALLINT NOT NULL,
    iso_weekday   SMALLINT NOT NULL
);
COMMENT ON TABLE dim_date IS 'Calendar dimension, one row per day (UTC).';
COMMENT ON COLUMN dim_date.date_key IS 'Surrogate key in YYYYMMDD form.';
COMMENT ON COLUMN dim_date.calendar_date IS 'The calendar day (UTC).';
COMMENT ON COLUMN dim_date.year IS 'Calendar year.';
COMMENT ON COLUMN dim_date.quarter IS 'Calendar quarter, 1-4.';
COMMENT ON COLUMN dim_date.month IS 'Month number, 1-12.';
COMMENT ON COLUMN dim_date.day_of_month IS 'Day of month, 1-31.';
COMMENT ON COLUMN dim_date.iso_weekday IS 'ISO weekday, 1 = Monday.';

INSERT INTO dim_date
SELECT to_char(d, 'YYYYMMDD')::int, d::date, extract(year FROM d), extract(quarter FROM d),
       extract(month FROM d), extract(day FROM d), extract(isodow FROM d)
FROM generate_series('2000-01-01'::date, '2035-12-31'::date, interval '1 day') AS d
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_rig_state (
    rig_state_code SMALLINT PRIMARY KEY,
    rig_state_name TEXT NOT NULL,
    is_drilling    BOOLEAN NOT NULL,
    is_productive  BOOLEAN NOT NULL
);
COMMENT ON TABLE dim_rig_state IS 'Rig activity codes (Baker Hughes RigActivityCode numbering, as mapped by DrillDash).';
COMMENT ON COLUMN dim_rig_state.rig_state_code IS 'Activity code 0-8 sent with every reading.';
COMMENT ON COLUMN dim_rig_state.rig_state_name IS 'Human-readable activity, e.g. Rotary drilling.';
COMMENT ON COLUMN dim_rig_state.is_drilling IS 'True when the bit is making new hole (rotary or slide).';
COMMENT ON COLUMN dim_rig_state.is_productive IS 'True for activities that are part of normal operations (not no-data).';

CREATE TABLE IF NOT EXISTS dim_wellbore (
    wellbore_sk   BIGSERIAL PRIMARY KEY,
    wellbore_id   TEXT NOT NULL,
    well_name     TEXT,
    field_name    TEXT,
    wellbore_type TEXT CHECK (wellbore_type IN ('MAIN', 'SIDETRACK')),
    operator      TEXT,
    status        TEXT,
    valid_from    TIMESTAMPTZ NOT NULL DEFAULT '1900-01-01',
    valid_to      TIMESTAMPTZ,
    is_current    BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_wellbore_current ON dim_wellbore (wellbore_id) WHERE is_current;
COMMENT ON TABLE dim_wellbore IS 'Wellbore dimension, slowly changing type 2: a new row per change of well/field/status attributes. Field and well are flattened in (star schema); they are separate entities in the logical model.';
COMMENT ON COLUMN dim_wellbore.wellbore_sk IS 'Surrogate key; facts point at the version current when the reading was loaded.';
COMMENT ON COLUMN dim_wellbore.wellbore_id IS 'Natural key: official wellbore name, e.g. NO 15/9-F-1 C. Kafka message key.';
COMMENT ON COLUMN dim_wellbore.well_name IS 'Parent well (surface location), e.g. NO 15/9-F-1.';
COMMENT ON COLUMN dim_wellbore.field_name IS 'Producing field, e.g. Volve.';
COMMENT ON COLUMN dim_wellbore.wellbore_type IS 'MAIN bore or SIDETRACK.';
COMMENT ON COLUMN dim_wellbore.operator IS 'Operating company.';
COMMENT ON COLUMN dim_wellbore.status IS 'Lifecycle status, e.g. DRILLING, COMPLETED.';
COMMENT ON COLUMN dim_wellbore.valid_from IS 'SCD2: start of validity of this version.';
COMMENT ON COLUMN dim_wellbore.valid_to IS 'SCD2: end of validity; null for the current version.';
COMMENT ON COLUMN dim_wellbore.is_current IS 'SCD2: true for the one current version per wellbore.';

-- SCD2 upsert: insert a new version only when tracked attributes change.
CREATE OR REPLACE FUNCTION upsert_wellbore(p_wellbore_id TEXT, p_well TEXT, p_field TEXT, p_type TEXT,
                                           p_operator TEXT, p_status TEXT, p_at TIMESTAMPTZ DEFAULT now())
RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE cur dim_wellbore%ROWTYPE; new_sk BIGINT;
BEGIN
    SELECT * INTO cur FROM dim_wellbore WHERE wellbore_id = p_wellbore_id AND is_current FOR UPDATE;
    IF FOUND AND cur.well_name IS NOT DISTINCT FROM p_well AND cur.field_name IS NOT DISTINCT FROM p_field
       AND cur.wellbore_type IS NOT DISTINCT FROM p_type AND cur.operator IS NOT DISTINCT FROM p_operator
       AND cur.status IS NOT DISTINCT FROM p_status THEN
        RETURN cur.wellbore_sk;
    END IF;
    IF FOUND THEN
        UPDATE dim_wellbore SET valid_to = p_at, is_current = FALSE WHERE wellbore_sk = cur.wellbore_sk;
    END IF;
    INSERT INTO dim_wellbore (wellbore_id, well_name, field_name, wellbore_type, operator, status, valid_from)
    VALUES (p_wellbore_id, p_well, p_field, p_type, p_operator, p_status,
            CASE WHEN FOUND THEN p_at ELSE '1900-01-01'::timestamptz END)
    RETURNING wellbore_sk INTO new_sk;
    RETURN new_sk;
END $$;

CREATE TABLE IF NOT EXISTS dim_hole_section (
    section_sk   BIGSERIAL PRIMARY KEY,
    wellbore_id  TEXT NOT NULL,
    bit_size_in  NUMERIC(5, 3) NOT NULL,
    top_md_m     DOUBLE PRECISION NOT NULL,
    base_md_m    DOUBLE PRECISION NOT NULL,
    UNIQUE (wellbore_id, bit_size_in),
    CHECK (base_md_m > top_md_m)
);
COMMENT ON TABLE dim_hole_section IS 'Hole sections: the depth interval drilled with one bit size. Joined to readings by depth range.';
COMMENT ON COLUMN dim_hole_section.section_sk IS 'Surrogate key.';
COMMENT ON COLUMN dim_hole_section.wellbore_id IS 'Natural key of the wellbore the section belongs to.';
COMMENT ON COLUMN dim_hole_section.bit_size_in IS 'Bit diameter, inches (8.5 = 8 1/2 in).';
COMMENT ON COLUMN dim_hole_section.top_md_m IS 'Measured depth where the section starts, m.';
COMMENT ON COLUMN dim_hole_section.base_md_m IS 'Measured depth where the section ends (section TD), m.';

-- --------------------------------------------------------------------- facts
CREATE TABLE IF NOT EXISTS fact_drilling_reading (
    wellbore_sk      BIGINT NOT NULL REFERENCES dim_wellbore (wellbore_sk),
    ts               TIMESTAMPTZ NOT NULL,
    date_key         INTEGER NOT NULL REFERENCES dim_date (date_key),
    rig_state_code   SMALLINT REFERENCES dim_rig_state (rig_state_code),
    hole_depth_m     DOUBLE PRECISION,
    bit_depth_m      DOUBLE PRECISION,
    hookload_t       DOUBLE PRECISION,
    wob_t            DOUBLE PRECISION,
    rpm              DOUBLE PRECISION,
    torque_knm       DOUBLE PRECISION,
    spp_bar          DOUBLE PRECISION,
    flow_lpm         DOUBLE PRECISION,
    rop_mh           DOUBLE PRECISION,
    mud_weight_kgm3  DOUBLE PRECISION,
    quality_flags    TEXT[] NOT NULL DEFAULT '{}',
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (wellbore_sk, ts)
);
COMMENT ON TABLE fact_drilling_reading IS 'Grain: one surface-logger sample per wellbore per timestamp (~3-5 s). Upserted on (wellbore_sk, ts), so Kafka replays never duplicate.';
COMMENT ON COLUMN fact_drilling_reading.wellbore_sk IS 'FK to dim_wellbore.';
COMMENT ON COLUMN fact_drilling_reading.ts IS 'Sample time, UTC.';
COMMENT ON COLUMN fact_drilling_reading.date_key IS 'FK to dim_date (UTC day of ts).';
COMMENT ON COLUMN fact_drilling_reading.rig_state_code IS 'FK to dim_rig_state.';
COMMENT ON COLUMN fact_drilling_reading.hole_depth_m IS 'Measured depth of the bottom of the hole, m.';
COMMENT ON COLUMN fact_drilling_reading.bit_depth_m IS 'Measured depth of the bit, m.';
COMMENT ON COLUMN fact_drilling_reading.hookload_t IS 'Hook load, tonnes-force.';
COMMENT ON COLUMN fact_drilling_reading.wob_t IS 'Weight on bit, tonnes-force.';
COMMENT ON COLUMN fact_drilling_reading.rpm IS 'Surface rotary speed, rev/min.';
COMMENT ON COLUMN fact_drilling_reading.torque_knm IS 'Surface torque, kN.m.';
COMMENT ON COLUMN fact_drilling_reading.spp_bar IS 'Standpipe pressure, bar.';
COMMENT ON COLUMN fact_drilling_reading.flow_lpm IS 'Mud flow in, L/min.';
COMMENT ON COLUMN fact_drilling_reading.rop_mh IS 'Rate of penetration, m/h.';
COMMENT ON COLUMN fact_drilling_reading.mud_weight_kgm3 IS 'Mud density in, kg/m3 (null when not reported).';
COMMENT ON COLUMN fact_drilling_reading.quality_flags IS 'Validator flags, e.g. wob_t_out_of_range (the value was nulled).';
COMMENT ON COLUMN fact_drilling_reading.loaded_at IS 'When the row was last written by the sink.';

CREATE TABLE IF NOT EXISTS fact_drilling_event (
    event_id         UUID PRIMARY KEY,
    wellbore_sk      BIGINT NOT NULL REFERENCES dim_wellbore (wellbore_sk),
    date_key         INTEGER NOT NULL REFERENCES dim_date (date_key),
    event_type       TEXT NOT NULL CHECK (event_type IN ('DOWNLINK', 'PRESSURE_LOSS', 'FROZEN_SENSOR', 'TORQUE_SPIKE')),
    status           TEXT NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    severity         SMALLINT NOT NULL CHECK (severity BETWEEN 1 AND 3),
    start_ts         TIMESTAMPTZ NOT NULL,
    end_ts           TIMESTAMPTZ,
    duration_s       DOUBLE PRECISION GENERATED ALWAYS AS (extract(epoch FROM end_ts - start_ts)) STORED,
    depth_m          DOUBLE PRECISION,
    detail           JSONB NOT NULL DEFAULT '{}',
    detector_version TEXT NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_event_wellbore_start ON fact_drilling_event (wellbore_sk, start_ts);
COMMENT ON TABLE fact_drilling_event IS 'Grain: one detected event (accumulating snapshot: inserted OPEN, updated when CLOSED).';
COMMENT ON COLUMN fact_drilling_event.event_id IS 'Deterministic UUIDv5 from the detector; the upsert key.';
COMMENT ON COLUMN fact_drilling_event.wellbore_sk IS 'FK to dim_wellbore.';
COMMENT ON COLUMN fact_drilling_event.date_key IS 'FK to dim_date (UTC day of start_ts).';
COMMENT ON COLUMN fact_drilling_event.event_type IS 'DOWNLINK, PRESSURE_LOSS, FROZEN_SENSOR or TORQUE_SPIKE.';
COMMENT ON COLUMN fact_drilling_event.status IS 'OPEN while ongoing, CLOSED once ended.';
COMMENT ON COLUMN fact_drilling_event.severity IS '1 info, 2 warning, 3 critical.';
COMMENT ON COLUMN fact_drilling_event.start_ts IS 'Event start, UTC.';
COMMENT ON COLUMN fact_drilling_event.end_ts IS 'Event end, UTC; null while open.';
COMMENT ON COLUMN fact_drilling_event.duration_s IS 'end_ts - start_ts in seconds (generated).';
COMMENT ON COLUMN fact_drilling_event.depth_m IS 'Bit depth at event start, m.';
COMMENT ON COLUMN fact_drilling_event.detail IS 'Event-specific values, e.g. channel, baseline_bar, max_drop_bar.';
COMMENT ON COLUMN fact_drilling_event.detector_version IS 'Detector release that produced the event (lineage).';
COMMENT ON COLUMN fact_drilling_event.loaded_at IS 'When the row was last written by the sink.';

-- Optional TimescaleDB: hypertable with 1-day chunks and compression after 7 days.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        PERFORM create_hypertable('fact_drilling_reading', 'ts', chunk_time_interval => INTERVAL '1 day',
                                  if_not_exists => TRUE, migrate_data => TRUE);
        ALTER TABLE fact_drilling_reading SET (timescaledb.compress, timescaledb.compress_segmentby = 'wellbore_sk');
        PERFORM add_compression_policy('fact_drilling_reading', INTERVAL '7 days', if_not_exists => TRUE);
    ELSE
        RAISE NOTICE 'timescaledb not available: fact_drilling_reading stays a plain table';
    END IF;
END $$;
