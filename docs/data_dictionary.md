# Data dictionary

Generated from the warehouse catalog (`wellstream data-dictionary`). Do not edit by hand: change the `COMMENT ON` statements in `src/wellstream/sql/ddl.sql`.


## `fact_drilling_event`

Grain: one detected event (accumulating snapshot: inserted OPEN, updated when CLOSED).

| Column | Type | Key | Nullable | Description |
| --- | --- | --- | --- | --- |
| `event_id` | uuid | PK | no | Deterministic UUIDv5 from the detector; the upsert key. |
| `wellbore_sk` | bigint | FK → `dim_wellbore` | no | FK to dim_wellbore. |
| `date_key` | integer | FK → `dim_date` | no | FK to dim_date (UTC day of start_ts). |
| `event_type` | text |  | no | DOWNLINK, PRESSURE_LOSS, FROZEN_SENSOR or TORQUE_SPIKE. |
| `status` | text |  | no | OPEN while ongoing, CLOSED once ended. |
| `severity` | smallint |  | no | 1 info, 2 warning, 3 critical. |
| `start_ts` | timestamp with time zone |  | no | Event start, UTC. |
| `end_ts` | timestamp with time zone |  | yes | Event end, UTC; null while open. |
| `duration_s` | double precision |  | yes | end_ts - start_ts in seconds (generated). |
| `depth_m` | double precision |  | yes | Bit depth at event start, m. |
| `detail` | jsonb |  | no | Event-specific values, e.g. channel, baseline_bar, max_drop_bar. |
| `detector_version` | text |  | no | Detector release that produced the event (lineage). |
| `loaded_at` | timestamp with time zone |  | no | When the row was last written by the sink. |

## `fact_drilling_reading`

Grain: one surface-logger sample per wellbore per timestamp (~3-5 s). Upserted on (wellbore_sk, ts), so Kafka replays never duplicate.

| Column | Type | Key | Nullable | Description |
| --- | --- | --- | --- | --- |
| `wellbore_sk` | bigint | PK, FK → `dim_wellbore` | no | FK to dim_wellbore. |
| `ts` | timestamp with time zone | PK | no | Sample time, UTC. |
| `date_key` | integer | FK → `dim_date` | no | FK to dim_date (UTC day of ts). |
| `rig_state_code` | smallint | FK → `dim_rig_state` | yes | FK to dim_rig_state. |
| `hole_depth_m` | double precision |  | yes | Measured depth of the bottom of the hole, m. |
| `bit_depth_m` | double precision |  | yes | Measured depth of the bit, m. |
| `hookload_t` | double precision |  | yes | Hook load, tonnes-force. |
| `wob_t` | double precision |  | yes | Weight on bit, tonnes-force. |
| `rpm` | double precision |  | yes | Surface rotary speed, rev/min. |
| `torque_knm` | double precision |  | yes | Surface torque, kN.m. |
| `spp_bar` | double precision |  | yes | Standpipe pressure, bar. |
| `flow_lpm` | double precision |  | yes | Mud flow in, L/min. |
| `rop_mh` | double precision |  | yes | Rate of penetration, m/h. |
| `mud_weight_kgm3` | double precision |  | yes | Mud density in, kg/m3 (null when not reported). |
| `quality_flags` | text[] |  | no | Validator flags, e.g. wob_t_out_of_range (the value was nulled). |
| `loaded_at` | timestamp with time zone |  | no | When the row was last written by the sink. |

## `dim_date`

Calendar dimension, one row per day (UTC).

| Column | Type | Key | Nullable | Description |
| --- | --- | --- | --- | --- |
| `date_key` | integer | PK | no | Surrogate key in YYYYMMDD form. |
| `calendar_date` | date |  | no | The calendar day (UTC). |
| `year` | smallint |  | no | Calendar year. |
| `quarter` | smallint |  | no | Calendar quarter, 1-4. |
| `month` | smallint |  | no | Month number, 1-12. |
| `day_of_month` | smallint |  | no | Day of month, 1-31. |
| `iso_weekday` | smallint |  | no | ISO weekday, 1 = Monday. |

## `dim_hole_section`

Hole sections: the depth interval drilled with one bit size. Joined to readings by depth range.

| Column | Type | Key | Nullable | Description |
| --- | --- | --- | --- | --- |
| `section_sk` | bigint | PK | no | Surrogate key. |
| `wellbore_id` | text |  | no | Natural key of the wellbore the section belongs to. |
| `bit_size_in` | numeric(5,3) |  | no | Bit diameter, inches (8.5 = 8 1/2 in). |
| `top_md_m` | double precision |  | no | Measured depth where the section starts, m. |
| `base_md_m` | double precision |  | no | Measured depth where the section ends (section TD), m. |

## `dim_rig_state`

Rig activity codes (Baker Hughes RigActivityCode numbering, as mapped by DrillDash).

| Column | Type | Key | Nullable | Description |
| --- | --- | --- | --- | --- |
| `rig_state_code` | smallint | PK | no | Activity code 0-8 sent with every reading. |
| `rig_state_name` | text |  | no | Human-readable activity, e.g. Rotary drilling. |
| `is_drilling` | boolean |  | no | True when the bit is making new hole (rotary or slide). |
| `is_productive` | boolean |  | no | True for activities that are part of normal operations (not no-data). |

## `dim_wellbore`

Wellbore dimension, slowly changing type 2: a new row per change of well/field/status attributes. Field and well are flattened in (star schema); they are separate entities in the logical model.

| Column | Type | Key | Nullable | Description |
| --- | --- | --- | --- | --- |
| `wellbore_sk` | bigint | PK | no | Surrogate key; facts point at the version current when the reading was loaded. |
| `wellbore_id` | text |  | no | Natural key: official wellbore name, e.g. NO 15/9-F-1 C. Kafka message key. |
| `well_name` | text |  | yes | Parent well (surface location), e.g. NO 15/9-F-1. |
| `field_name` | text |  | yes | Producing field, e.g. Volve. |
| `wellbore_type` | text |  | yes | MAIN bore or SIDETRACK. |
| `operator` | text |  | yes | Operating company. |
| `status` | text |  | yes | Lifecycle status, e.g. DRILLING, COMPLETED. |
| `valid_from` | timestamp with time zone |  | no | SCD2: start of validity of this version. |
| `valid_to` | timestamp with time zone |  | yes | SCD2: end of validity; null for the current version. |
| `is_current` | boolean |  | no | SCD2: true for the one current version per wellbore. |
