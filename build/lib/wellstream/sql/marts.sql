-- Analytics marts and the ML feature table. Re-runnable.

-- Section a reading belongs to, by depth range.
CREATE OR REPLACE VIEW v_reading_section AS
SELECT r.wellbore_sk, r.ts, s.section_sk, s.bit_size_in
FROM fact_drilling_reading r
JOIN dim_wellbore w ON w.wellbore_sk = r.wellbore_sk
JOIN dim_hole_section s ON s.wellbore_id = w.wellbore_id
     AND r.hole_depth_m >= s.top_md_m AND r.hole_depth_m <= s.base_md_m;

-- Daily drilling KPIs per wellbore.
CREATE OR REPLACE VIEW mart_daily_drilling AS
WITH r AS (
    SELECT r.*, LEAST(extract(epoch FROM lead(ts) OVER (PARTITION BY wellbore_sk ORDER BY ts) - ts), 60)
               AS dt_s  -- cap gaps at 60 s so logger outages do not count as activity
    FROM fact_drilling_reading r
)
SELECT w.wellbore_id, d.calendar_date,
       round((sum(r.dt_s) / 3600)::numeric, 2)                                      AS hours_logged,
       round((sum(r.dt_s) FILTER (WHERE rs.is_drilling) / 3600)::numeric, 2)        AS drilling_hours,
       round((max(r.hole_depth_m) - min(r.hole_depth_m))::numeric, 1)               AS meters_drilled,
       round(avg(r.rop_mh) FILTER (WHERE rs.is_drilling AND r.rop_mh > 0)::numeric, 1) AS avg_rop_mh,
       (SELECT count(*) FROM fact_drilling_event e
         WHERE e.wellbore_sk = r.wellbore_sk AND e.date_key = r.date_key AND e.severity >= 2) AS alerts
FROM r
JOIN dim_wellbore w ON w.wellbore_sk = r.wellbore_sk
JOIN dim_date d ON d.date_key = r.date_key
LEFT JOIN dim_rig_state rs ON rs.rig_state_code = r.rig_state_code
GROUP BY w.wellbore_id, d.calendar_date, r.wellbore_sk, r.date_key;

-- One-minute feature table for data scientists: aggregates plus event labels.
DROP MATERIALIZED VIEW IF EXISTS ml_features_1min;
CREATE MATERIALIZED VIEW ml_features_1min AS
WITH m AS (
    SELECT wellbore_sk, date_trunc('minute', ts) AS minute,
           count(*) AS n_samples,
           mode() WITHIN GROUP (ORDER BY rig_state_code) AS rig_state_code,
           max(hole_depth_m) AS hole_depth_m, avg(bit_depth_m) AS bit_depth_m,
           avg(spp_bar) AS spp_bar_mean, stddev_samp(spp_bar) AS spp_bar_std,
           avg(flow_lpm) AS flow_lpm_mean, avg(rpm) AS rpm_mean,
           avg(torque_knm) AS torque_knm_mean, stddev_samp(torque_knm) AS torque_knm_std,
           avg(wob_t) AS wob_t_mean, avg(hookload_t) AS hookload_t_mean, avg(rop_mh) AS rop_mh_mean,
           bool_or(cardinality(quality_flags) > 0) AS any_quality_flag
    FROM fact_drilling_reading
    GROUP BY 1, 2
)
SELECT w.wellbore_id, m.*,
       EXISTS (SELECT 1 FROM fact_drilling_event e WHERE e.wellbore_sk = m.wellbore_sk
               AND e.event_type = 'DOWNLINK' AND e.start_ts < m.minute + interval '1 minute'
               AND coalesce(e.end_ts, 'infinity') >= m.minute) AS label_downlink,
       EXISTS (SELECT 1 FROM fact_drilling_event e WHERE e.wellbore_sk = m.wellbore_sk
               AND e.event_type IN ('PRESSURE_LOSS', 'TORQUE_SPIKE') AND e.start_ts < m.minute + interval '1 minute'
               AND coalesce(e.end_ts, 'infinity') >= m.minute) AS label_drilling_alert,
       EXISTS (SELECT 1 FROM fact_drilling_event e WHERE e.wellbore_sk = m.wellbore_sk
               AND e.event_type = 'FROZEN_SENSOR' AND e.start_ts < m.minute + interval '1 minute'
               AND coalesce(e.end_ts, 'infinity') >= m.minute) AS label_sensor_fault
FROM m JOIN dim_wellbore w ON w.wellbore_sk = m.wellbore_sk;
CREATE UNIQUE INDEX ux_ml_features_1min ON ml_features_1min (wellbore_sk, minute);
COMMENT ON MATERIALIZED VIEW ml_features_1min IS 'Grain: one wellbore-minute. Channel aggregates plus event labels for model training. Refresh with: wellstream refresh-marts.';
