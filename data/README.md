# Data

Run `make data` (or `bash scripts/fetch_data.sh`) to download, with checksum verification:

| File | What |
|---|---|
| `F-1C_drilling.parquet` | ~191k samples (~3 s) of surface drilling channels, wellbore NO 15/9-F-1 C, 8½ in section, 22–28 Aug 2013 (Baker Hughes logger, rig activity code included) |
| `F-5_drilling.parquet` | ~59k samples (~5 s), wellbore NO 15/9-F-5 top-hole section, 17–20 Dec 2007 (SLB/Geoservices) |
| `F-1C_messages.json` | 255 timestamped driller / mud-logger remarks (WITSML `message` objects), used to check detected downlinks |

Source: Equinor's Volve data release, "WITSML Realtime drilling data", converted to a canonical channel
table by [DrillDash](https://github.com/Sky370/DrillDash) (MIT) and published with
[volve-drilling-anomaly](https://github.com/YamengGu/volve-drilling-anomaly). Values are SI as logged.

Volve data © Equinor and the Volve licence partners, released under the Equinor Open Data Licence:
https://www.equinor.com/energy/volve-data-sharing

`tests/fixtures/F-1C_excerpt.parquet` holds two short real slices of F-1 C (23 Aug 08:35–09:30 and
25 Aug 05:15–06:30 UTC) so the tests and CI run without downloading anything.
