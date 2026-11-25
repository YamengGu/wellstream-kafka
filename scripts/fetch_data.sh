#!/usr/bin/env bash
# Download the Equinor Volve drilling logs used by WellStream into ./data, verifying checksums.
# Files are the canonical tables published with github.com/YamengGu/volve-drilling-anomaly
# (Volve WITSML real-time drilling data converted by DrillDash). Volve data (c) Equinor and the
# Volve licence partners, Equinor Open Data Licence: https://www.equinor.com/energy/volve-data-sharing
set -euo pipefail

COMMIT=1bc7f02aed6444681d76750c735e3f2d9ad31e39
BASE="https://raw.githubusercontent.com/YamengGu/volve-drilling-anomaly/${COMMIT}/data/raw"
DEST="${1:-data}"
mkdir -p "$DEST"

fetch() {
  local name=$1 sha=$2
  if [[ -f "$DEST/$name" ]] && echo "$sha  $DEST/$name" | sha256sum -c --quiet - 2>/dev/null; then
    echo "ok       $name"
    return
  fi
  curl -fsSL "$BASE/$name" -o "$DEST/$name.part"
  echo "$sha  $DEST/$name.part" | sha256sum -c --quiet -
  mv "$DEST/$name.part" "$DEST/$name"
  echo "fetched  $name"
}

fetch F-1C_drilling.parquet f38ac0f882355b6747cabea39f2539ac2cf9054c712f39646d60a1988d47db78
fetch F-5_drilling.parquet  f5f53a960c0874c1f9c30aeb4924264e9e720974df1a100a24052e80095a0815
fetch F-1C_messages.json    8ce9a3a9e805792bcf09f7a23446af9feeaf1864c5c493dcd7f770b88b2167db
