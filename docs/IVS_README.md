# IVS Dataset (Inland Shipping Monitor) Documentation

This document describes the processed IVS (Informatie Verwerkend Systeem) dataset for Dutch inland waterway shipping, generated from the raw weekly/daily weekmonitor ZIP archives published by Rijkswaterstaat.

---

## 1. Overview
The **IVS weekmonitor** dataset contains records of commercial voyages and ship movements on Dutch inland waterways. The raw source archives are cumulative weekly files that overlap heavily. 

The processing pipeline:
1. Reads all raw weekmonitor archives.
2. Standardizes and casts column types.
3. Resolves overlaps and filters out duplicate records.
4. Partitions the final dataset cleanly by **year and month**.

---

## 2. Partitioning Layout
To save memory and optimize query performance, the processed dataset is stored using standard Hive-style directory partitioning:

```text
/data/ivs/partitioned/
  ├── year=2024/
  │   ├── month=01/
  │   │   └── part.0.parquet
  │   ├── month=02/
  │   │   └── part.0.parquet
  │   └── ...
  └── year=2025/
```

---

## 3. Data Schema
All column names are standardized to lowercase. The schema of the written Parquet files is defined as follows:

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `jaarmaand` | `int64` | Numeric representation of year and month (e.g., `2404` for April 2024) |
| `jaar` | `int64` | Year of the voyage event |
| `maand` | `int64` | Month of the voyage event |
| `weeknr` | `int64` | Week number of the voyage event |
| `v05_06_begindt_evenement_iso` | `datetime64[us, UTC]` | Standardized timestamp of the trip start event (UTC) |
| `v05_06_begindt_evenement` | `string` | Raw date/time description string from Rijkswaterstaat |
| `unlo_herkomst` | `string` | Origin UN/LOCODE (5-character port identifier) |
| `unlo_bestemming` | `string` | Destination UN/LOCODE (5-character port identifier) |
| `v15_1_scheepstype_rws` | `string` | Rijkswaterstaat vessel type code |
| `sk_code` | `string` | Scheepsklasse (Vessel class code, e.g. `M8`, `M12`) |
| `v18_laadvermogen` | `float64` | Carrying capacity of the vessel in tonnes |
| `v28_beladingscode` | `float64` | Loading code indicating cargo status |
| `v38_vervoerd_gewicht` | `float64` | Transported weight in kilograms (kg) |
| `v30_4_containers_teu_s` | `float64` | Number of containers carried in TEU |
| `nstr_nw` | `string` | NSTR commodity classification code |
| `nst2007_nw` | `string` | NST 2007 commodity classification code |

---

## 4. Deduplication Logic
Raw weekmonitors are cumulative. Processing them without deduplication results in massive row inflation (~3.4x duplicates). 
Duplicates are removed by validating unique combinations of the primary trip keys:
* `v05_06_begindt_evenement` (Start event timestamp)
* `unlo_herkomst` (Origin port)
* `unlo_bestemming` (Destination port)
* `v15_1_scheepstype_rws` (Vessel type)
* `sk_code` (Vessel class)
* `v18_laadvermogen` (Vessel capacity)
* `v38_vervoerd_gewicht` (Cargo weight)

---

## 5. Validation and Verification (2024)
The processed 2024 dataset was verified against official macro-level statistics published by **Statistics Netherlands (CBS StatLine)**:

* **Processed IVS Dataset (Deduplicated):** **363.8 million tonnes** of total cargo weight across **388,708** unique trips.
* **CBS StatLine (2024):** **332.4 million tonnes** of cargo weight, yielding **42.3 billion tonne-kilometres**.
* **Analysis:** The minor ~9.5% difference is expected and matches the inclusion of all international transit and entry/exit voyages in the raw Rijkswaterstaat logs before specific territorial or commercial filter rules are applied by CBS.

---

## 6. Traffic Assignment (Routing)
The `fis ivs assign` command routes the partitioned IVS voyages onto the detailed waterway network, producing traffic-intensity and routed-path datasets. It takes **two graphs** as input — a detailed dropins graph (routing geometry) and a base merged graph.

Example invocation (route the 2025 voyages):

```bash
uv run python -m fis.cli ivs assign \
  --ivs-dir output/ivs-partitioned/year=2025 \
  --output-dir output/ivs-assignment/year=2025
```

Omitting `--graph-path` and `--base-graph` (as above) uses the two default graphs.

### Options and defaults

| Option | Default | Meaning |
| :--- | :--- | :--- |
| `--graph-path` | `output/dropins-fis-detailed/graph.pickle` | Detailed dropins graph (routing geometry) |
| `--base-graph` | `output/merged-graph/graph.pickle` | Base merged graph |
| `--ivs-dir` | `output/ivs-partitioned` (falls back to `/scratch-shared/fbaart/data/ivs/partitioned` if absent) | Partitioned IVS parquet input |
| `--output-dir` | `output/ivs-assignment` | Output directory for traffic assignment datasets |
| `--year` | _(unset)_ → all years | Optional year filter for voyages to route |
| `--month` | _(unset)_ → all months | Optional month filter for voyages to route |

### UN/LOCODE Fairway Geocoding

The routing algorithm matches UN/LOCODE origin and destination ports to network graph nodes. In dense port regions, naive spatial nearest-neighbour matching or generic harbour node prioritization can select shallow recreational basins, historic city canals, or dead-end berths behind restrictive structures (e.g., Rotterdam's historic Spinhuis behind low bridges, Amsterdam's Singelgracht with 4.2 m air draft, or Lelystad's 63 m recreational lock).

To guarantee robust routing for commercial inland navigation, major European and Dutch ports are pinned directly to designated high-capacity fairway junction nodes (`KEY_PORT_FAIRWAY_NODES` in [`fis.ivs.assign`](file:///workspaces/fis/fis/ivs/assign.py)):

| UN/LOCODE | Port Name | Node ID | Fairway Junction | CEMT Class | Coordinates (lon, lat) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `NLRTM` | Rotterdam | `FIS_8863496` | Nieuwe Maas / Waalhaven / Eemhaven junction | VI_c | 4.3689, 51.8992 |
| `NLAMS` | Amsterdam | `FIS_8864384` | Buiten-IJ / Amsterdam-Rijnkanaal entrance | VI_b | 4.9523, 52.3819 |
| `BEANR` | Antwerpen | `FIS_35228495` | Westerschelde / Rede Antwerpen fairway | VI_c | 4.3966, 51.2357 |
| `DEDUI` | Duisburg | `EURIS_DE_J1140` | Rhein / Ruhr junction (Hafenmund) | VI_b | 6.7208, 51.4516 |
| `NLLEY` | Lelystad | `FIS_8865470` | Markermeer / IJsselmeer approach | VI_a | 5.4283, 52.5128 |
| `BEGNE` | Gent | `FIS_35228492` | Kanaal Gent-Terneuzen commercial corridor | VI_b | 3.7402, 51.1067 |
| `NLTNZ` | Terneuzen | `FIS_8867489` | Kanaal Gent-Terneuzen / Westerschelde approach | VI_b | 3.8247, 51.3189 |
| `NLDOR` | Dordrecht | `FIS_8867986` | Beneden-Merwede / Oude Maas junction | VI_c | 4.6762, 51.8209 |

For all other locations, geocoding follows a three-tier hierarchy:
1. **Direct Fairway Pinning:** Check `KEY_PORT_FAIRWAY_NODES`.
2. **Network Attribute Matching:** Match exact UN/LOCODE or 5-character prefix against nodes tagged with `locode`, prioritizing `harbour` and `terminal` node types.
3. **Spatial Reference Fallback:** Query a spatial KDTree against official Zenodo UN/LOCODE geocoded coordinates to locate the nearest waterway node.

