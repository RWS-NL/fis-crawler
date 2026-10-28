# Ligplaatsen: toetsing aan verkeersintensiteit

De toetsing van het ligplaatsaanbod aan de verkeersintensiteit bestaat uit drie stappen die los van elkaar worden uitgevoerd. Elke stap schrijft bestanden; de volgende stap leest alleen die bestanden.

| Stap | Document | Doet | Uitvoer |
|---|---|---|---|
| 1. Capaciteit | [stap1_capaciteit.md](stap1_capaciteit.md) | Ligplaatsaanbod per route berekenen uit FIS | `output/berth-analysis/route_berth_capacity.geoparquet` |
| 2. Intensiteit | [stap2_intensiteit_bivas.md](stap2_intensiteit_bivas.md) | BIVAS-database projecteren op het FIS-routenetwerk (onafhankelijk van stap 1) | `output/bivas-intensity/*.geoparquet` |
| 3. Toetsing | [stap3_toetsing.md](stap3_toetsing.md) | Stap 1 en 2 koppelen en vraag tegen aanbod leggen | `output/berth-assessment/*.geoparquet` |

**Opgeleverde map** `output/berth-analysis/` (uitvoer van stap 1, wordt als geheel overgedragen):

```text
output/berth-analysis/
  route_berth_capacity.geoparquet     interface naar stap 2 en 3 (49 HVWN-routes)
  route_segments_compliance.geoparquet
  berths_analyzed.geoparquet
  berth_clusters.geoparquet
  berth_spatial_report.md / .html     rapport
  raw/                                ruwe FIS-bestanden (route, routejunction, berth, cardropoff, fairwayclassification) en schema.toml
  docs/                               deze documenten, als .md en gerenderd .html
```

**Afspraken die voor alle stappen gelden**

- **Reikwijdte:** alleen het Hoofdvaarwegennet: de 49 FIS-routes met `network_type` Hoofdtransportas of Hoofdvaarweg (`is_hvwn`).
- **Sleutel:** FIS-vaarwegroute `route_id` (string). Primair wordt op `route_id` gekoppeld, met een ruimtelijke terugval op de routegeometrie.
- **Formaat:** ruimtelijke bestanden (GeoParquet) met `route_id` en de routegeometrie ongewijzigd (EPSG:4326, niet vereenvoudigd). Een CSV is geen interface.
- **Parameters:** de rekenparameters (`f_overnacht`, `sailing_speed_kmh`, `sailing_hours_per_day`) staan als kolommen in het uitvoerbestand van stap 1; stap 3 gebruikt dezelfde waarden.
- **Literatuur:** norm is de Richtlijnen Vaarwegen 2020 (RVW 2020); de IMA 2021 is referentie; details komen uit oudere studies. Bronnen staan in [stap3_toetsing.md](stap3_toetsing.md#bronnen).
