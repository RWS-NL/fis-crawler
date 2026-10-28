# Stap 2: BIVAS-intensiteit projecteren op de FIS-routes

Zelfstandig uit te voeren opdracht. Resultaat: per FIS-vaarwegroute de verkeersintensiteit uit BIVAS, in bestanden die in stap 3 aan het ligplaatsaanbod (stap 1) worden gekoppeld. Overzicht van alle stappen: [README.md](README.md).

## 1. Wat je krijgt uit stap 1

Bestand `output/berth-analysis/route_berth_capacity.geoparquet`: één rij per FIS-vaarwegroute op het **Hoofdvaarwegennet** (49 routes met `network_type` Hoofdtransportas of Hoofdvaarweg, `is_hvwn`). Alleen deze routes worden uitgewerkt.

| Kolom | Betekenis |
|---|---|
| `route_id` (string, uniek) | Sleutel. Gebruik niet `route_code`: die is niet in alle FIS-routes uniek. |
| `route_code`, `route_name` | VIN-code en naam van de route (voor koppeling en controle) |
| `total_length_km` | Lengte van de route (km) |
| `geometry` | `LineString` in EPSG:4326 |

Daarnaast krijg je de **ruwe FIS-bestanden**, ongewijzigd uit de FIS-export (FIS-kolomnamen; gebruik alleen de routes uit `route_berth_capacity.geoparquet`), in de submap `output/berth-analysis/raw/`:

| Bestand | Rijen | Belangrijke kolommen |
|---|---|---|
| `route.geoparquet` | 2.068 routes (`LineString`, EPSG:4326) | `Id` (= `route_id`), `Code`, `VinCode`, `Name`, `RouteKmBegin`, `RouteKmEnd`, `geometry` |
| `routejunction.geoparquet` | 6.438 punten op routes (`Point`, EPSG:4326) | `RouteId` (= `route_id`), `RouteKmBegin`, `RouteKmEnd`, `SectionJunctionId`, `geometry` |
| `berth.geoparquet` | 7.883 ligplaatsen (`Point` of `Polygon`, EPSG:4326) | `Id`, `RouteId` (= `route_id`), `RouteKmBegin`, `RouteKmEnd`, `geometry` |
| `cardropoff.geoparquet` | 150 autoafzetplaatsen (`Point` of `Polygon`, EPSG:4326) | `Id`, `RouteId` (= `route_id`), `RouteKmBegin`, `RouteKmEnd`, `geometry` |

Gebruik ze om kilometers op de route te koppelen aan locaties: de routekilometrering (`RouteKmBegin`/`RouteKmEnd`) is het FIS-equivalent van `StartKilometer`/`EndKilometer` bij BIVAS (§ 3, stap 1).

BIVAS en de rest van het project werken in EPSG:28992: herproject voor ruimtelijke bewerkingen. In je uitvoer blijven `route_id` en de routegeometrie uit stap 1 **ongewijzigd** (zelfde CRS, niet vereenvoudigd).

## 2. Invoer vanuit BIVAS

- Netwerk: `reference/Bivas.5.10.1.sqlite`, `BranchSetId = 337`, `CountryCode = 'NL'` (9.023 arcs; alleen arcs op de 49 routes zijn relevant). Een arc heeft via `arc_vin_trajectory_connection` een `TrajectCode` (bv. `116a`) en `StartKilometer`/`EndKilometer`; lengte in `arcs.Length__m`.
- Rekenresultaat van het gekozen scenario: per reis de gevolgde arcs, datum/tijd, richting en scheepslengte. De repo-database bevat alleen herkomst en bestemming per reis (`trips`), geen belasting per arc; die moet uit een BIVAS-run komen. Leg scenario (`scenarios.ID`) en jaar vast.

## 3. Arcs koppelen aan routes

1. **Exacte code plus kilometeroverlap:** `route_code` = `TrajectCode` en overlap van het kilometerbereik (`fis.graph.bivas.has_km_overlap`, met omgekeerde kilometrering).
2. **Niet alleen `normalize_code`:** die verwijdert letters, waardoor Beerkanaal (`116a`) en Calandkanaal (`116`) dezelfde arcs krijgen (idem `101`/`101a`, `130`/`130e`, `225`/`225e`, `233`/`233y`). BIVAS-subcodes (`088a/b/c`) kunnen andere FIS-routes zijn.
3. **Ruimtelijke terugval** (50 m-buffer, zoals `scripts/bivas/compare_networks.py`) voor routes zonder codematch: nu 13 van de 49, vooral Zeeland (o.a. `147`, `149`, `160`, `161`, `180d`, `181`-`186`).
4. **Controle:** som `Length__m` van de gekoppelde arcs tegen `total_length_km`; ratio buiten ca. 0,8-1,2 handmatig beoordelen. Routes zonder BIVAS-arcs krijgen geen intensiteit (leeg, niet nul).

## 4. Te berekenen grootheden

Een reis die een route (deels) bevaart levert een **bijdrage** `f = bevaren afstand op de route / total_length_km` (0 < f <= 1; een reis over de hele route telt als 1).

| Grootheid | Definitie | Eenheid |
|---|---|---|
| `trips_yr` | Aantal reizen dat minstens een arc van de route gebruikt | reizen/jaar |
| `passages_eq_yr` | Som van f over alle reizen ("volledige-route-equivalenten") | passages/jaar |
| `ship_length_km_yr` | Som van f x scheepslengte (km) | km schip/jaar |
| `mean_ship_length_m` | `ship_length_km_yr * 1000 / passages_eq_yr` | m |
| `passages_eq_day_mean` | `passages_eq_yr / 365` | passages/dag |
| `passages_eq_day_p95` | 95e percentiel over de dagen van het jaar van de dagelijkse som van f | passages/dag |

Het 95e percentiel per dag is de maatgevende waarde in de Richtlijnen Vaarwegen 2020 (§ 6.4.2, p. 145-146: "maatgevend aantal passerende schepen per dag, het 95e percentiel"). Het volgt uit de dagverdeling (`trips.DateTime`).

Uitsplitsing: per scenario, per richting (`total`, `up`, `down`) en per scheepsklasse: `85m`, `110m`, `135m` (dichtstbijzijnde van 85, 110 en 135 m; grenzen 97,5 en 122,5 m) en `all`.

## 5. Uitvoer (`output/bivas-intensity/`)

| Bestand | Niveau / rijen | Kolommen |
|---|---|---|
| `route_intensity.geoparquet` | route x scenario x richting x scheepsklasse | `route_id`, `route_code`, `scenario_id`, `year`, `direction`, `ship_class`, alle grootheden uit § 4, `geometry` |
| `route_intensity_daily.parquet` | route x scenario x datum (tabel zonder geometrie) | `route_id`, `scenario_id`, `date`, `passages_eq`, `trips` |
| `arc_route_mapping.geoparquet` | BIVAS-arc | `arc_id`, `route_id` (leeg als niet gekoppeld), `trajectcode`, `match_method` (`code_km`, `spatial`, `none`), `km_overlap`, `length_m`, `geometry` |
| `route_coverage.geoparquet` | route | `route_id`, `n_arcs`, `bivas_length_km`, `fis_length_km`, `length_ratio`, aandeel per `match_method`, `geometry` |

Alle bestanden met een rij per route of arc zijn ruimtelijk. De routegeometrie en `route_id` komen ongewijzigd uit stap 1, zodat stap 3 op id kan koppelen met een ruimtelijke terugval.

## 6. Open punten

- Scenario en jaar van het BIVAS-rekenresultaat vaststellen en vastleggen.
- Het nachtaandeel (continuvaart) komt niet uit BIVAS (reizen hebben een vertrektijd, geen tijdstip per arc); dat is een parameter in stap 3.
