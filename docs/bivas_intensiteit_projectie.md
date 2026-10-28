# Ligplaatsen: capaciteit, intensiteit en toetsing in drie stappen

De toetsing van ligplaatsen aan de verkeersintensiteit bestaat uit drie onafhankelijke stappen. Elke stap schrijft bestanden; de volgende stap leest alleen die bestanden. Alles wordt gekoppeld op **één sleutel: FIS-vaarwegroute `route_id` (string)**.

**Reikwijdte:** alleen het **Hoofdvaarwegennet**: de routes met `network_type` Hoofdtransportas of Hoofdvaarweg (`is_hvwn`, 49 routes). Alle uitspraken over koppelbaarheid, dekking en aantallen in dit document gaan over deze 49 routes; andere FIS-routes en BIVAS-arcs daarbuiten zijn buiten beschouwing.

| Stap | Doet | Invoer | Uitvoer |
|---|---|---|---|
| 1. Capaciteit | Ligplaatsaanbod per route berekenen | FIS-export (`berth`, `route`, …) | `output/berth-analysis/route_berth_capacity.geoparquet` (ruimtelijk, bestaat) |
| 2. Intensiteit | BIVAS-verkeer op dezelfde routes projecteren | BIVAS-database en -rekenresultaat, FIS-routes | `output/bivas-intensity/*` (hieronder gespecificeerd) |
| 3. Toetsing | Vraag (RVW-formule) tegen aanbod leggen | uitvoer stap 1 en 2 | eigen rapport |

## Stap 1: capaciteit (bestaand)

Methodiek: [berth_spatial_capacity.md](berth_spatial_capacity.md). Eén rij per route, de 49 HVWN-routes in `route_berth_capacity.geoparquet`. De uitvoer van stap 1 is een **ruimtelijk bestand** (GeoParquet, route-`LineString` in EPSG:4326); alleen dat bestand is de koppeling met stap 2 en 3. Een CSV (zonder geometrie) is geen invoer.

| Grootheid | Kolom | Eenheid | Niveau |
|---|---|---|---|
| Sleutel | `route_id` (str), `route_code`, `route_name`, `network_type`, `is_hvwn` | – | route |
| Lengte route `L` | `total_length_km` | km | route |
| Ligplaatsen lokaal | `overnight_berth_length_m`, `overnight_capacity_ships_{85m,110m,135m}` | m, schepen | route |
| Ligplaatsen corridor ≤ 15 km | `corridor_overnight_berth_length_m`, `corridor_overnight_capacity_ships_{85m,110m,135m}` | m, schepen | corridor rond de route |
| Kegelligplaatsen | `kegel_*`, `corridor_kegel_*` (idem) | m, schepen | route / corridor |

Details van de methode: [berth_spatial_capacity.md](berth_spatial_capacity.md) (§ 4 beschrijft deze koppeling vanuit stap 1). `route_id` is uniek (ook `route_code` is dat in de 49 HVWN-routes, maar `route_id` blijft de sleutel). De routegeometrie staat in EPSG:4326, BIVAS in EPSG:28992: herprojecteer voor de ruimtelijke koppeling.

Voor de toetsing in stap 3: de **aantallen schepen** (`*_capacity_ships_*`), de kadelengtes, `total_length_km` en `expected_overnight_stops` (= `P_stop,gecorrigeerd`, met `f_overnacht` = 0,82, 12 km/u en 14 u). Die parameters staan als kolommen in het bestand (`f_overnacht`, `sailing_speed_kmh`, `sailing_hours_per_day`), zodat stap 3 exact dezelfde waarden gebruikt als stap 1.

## Stap 2: intensiteit (te leveren)

### Invoer

- BIVAS-netwerk: `reference/Bivas.5.10.1.sqlite`, `BranchSetId = 337`, `CountryCode = 'NL'` (9.023 arcs in totaal, waarvan alleen de arcs op de 49 HVWN-routes relevant zijn; `arcs.Length__m`, `arc_vin_trajectory_connection` met `TrajectCode`, `StartKilometer`, `EndKilometer`).
- BIVAS-rekenresultaat van het gekozen scenario: per reis de gevolgde arcs, datum/tijd, richting en schip (lengte). De repo-database bevat alleen herkomst/bestemming per reis (`trips`), geen arcbelasting; het resultaat moet uit een BIVAS-run komen. Scenario (`scenarios.ID`) en jaar vastleggen.
- FIS-routes uit stap 1 (`route_id`, `route_code`, `total_length_km`, geometrie).

### Arcs koppelen aan routes

1. **Exacte code plus kilometeroverlap:** `route_code` = `TrajectCode` en overlap van het kilometerbereik (`fis.graph.bivas.has_km_overlap`, met omgekeerde kilometrering).
2. **Niet alleen `normalize_code`:** die verwijdert letters, waardoor Beerkanaal (`116a`) en Calandkanaal (`116`) dezelfde arcs krijgen (idem `101`/`101a`, `130`/`130e`, `225`/`225e`, `233`/`233y`). BIVAS-subcodes (`088a/b/c`) kunnen andere FIS-routes zijn.
3. **Ruimtelijke terugval** (50 m-buffer, zoals `scripts/bivas/compare_networks.py`) voor HVWN-routes zonder codematch: nu 13 van de 49, vooral Zeeland (o.a. `147`, `149`, `160`, `161`, `180d`, `181`–`186`).
4. **Controle:** som `Length__m` van de gekoppelde arcs tegen `total_length_km`; ratio buiten ca. 0,8–1,2 handmatig beoordelen.

### Grootheden

Een reis die een route (deels) bevaart levert een **bijdrage** `f = bevaren afstand op de route / total_length_km` (0 < f ≤ 1; een reis over de hele route telt als 1).

| Grootheid | Definitie | Eenheid |
|---|---|---|
| `trips_yr` | Aantal reizen dat minstens één arc van de route gebruikt | reizen/jaar |
| `passages_eq_yr` | Σ f over alle reizen ("volledige-route-equivalenten") | passages/jaar |
| `ship_length_km_yr` | Σ f · scheepslengte (km); vergelijkbaar met `max_overnight_traffic_volume_km_yr` uit stap 1 | km schip/jaar |
| `mean_ship_length_m` | `ship_length_km_yr · 1000 / passages_eq_yr` | m |
| `passages_eq_day_mean` | `passages_eq_yr / 365` | passages/dag |
| `passages_eq_day_p95` | 95e percentiel over de dagen van het jaar van de dagelijkse Σ f (maatgevend, RVW 2020 § 6.4.2) | passages/dag |

### Uitvoer (`output/bivas-intensity/`)

| Bestand | Niveau / rijen | Kolommen |
|---|---|---|
| `route_intensity.geoparquet` | route × scenario × richting × scheepsklasse | `route_id`, `route_code`, `scenario_id`, `year`, `direction` (`total`, `up`, `down`), `ship_class` (`all`, `85m`, `110m`, `135m`), alle grootheden uit de tabel hierboven |
| `route_intensity_daily.parquet` | route × scenario × datum | `route_id`, `scenario_id`, `date`, `passages_eq`, `trips` (voor controle van het 95e percentiel) |
| `arc_route_mapping.geoparquet` | BIVAS-arc | `arc_id`, `route_id` (leeg als niet gekoppeld), `trajectcode`, `match_method` (`code_km`, `spatial`, `none`), `km_overlap`, `length_m` |
| `route_coverage.geoparquet` | route | `route_id`, `n_arcs`, `bivas_length_km`, `fis_length_km`, `length_ratio`, aandeel per `match_method` |

Alle bestanden met een rij per route of arc zijn ruimtelijk (GeoParquet met de routegeometrie uit stap 1, EPSG:4326; `arc_route_mapping` met de arcgeometrie). Alleen `route_intensity_daily.parquet` is een tabel zonder geometrie (tijdreeks, koppeling via `route_id`).

**Behoud van geometrie en id:** stap 2 neemt de routegeometrie uit stap 1 ongewijzigd over (zelfde `LineString`, zelfde CRS, niet vereenvoudigd) en behoudt `route_id`. Zo kan stap 3 de uitvoer van stap 1 en 2 opnieuw koppelen, ook als de ene stap later opnieuw is gedraaid.

Scheepsklassen: schepen worden ingedeeld bij de dichtstbijzijnde van 85, 110 en 135 m (grenzen 97,5 en 122,5 m), gelijk aan de scenario's in stap 1; `all` bevat alle schepen. De sleutels `route_id` en de routes (inclusief `is_hvwn`) zijn identiek aan stap 1, zodat stap 3 een directe join op `route_id` kan doen.

## Stap 3: toetsing

### Koppelen van stap 1 en 2

1. **Primair op id:** join op `route_id` (string, exact).
2. **Terugval ruimtelijk** (voor routes waar de id niet matcht, bijvoorbeeld na een nieuwe FIS-export met andere ids): herproject beide bestanden naar EPSG:28992 (`PROJECTED_CRS`) en koppel de routegeometrieën. Gebruik een buffer van 50 m (zoals `scripts/bivas/compare_networks.py`) en wijs toe aan de route met de grootste overlappende lengte. Beoordeel afwijkende lengteverhoudingen handmatig.
3. Leg per route vast hoe gekoppeld is (`join_method`: `id` of `spatial`) en rapporteer routes zonder koppeling apart.


Norm: **Richtlijnen Vaarwegen 2020, § 6.4.2**. De RVW geeft voor gebieden met weinig ligplaatsen (p. 146):

```
NB                                    = aantal schepen · percentage schepen dat overnacht
percentage schepen dat overnacht      = gemiddelde reistijd / maximale vaartijd
gemiddelde reistijd                   = lengte vaarweg / gemiddelde snelheid
```

waarbij "aantal schepen" het maatgevend aantal passerende schepen per dag is (95e percentiel). Voor de Rijn gebruikt de RVW (p. 147) `Id` = "schepen per dag die overnachtingsplaatsen vereisen". Toegepast per route in notatie van de RVW:

```
P_stop       = (L / v) / T               percentage schepen dat overnacht (RVW)
f_overnacht  = P_stop,gecorrigeerd / P_stop = 1 - c
                                         correctiefactor op P_stop voor continuvaart (c = aandeel continuvaart)
P_stop,gecorrigeerd = f_overnacht * P_stop    (stap 1: kolom expected_overnight_stops)
Id           = I * P_stop,gecorrigeerd   schepen per dag die overnachtingsplaatsen vereisen (= NB)
```

`I` is het "aantal schepen" uit de RVW en `P_stop` het percentage uit de RVW (reistijd / maximale vaartijd). De factor `f_overnacht` is **geen onderdeel van de RVW**: het is een aanvulling uit het Corporate uitvoeringskader (p. 19–20, "rekening houden met continuvaart") met waarden uit het Lobith-onderzoek 2013. Stap 1 en stap 3 gebruiken dezelfde `f_overnacht` (0,82) en dezelfde `v` en `T`; stap 3 neemt ze uit de kolommen van stap 1.

Toets: aanbod (stap 1, corridor) ≥ `Id` plaatsen, of ≥ `LO = Id · (LP + s)` meter oeverlengte (RVW p. 146: `LO = NB · (LP + s) · PP/PB`, met `PP/PB` = 1 voor het basisjaar).

| Parameter | Waarde | Bron | Uit stap |
|---|---|---|---|
| `I` (RVW: "aantal schepen") | `passages_eq_day_p95` (95e percentiel passerende schepen per dag) | RVW 2020 § 6.4.2, p. 146 | 2 |
| `LP` | `mean_ship_length_m` | – | 2 |
| `L` | `total_length_km` | FIS | 1 |
| `P_stop,gecorrigeerd` | `expected_overnight_stops` | RVW 2020 § 6.4.2, p. 146, met `f_overnacht` | 1 |
| `v` | 12 km/u (projectaanname; 2 uur ≈ 30 km, dus 15 km/u, volgt uit RVW § 6.3) | RVW 2020 § 6.3 | 1 (`sailing_speed_kmh`) |
| `T` | A1 = 14 u, A2 = 18 u, B = 24 u (continuvaart); stap 1 gebruikt 14 u | Corporate uitvoeringskader ligplaatsen RWS 2011, p. 41 | 1 (`sailing_hours_per_day`) |
| `f_overnacht` = `1 − c` (aanvulling op de RVW) | `c` = aandeel scheepvaart tussen 23.00 en 06.00 uur (continuvaart): 0,18 Waal 2012, dus `f_overnacht` = 0,82; Duitsland `c` = 0,20, dus 0,80; tankers klasse IV–VI 0,25 | Lobith 2013, p. 10 en 15; Corporate uitvoeringskader p. 19–20 | 1 (`f_overnacht` = 0,82) |
| `s` | 5 m (klasse I–IV), 10 m (V en hoger) | RVW 2020 § 6.4.2 | parameter |
| Schepen naast elkaar | kade twee breed, steiger drie breed | RVW 2020 § 6.4.2, p. 147 | stap 1 |
| Afmeerlengte | 1,2·L (twee breed of meer), 1,1·L (één breed) | RVW 2020 § 6.2.4 | stap 1 |

`f_overnacht` (via `c`) kan niet uit BIVAS komen (reizen hebben een vertrektijd, geen tijdstip per arc): bepaal het uit IVS-passages of gebruik de tabelwaarden.

## Bronnen

- **Norm:** Richtlijnen Vaarwegen 2020, 2e gewijzigde druk (RWS WVL, 20-11-2020), [open.rijkswaterstaat.nl](https://open.rijkswaterstaat.nl/@198385/richtlijnen-vaarwegen-2020-2e-gewijzigde). § 6.3–6.4 overnachtingsplaatsen en -havens, § 6.6 autoafzetplaats.
- **Referentie:** Achtergrondrapportage Vaarwegen, Integrale Mobiliteitsanalyse 2021 (RWS, v4.2, 10-5-2021), [Tweede Kamer 2021D26191](https://www.tweedekamer.nl/downloads/document?id=2021D26191). Toepassing van de RVW-normen; 95e percentiel in voetnoot 16, p. 57.
- **Details uit oudere studies:**
  - Corporate uitvoeringskader ligplaatsen RWS (DHV/DVS, 6-7-2011), [pdf](https://open.rijkswaterstaat.nl/publish/pages/153275/corporate_uitvoeringskader_ligplaatsen_rws.pdf): uitwerking van de RVW-methode en vaarregimes A1/A2/B.
  - Toekomstige Ligplaatsbehoefte Overnachtingshaven Lobith 2013 (RWS WVL, 2-10-2013), [pdf](https://gelderland.stateninformatie.nl/document/3474769/1/bijlage_38_ligplaatsbehoefte-onderzoek?connection_type=1&connection_id=2544786): definitie en waarden continuvaart (11% in 2005, 18% in 2012; Duitsland 20%).
  - Ligplaatsen Waal, een vooruitblik naar 2030 (V&W/AVV, 25-5-2007), [gearchiveerd pdf](https://archief28.sitearchief.nl/archives/sitearchief/20171226181910/https://www.rijksoverheid.nl/binaries/rijksoverheid/documenten/rapporten/2007/05/25/ligplaatsen-waal-een-vooruitblik-naar-2030/ligplaatsen-waal.pdf): oorspronkelijke bron van de continuvaartdefinitie; groei van overnachters evenredig met passanten (p. 32).

## Open punten

- Snelheid `v` en regime (A1/A2/B) per vaarweg onderbouwen, bijvoorbeeld met `ship_speeds` in BIVAS of IVS-data.
- Zijhavencapaciteit kan dubbel tellen in naburige corridors: de capaciteit is per corridor (≤ 15 km), de intensiteit per route (Voronoi-toewijzing, rapport § 7.2, is nog niet geïmplementeerd).
- HVWN-routes zonder BIVAS-arcs (nu 13 van de 49) krijgen geen intensiteit; stap 3 moet die als "onbekend" tonen, niet als nul.
