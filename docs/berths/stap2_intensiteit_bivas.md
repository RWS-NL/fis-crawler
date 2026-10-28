# Stap 2: BIVAS-database projecteren op het FIS-routenetwerk

Zelfstandig uit te voeren opdracht, onafhankelijk van de uitvoer van stap 1. Resultaat: de verkeersintensiteit uit BIVAS per FIS-vaarwegroute (`route_id`). In stap 3 wordt die intensiteit op `route_id` (met ruimtelijke terugval) aan het ligplaatsaanbod uit stap 1 gekoppeld en vergeleken. Overzicht van alle stappen: [README.md](README.md).

## 1. Invoer

**FIS-routenetwerk** (ruwe bestanden uit de FIS-export; FIS-kolomnamen, EPSG:4326). Ze staan in `output/fis-export/` en ongewijzigd ook in `output/berth-analysis/raw/`.

| Bestand | Rijen | Belangrijke kolommen |
|---|---|---|
| `route.geoparquet` | 2.068 routes (`LineString`) | `Id` (= `route_id`, gebruik als string), `Code` (VIN-code, bv. `116a`), `Name`, `RouteKmBegin`, `RouteKmEnd`, `geometry` |
| `routejunction.geoparquet` | 6.438 punten op routes (`Point`) | `RouteId` (= `route_id`), `RouteKmBegin`, `RouteKmEnd`, `SectionJunctionId`, `geometry` |

Gebruik `Id` van de route als sleutel; `Code` is niet in alle routes uniek. De routekilometrering (`RouteKmBegin`/`RouteKmEnd`) is het FIS-equivalent van `StartKilometer`/`EndKilometer` bij BIVAS. De routelengte `route_length_km` is de lengte van de routegeometrie in EPSG:28992 (BIVAS en de rest van het project werken in EPSG:28992: herproject voor ruimtelijke bewerkingen). In je uitvoer blijven `route_id` en de routegeometrie **ongewijzigd** (zelfde CRS, niet vereenvoudigd).

**BIVAS-database**

- Netwerk: een BIVAS-database (SQLite) met het Nederlandse netwerk. De versie ligt niet vast; de beschrijving is getest op een database van versie 5.10.1 (`reference/Bivas.5.10.1.sqlite`, `BranchSetId = 337`, `CountryCode = 'NL'`, 9.023 arcs). Geef het pad van de database en de te gebruiken `BranchSetId` als parameter mee en controleer bij een andere versie of de tabellen en kolommen hieronder bestaan. Een arc heeft via `arc_vin_trajectory_connection` een `TrajectCode` (bv. `116a`) en `StartKilometer`/`EndKilometer`; lengte in `arcs.Length__m`.
- Rekenresultaat van het gekozen scenario: per reis de gevolgde arcs, datum/tijd, richting en scheepslengte. De repo-database bevat alleen herkomst en bestemming per reis (`trips`), geen belasting per arc; die moet uit een BIVAS-run komen. Leg scenario (`scenarios.ID`) en jaar vast.

Reikwijdte: projecteer op alle routes waarvoor BIVAS-arcs bestaan. Het beperken tot het Hoofdvaarwegennet (49 routes) gebeurt in stap 3, bij het koppelen aan het ligplaatsaanbod.

## 2. BIVAS-arcs koppelen aan routes

1. **Exacte code plus kilometeroverlap:** `route.Code` = `TrajectCode` en overlap van het kilometerbereik van de arc met dat van de route (`fis.graph.bivas.has_km_overlap`, met omgekeerde kilometrering).
2. **Niet alleen `normalize_code`:** die verwijdert letters, waardoor Beerkanaal (`116a`) en Calandkanaal (`116`) dezelfde arcs krijgen (idem `101`/`101a`, `130`/`130e`, `225`/`225e`, `233`/`233y`). BIVAS-subcodes (`088a/b/c`) kunnen andere FIS-routes zijn.
3. **Ruimtelijke terugval** (50 m-buffer, zoals `scripts/bivas/compare_networks.py`) voor arcs zonder codematch. Op het Hoofdvaarwegennet gaat het om ca. 13 van de 49 routes zonder codematch, vooral Zeeland (o.a. `147`, `149`, `160`, `161`, `180d`, `181`-`186`).
4. **Controle:** som `Length__m` van de gekoppelde arcs tegen `route_length_km`; ratio buiten ca. 0,8-1,2 handmatig beoordelen. Routes zonder BIVAS-arcs krijgen geen intensiteit (leeg, niet nul).

## 3. Te berekenen grootheden

Per route, scenario, richting en scheepsklasse. Een reis `i` heeft een scheepslengte `L_i` (m, uit BIVAS) en bevaart een route (deels). Een reis die de route bevaart levert een **bijdrage** `f_i = bevaren afstand op de route / route_length_km` (0 < f_i <= 1; een reis over de hele route telt als 1).

| Grootheid | Definitie | Eenheid |
|---|---|---|
| `trips_yr` | Aantal reizen dat minstens een arc van de route gebruikt (afstand telt niet mee) | reizen/jaar |
| `trips_length_km_yr` | Som van `L_i / 1000` over die reizen: trips x scheepslengte, een reis telt volledig | km schip/jaar |
| `passages_eq_yr` | Som van `f_i` ("volledige-route-equivalenten": afstandsgewogen aantal passages) | passages/jaar |
| `ship_length_km_yr` | Som van `f_i x L_i / 1000`: afstandsgewogen trips x scheepslengte; vergelijkbaar met `max_overnight_traffic_volume_km_yr` uit stap 1 | km schip/jaar |
| `mean_ship_length_m` | `ship_length_km_yr * 1000 / passages_eq_yr` | m |
| `passages_eq_day_mean` | `passages_eq_yr / 365` | passages/dag |
| `passages_eq_day_p95` | 95e percentiel van de dagelijkse `passages_eq`, zie hieronder | passages/dag |

**Welke grootheid waarvoor** (gebruik in stap 3):

| Doel | Grootheid |
|---|---|
| Aantal schepen `I` in de RVW-formule voor het aantal overnachters | `passages_eq_day_p95` (afstandsgewogen, omdat het percentage dat overnacht evenredig is met de afgelegde afstand) |
| Gemiddelde scheepslengte `LP` voor de benodigde oeverlengte | `mean_ship_length_m` |
| Vergelijking met de kadecapaciteit in km schip per jaar | `ship_length_km_yr` |
| Rapporteren van het verkeer (onafhankelijk van de routelengte) | `trips_yr` en `trips_length_km_yr` |

### Het 95e percentiel per dag

De Richtlijnen Vaarwegen 2020 (§ 6.4.2, p. 145-146) rekenen met het **maatgevend aantal passerende schepen per dag, het 95e percentiel**: een drukke dag, niet de allerdrukste en niet het gemiddelde. Zo bepaal je het:

1. Bouw de **dagreeks** voor het gekozen jaar: voor elke kalenderdag `d` de som van `f_i` van de reizen op die dag (`passages_eq`), per route, scenario, richting en scheepsklasse. Neem **alle dagen** van het jaar mee, ook dagen zonder verkeer (waarde 0). De dag volgt uit `trips.DateTime` (of de datum in het BIVAS-rekenresultaat).
2. `passages_eq_day_p95` is de waarde die op 95% van de dagen niet wordt overschreden: het 95e percentiel van die 365 waarden (lineaire interpolatie, `numpy.percentile(waarden, 95)`). Dat is de waarde van ca. dag 347 van de gesorteerde reeks; de ca. 18 drukste dagen liggen erboven.
3. Bepaal het percentiel **op de dagtotalen**, niet door percentielen van andere uitsplitsingen op te tellen of te middelen. Een percentiel is niet additief: p95 voor richting `total` volgt uit de dagelijkse som van beide richtingen, niet uit `up` + `down`. Hetzelfde geldt per scheepsklasse en bij het aggregeren naar een andere route of periode. Voor de toetsing in stap 3 geldt richting `total` en scheepsklasse `all` (schepen overnachten ongeacht de richting); de overige uitsplitsingen zijn ter informatie.
4. Lever de dagreeks mee (`route_intensity_daily`, zie § 4), zodat het percentiel is na te rekenen.

Gebruik in stap 3: dit is `I` in `Id = I x P_stop,gecorrigeerd`. Het percentiel wordt dus niet verder omgerekend. Is er geen dagverdeling beschikbaar (bijvoorbeeld alleen een jaartotaal), lever dan alleen `passages_eq_day_mean` en meld dat het 95e percentiel ontbreekt; een piekfactor op het gemiddelde is geen onderdeel van de RVW en moet apart worden onderbouwd.

Uitsplitsing: per scenario, per richting (`total`, `up`, `down`) en per scheepsklasse: `85m`, `110m`, `135m` (dichtstbijzijnde van 85, 110 en 135 m; grenzen 97,5 en 122,5 m) en `all`.

## 4. Uitvoer (`output/bivas-intensity/`)

| Bestand | Niveau / rijen | Kolommen |
|---|---|---|
| `route_intensity.geoparquet` | route x scenario x richting x scheepsklasse | `route_id`, `route_code`, `route_length_km`, `scenario_id`, `year`, `direction`, `ship_class`, alle grootheden uit § 3, `geometry` |
| `route_intensity_daily.geoparquet` | route x scenario x datum x richting x scheepsklasse, alle dagen van het jaar incl. dagen met 0 | `route_id`, `scenario_id`, `date`, `direction`, `ship_class`, `trips`, `trips_length_km`, `passages_eq`, `ship_length_km`, `geometry` |
| `arc_route_mapping.geoparquet` | BIVAS-arc | `arc_id`, `route_id` (leeg als niet gekoppeld), `trajectcode`, `match_method` (`code_km`, `spatial`, `none`), `km_overlap`, `length_m`, `geometry` |
| `route_coverage.geoparquet` | route | `route_id`, `n_arcs`, `bivas_length_km`, `route_length_km`, `length_ratio`, aandeel per `match_method`, `geometry` |

Alle bestanden met een rij per route of arc zijn ruimtelijk. `route_id` en de routegeometrie komen ongewijzigd uit de FIS-routebestanden, zodat stap 3 op id kan koppelen met een ruimtelijke terugval op de geometrie.

## 5. Open punten

- Scenario en jaar van het BIVAS-rekenresultaat vaststellen en vastleggen.
- Het nachtaandeel (continuvaart) komt niet uit BIVAS (reizen hebben een vertrektijd, geen tijdstip per arc); dat is een parameter in stap 3.
