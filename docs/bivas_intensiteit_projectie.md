# Instructie: BIVAS-intensiteit projecteren op het ligplaatsennetwerk

Doel: de verkeersintensiteit uit BIVAS berekenen per **FIS-vaarwegroute (`route_id`)**, de eenheid waarop de ligplaatsbezetting is berekend, zodat vraag en aanbod per route vergeleken kunnen worden. Methodiek aanbod: [berth_spatial_capacity.md](berth_spatial_capacity.md).

## 1. Aanbod (ligplaatsen)

- Uitvoer: `output/berth-analysis/route_berth_capacity.csv` (49 routes op het Hoofdvaarwegnet) en `route_berth_capacity_all.geoparquet`.
- Per route: lokaal (`overnight_berth_length_m`) en corridor ≤ 15 km (`corridor_overnight_berth_length_m`, `corridor_overnight_capacity_ships_{85m,110m,135m}`). Toets op de corridor: bij 24 van de 49 routes is lokaal 0 m kade, omdat zijhavens een eigen `route_id` hebben.

## 2. BIVAS-arcs koppelen aan routes

- Netwerk: `reference/Bivas.5.10.1.sqlite`, `BranchSetId = 337`, `CountryCode = 'NL'` (9.023 arcs). Via `arc_vin_trajectory_connection` heeft een arc een `TrajectCode` (bv. `116a`) en `StartKilometer`/`EndKilometer`.
- **Koppel op exacte code plus kilometeroverlap** (`fis.graph.bivas.has_km_overlap`). Niet op `normalize_code` alleen: dat verwijdert letters, waardoor Beerkanaal (`116a`) en Calandkanaal (`116`) dezelfde arcs krijgen (idem `101`/`101a`, `130`/`130e`, `225`/`225e`, `233`/`233y`). BIVAS-subcodes (`088a/b/c`) kunnen andere FIS-routes zijn.
- 13 van de 49 routes (vooral Zeeland, o.a. `147`, `149`, `160`, `161`, `180d`, `181`–`186`) hebben geen codematch: terugval op ruimtelijke match (50 m-buffer, `scripts/bivas/compare_networks.py`) en apart rapporteren.
- Controle: som `Length__m` van de arcs tegen `total_length_km` van de route (ratio buiten ca. 0,8–1,2 handmatig beoordelen). Leg `arc_id → route_id` en de matchmethode vast.

## 3. Intensiteit per route

- De repo-database bevat alleen reizen met herkomst/bestemming (`trips`), geen belasting per arc. Die moet uit het BIVAS-rekenresultaat (route-toedeling) van het gekozen scenario komen; leg scenario en jaar vast.
- Per route, per richting en lengteklasse (85/110/135 m):
  - **Passages** (reizen/jaar): lengtegewogen gemiddelde van de arcbelasting, niet het maximum.
  - **Scheepskilometers** (km schip/jaar): `Σ reizen × Length__m/1000`, vergelijkbaar met `max_overnight_traffic_volume_km_yr`.
- Maatgevende waarde: het **95e percentiel van het aantal passerende schepen per dag**, uit de dagverdeling (`trips.DateTime`).

## 4. Norm en formule

Norm: **Richtlijnen Vaarwegen 2020, § 6.4.2** (p. 145–147), kansmethode voor gebieden met weinig ligplaatsen:

```
NB     = I · P_stop                       benodigd aantal overnachtingsplaatsen
P_stop = f_overnacht · (L / v) / T        aandeel passanten dat op het traject overnacht
```

Toets: aanbod ≥ `NB` plaatsen, of ≥ `LO = NB · (LP + s)` meter oeverlengte.

| Parameter | Waarde | Bron |
|---|---|---|
| `I` | 95e percentiel passerende schepen per dag | RVW 2020 § 6.4.2 |
| `L` | `total_length_km` van de route | FIS |
| `v` | 12 km/u (projectaanname; 2 uur ≈ 30 km, dus 15 km/u, volgt uit RVW § 6.3) | RVW 2020 § 6.3 |
| `T` | A1 = 14 u, A2 = 18 u, B = 24 u (continuvaart); code rekent met 14 u | Corporate uitvoeringskader ligplaatsen RWS 2011, p. 41 |
| `f_overnacht` | 1 − aandeel continuvaart (23.00–06.00 u): ca. 0,82 Waal 2012, 0,80 Duitsland; tankers klasse IV–VI 0,25 | Lobith 2013, p. 10 en 15 |
| `s` | 5 m (klasse I–IV), 10 m (V en hoger) | RVW 2020 § 6.4.2 |
| Schepen naast elkaar | kade twee breed, steiger drie breed | RVW 2020 § 6.4.2, p. 147 |
| Afmeerlengte | 1,2·L (twee breed of meer), 1,1·L (één breed) | RVW 2020 § 6.2.4 |

Het nachtaandeel kan niet direct uit BIVAS komen (reizen hebben een vertrektijd, geen tijdstip per arc): bepaal het uit IVS-passages of gebruik de tabelwaarden.

## 5. Bronnen

- **Norm:** Richtlijnen Vaarwegen 2020, 2e gewijzigde druk (RWS WVL, 20-11-2020), [open.rijkswaterstaat.nl](https://open.rijkswaterstaat.nl/@198385/richtlijnen-vaarwegen-2020-2e-gewijzigde). § 6.3–6.4 overnachtingsplaatsen en -havens, § 6.6 autoafzetplaats.
- **Referentie:** Achtergrondrapportage Vaarwegen, Integrale Mobiliteitsanalyse 2021 (RWS, v4.2, 10-5-2021), [Tweede Kamer 2021D26191](https://www.tweedekamer.nl/downloads/document?id=2021D26191). Toepassing van de RVW-normen; 95e percentiel in voetnoot 16, p. 57.
- **Details uit oudere studies:**
  - Corporate uitvoeringskader ligplaatsen RWS (DHV/DVS, 6-7-2011), [pdf](https://open.rijkswaterstaat.nl/publish/pages/153275/corporate_uitvoeringskader_ligplaatsen_rws.pdf): uitwerking van de RVW-methode en vaarregimes A1/A2/B.
  - Toekomstige Ligplaatsbehoefte Overnachtingshaven Lobith 2013 (RWS WVL, 2-10-2013), [pdf](https://gelderland.stateninformatie.nl/document/3474769/1/bijlage_38_ligplaatsbehoefte-onderzoek?connection_type=1&connection_id=2544786): definitie en waarden continuvaart (11% in 2005, 18% in 2012; Duitsland 20%).
  - Ligplaatsen Waal, een vooruitblik naar 2030 (V&W/AVV, 25-5-2007), [gearchiveerd pdf](https://archief28.sitearchief.nl/archives/sitearchief/20171226181910/https://www.rijksoverheid.nl/binaries/rijksoverheid/documenten/rapporten/2007/05/25/ligplaatsen-waal-een-vooruitblik-naar-2030/ligplaatsen-waal.pdf): oorspronkelijke bron van de continuvaartdefinitie; groei van overnachters evenredig met passanten (p. 32).

## 6. Open punten

- Snelheid `v` en regime (A1/A2/B) per vaarweg onderbouwen, bijvoorbeeld met `ship_speeds` in BIVAS of IVS-data.
- 95e percentiel per dag uit de BIVAS-dagverdeling bepalen.
- Zijhavencapaciteit kan dubbel tellen in naburige corridors (Voronoi-toewijzing, rapport § 7.2, is nog niet geïmplementeerd).
