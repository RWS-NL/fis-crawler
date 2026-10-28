# Stap 3: ligplaatsaanbod toetsen aan verkeersintensiteit

Voegt de uitvoer van stap 1 ([stap1_capaciteit.md](stap1_capaciteit.md)) en stap 2 ([stap2_intensiteit_bivas.md](stap2_intensiteit_bivas.md)) samen en legt vraag tegen aanbod. Overzicht van alle stappen: [README.md](README.md). Reikwijdte: de 49 routes van het Hoofdvaarwegennet (Hoofdtransportas en Hoofdvaarweg).

## 1. Invoer

| Bron | Bestand | Gebruikt |
|---|---|---|
| Stap 1 | `output/berth-analysis/route_berth_capacity.geoparquet` | `route_id`, `total_length_km`, `expected_overnight_stops`, `f_overnacht`, `sailing_speed_kmh`, `sailing_hours_per_day`, `overnight_*` en `corridor_overnight_*` (kadelengte, `*_capacity_ships_{85m,110m,135m}`), kegel-equivalenten, `geometry` |
| Stap 2 | `output/bivas-intensity/route_intensity.geoparquet` | `route_id`, `scenario_id`, `direction`, `ship_class`, `route_length_km`, `passages_eq_day_p95`, `mean_ship_length_m`, `passages_eq_yr`, `ship_length_km_yr`, `trips_yr`, `trips_length_km_yr`, `geometry` |

## 2. Koppelen van stap 1 en 2

1. **Primair op id:** join op `route_id` (string, exact).
2. **Terugval ruimtelijk** (routes waar de id niet matcht, bijvoorbeeld na een nieuwe FIS-export met andere ids): herproject beide bestanden naar EPSG:28992 (`PROJECTED_CRS`) en koppel de routegeometrieen met een buffer van 50 m (zoals `scripts/bivas/compare_networks.py`); wijs toe aan de route met de grootste overlappende lengte. Beoordeel afwijkende lengteverhoudingen handmatig.
3. **Controle:** vergelijk `route_length_km` (stap 2, uit de routegeometrie) met `total_length_km` (stap 1, som van de routesegmenten); beoordeel afwijkende verhoudingen handmatig. Stap 3 beperkt zich tot de routes in `route_berth_capacity.geoparquet` (49 routes op het Hoofdvaarwegennet); stap 2 bevat alle routes met BIVAS-arcs.
4. Leg per route vast hoe gekoppeld is (`join_method`: `id` of `spatial`) en rapporteer routes zonder koppeling apart. Routes zonder BIVAS-data (nu 13 van de 49) krijgen "onbekend", niet nul.

## 3. Norm en formule

Norm: **Richtlijnen Vaarwegen 2020, § 6.4.2** (p. 145-147), kansmethode voor gebieden met weinig ligplaatsen (p. 146):

```text
NB                                = aantal schepen * percentage schepen dat overnacht
percentage schepen dat overnacht  = gemiddelde reistijd / maximale vaartijd
gemiddelde reistijd               = lengte vaarweg / gemiddelde snelheid
```

Hierin is "aantal schepen" het maatgevend aantal passerende schepen per dag (95e percentiel). Voor de Rijn gebruikt de RVW (p. 147) `Id`, "schepen per dag die overnachtingsplaatsen vereisen". Toegepast per route:

```text
P_stop              = (L / v) / T                  percentage schepen dat overnacht (RVW)
f_overnacht         = P_stop,gecorrigeerd / P_stop = 1 - c
P_stop,gecorrigeerd = f_overnacht * P_stop         (stap 1: kolom expected_overnight_stops)
Id                  = I * P_stop,gecorrigeerd      schepen per dag die overnachtingsplaatsen vereisen (= NB)
Toets:                C_gebied >= Id
Meters oeverlengte:   LO = Id * (LP + s)           (RVW p. 146: LO = NB * (LP + s) * PP/PB, PP/PB = 1 in het basisjaar)
```

`f_overnacht` is **geen onderdeel van de RVW**: het is een aanvulling voor continuvaart (c = aandeel continuvaart) uit het Corporate uitvoeringskader en het Lobith-onderzoek 2013. Stap 1 en 3 gebruiken dezelfde `f_overnacht`, `v` en `T`; stap 3 neemt ze uit de kolommen van stap 1.

| Parameter | Waarde | Bron | Uit stap |
|---|---|---|---|
| `I` (RVW: "aantal schepen") | `passages_eq_day_p95` voor richting `total` en scheepsklasse `all`: het 95e percentiel van de dagelijkse afstandsgewogen passages (zie stap 2, § 3); niet verder omrekenen | RVW 2020 § 6.4.2, p. 145-146 | 2 |
| `LP` | `mean_ship_length_m` | - | 2 |
| `L` | `total_length_km` | FIS | 1 |
| `P_stop,gecorrigeerd` | `expected_overnight_stops` | RVW 2020 § 6.4.2, p. 146, met `f_overnacht` | 1 |
| `v` | 12 km/u (projectaanname; 2 uur is circa 30 km, dus 15 km/u, volgt uit RVW § 6.3) | RVW 2020 § 6.3, p. 144 | 1 (`sailing_speed_kmh`) |
| `T` | A1 = 14 u, A2 = 18 u, B = 24 u (continuvaart); stap 1 gebruikt 14 u | Corporate uitvoeringskader ligplaatsen RWS 2011, p. 41 | 1 (`sailing_hours_per_day`) |
| `f_overnacht` = `1 - c` | c = aandeel scheepvaart tussen 23.00 en 06.00 uur: 0,18 Waal 2012, dus 0,82; Duitsland c = 0,20; tankers klasse IV-VI 0,25 | Lobith 2013, p. 10 en 15; Corporate uitvoeringskader p. 19-20 | 1 (`f_overnacht` = 0,82) |
| `s` | 5 m (klasse I-IV), 10 m (V en hoger) | RVW 2020 § 6.4.2, p. 146 | parameter |
| `C_gebied` | `corridor_overnight_capacity_ships_{85m,110m,135m}` (schepen) of `corridor_overnight_berth_length_m` (m) | stap 1 | 1 |

**Jaarvergelijking als controle:** de kolom `corridor_max_overnight_traffic_volume_km_yr` (stap 1, km schip per jaar) is het jaarequivalent van de capaciteit en is te vergelijken met `ship_length_km_yr` uit stap 2 (afstandsgewogen trips x scheepslengte, dezelfde eenheid). Dit gebruikt het jaargemiddelde in plaats van het 95e percentiel en is daarom een indicatie, geen RVW-toets.

Het nachtaandeel (continuvaart) kan niet uit BIVAS komen (reizen hebben een vertrektijd, geen tijdstip per arc): bepaal het uit IVS-passages of gebruik de tabelwaarden.

## 4. Uitvoer (`output/berth-assessment/`)

Voorstel: `route_assessment.geoparquet`, een rij per route x scenario x scheepsklasse, ruimtelijk (routegeometrie uit stap 1, `route_id` ongewijzigd), met `join_method`, `I`, `P_stop,gecorrigeerd`, `Id`, `C_gebied` (schepen en meters), de verhouding `Id / C_gebied` en een vlag `voldoet`.

## 5. Open punten

- Snelheid `v` en regime (A1/A2/B) per vaarweg onderbouwen, bijvoorbeeld met `ship_speeds` in BIVAS of IVS-data.
- `f_overnacht` = 0,82 is de waarde voor de Waal (2012); voor de overige hoofdvaarwegen noemt het Lobith-onderzoek een veel lager aandeel continuvaart (3% in 2005).
- Zijhavencapaciteit kan dubbel tellen in naburige corridors (Voronoi-toewijzing, rapport § 7.2, is niet geimplementeerd): de capaciteit is per corridor (<= 15 km), de intensiteit per route.
- Modelkeuzes in stap 1 die afwijken van de RVW: zie [stap1_capaciteit.md](stap1_capaciteit.md), § 2.3.

## Bronnen

- **Norm:** Richtlijnen Vaarwegen 2020, 2e gewijzigde druk (RWS WVL, 20-11-2020), [open.rijkswaterstaat.nl](https://open.rijkswaterstaat.nl/@198385/richtlijnen-vaarwegen-2020-2e-gewijzigde). § 6.3-6.4 overnachtingsplaatsen en -havens, § 6.6 autoafzetplaats.
- **Referentie:** Achtergrondrapportage Vaarwegen, Integrale Mobiliteitsanalyse 2021 (RWS, v4.2, 10-5-2021), [Tweede Kamer 2021D26191](https://www.tweedekamer.nl/downloads/document?id=2021D26191). Toepassing van de RVW-normen; 95e percentiel in voetnoot 16, p. 57.
- **Details uit oudere studies:**
  - Corporate uitvoeringskader ligplaatsen RWS (DHV/DVS, 6-7-2011), [pdf](https://open.rijkswaterstaat.nl/publish/pages/153275/corporate_uitvoeringskader_ligplaatsen_rws.pdf): uitwerking van de RVW-methode en vaarregimes A1/A2/B.
  - Toekomstige Ligplaatsbehoefte Overnachtingshaven Lobith 2013 (RWS WVL, 2-10-2013), [pdf](https://gelderland.stateninformatie.nl/document/3474769/1/bijlage_38_ligplaatsbehoefte-onderzoek?connection_type=1&connection_id=2544786): definitie en waarden continuvaart (11% in 2005, 18% in 2012; Duitsland 20%).
  - Ligplaatsen Waal, een vooruitblik naar 2030 (V&W/AVV, 25-5-2007), [gearchiveerd pdf](https://archief28.sitearchief.nl/archives/sitearchief/20171226181910/https://www.rijksoverheid.nl/binaries/rijksoverheid/documenten/rapporten/2007/05/25/ligplaatsen-waal-een-vooruitblik-naar-2030/ligplaatsen-waal.pdf): oorspronkelijke bron van de continuvaartdefinitie; groei van overnachters evenredig met passanten (p. 32).
