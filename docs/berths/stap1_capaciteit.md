# Ligplaatsen Ruimtelijke Capaciteits- en Netwerkdekkingsanalyse (`berth-analysis`)

Dit document beschrijft de rekenmethodiek, de gebruikte formules (conform **Richtlijnen Vaarwegen 2020**, § 6.3–6.6) en het volledige gegevensschema (alle velden) van de uitvoerbestanden die worden gegenereerd door [`analyze_spatial_capacity.py`](../scripts/berth_analysis/analyze_spatial_capacity.py) en [`route_network_graph.py`](../scripts/berth_analysis/route_network_graph.py).


> **Stap 1 van 3** (overzicht: [README.md](README.md)). Dit document beschrijft de **capaciteitsberekening** (aanbod per route) en is klaar om aan stap 2 ([stap2_intensiteit_bivas.md](stap2_intensiteit_bivas.md)) en stap 3 ([stap3_toetsing.md](stap3_toetsing.md)) over te dragen. De koppeling loopt uitsluitend via `route_berth_capacity.geoparquet` en de sleutel `route_id`, zie [§ 4](#4-oplevering-aan-stap-2-en-3-interface).

---

## 1. Overzicht en Uitvoerbestanden

De pijplijn voert een landelijke netwerkanalyse uit op de FIS-ligplaatsendata (`output/fis-export/berth.geoparquet`) gekoppeld aan de FIS-vaarwegroutes (`route.geoparquet`, `routejunction.geoparquet`, `cardropoff.geoparquet` en `fairwayclassification.geoparquet`).

Uitvoeren van de analyse:

```bash
uv run python -m scripts.berth_analysis.analyze_spatial_capacity \
  --export-dir output/fis-export \
  --output-dir output/berth-analysis
```

De toetsing beperkt zich tot het **Hoofdvaarwegennet**: Hoofdtransportassen en Hoofdvaarwegen (`is_hvwn`). Het script schrijft daarnaast nog `route_berth_capacity_all.geoparquet` en `route_segments_compliance_all.geoparquet` (alle routes); die worden nergens gebruikt en vervallen.

De analyse genereert de volgende bestanden in `output/berth-analysis/`:

| Bestand | Inhoud |
| :--- | :--- |
| `berths_analyzed.geoparquet` | Alle FIS-ligplaatsen verrijkt met classificatievlaggen, kegelstatus, rij-afleiding uit `width` en scheepscapaciteit (85m / 110m / 135m) |
| `berth_clusters.geojson` | Geclusterde overnachtingslocaties (`Delta km <= 0,5` km) met toetsing op aanwezigheid van een autoafzetplaats (RVW 2020 § 6.6) |
| `route_segments_compliance.geoparquet` (`.geojson`) | Ruimtelijke dekkingsanalyse per routetussenstuk op het **Hoofdvaarwegnet (HVWN)** (Hoofdtransportassen + Hoofdvaarwegen) |
| `route_berth_capacity.geoparquet` (`.csv`, `.geojson`) | Geaggregeerde ligplaatscapaciteit, corridorcapaciteit (≤ 15 km) en maximale jaarlijkse bedienbare verkeersintensiteit per **HVWN-route** |
| `berth_spatial_report.md` | Samenvattend rapport in Markdown met landelijke tabellen, knelpunten, autoafzetplaatstest en rekenvoorbeelden |

---

## 2. Formules en Rekenmethodiek (RVW 2020 § 6.3–6.6)

### 2.1 Classificatie van Overnachtingsligplaatsen (`is_overnight`)
Omdat de ruwe FIS-export het vlaggetje `OVERNIGHT_ACCOMODATION` in `facility` slechts bij een deel van de ligplaatsen invult, wordt een ligplaats geclassificeerd als overnachtingsligplaats (`is_overnight = True`) wanneer deze **niet is uitgesloten** (`not_excl`) én aan ten minste één van vier inclusiecriteria voldoet:

```text
not_excl = (maximum_stay_duration_h is NaN OF maximum_stay_duration_h >= 12,0)
           EN category niet in {WAITING_AREA, LOADING_AND_UNLOADING}
           EN NIET is_recreation
```

waarbij recreatie- en pleziervaartvoorzieningen (`is_recreation`) hard worden uitgesloten van de commerciële beroepsvaartcapaciteit:
* Ligplaatsen met `recreatie`, `pleziervaart`, `passanten` of `jacht` in de naam of toelichting (tenzij expliciet bestemd voor `duwvaart` of `beroepsvaart`);
* Ligplaatsen gelegen op routes van **jachtensluizen** of jachthavens (zoals *Ankerplaats 1 Krammer* in de westelijke voorhaven van de Krammer Jachtsluizen).

```text
is_overnight = ov_by_facility OF ov_by_stay OF ov_by_category OF ov_by_long_quay
```

waarbij (elk gecombineerd met `not_excl`):

1. **Expliciete overnachtingsfaciliteit (`ov_by_facility`)**: `"OVERNIGHT_ACCOMODATION" in facility EN not_excl`
2. **Verblijfsduur `>= 12` uur (`ov_by_stay`)**: `maximum_stay_duration_h >= 12,0 EN not_excl`
3. **Commerciële binnenvaartcategorie (`ov_by_category`)**: `category in {"ALL_VESSELS", "PUSH_TOW_VESSELS", "NON_PUSH_TOW_VESSELS"} EN not_excl`
4. **Algemene lange kade zonder verblijfsbeperking (`ov_by_long_quay`)**: `length >= 100,0 m EN category = "OTHER" EN maximum_stay_duration_h is NaN EN not_excl`

---

### 2.2 Classificatie van Kegelligplaatsen (`is_kegel` & `kegel_count`)
Voor schepen met gevaarlijke stoffen (ADN: 1 of 2 blauwe kegels / lichten) wordt het aantal toegestane kegels bepaald uit het veld `signalling` en de naam `name`:

```text
kegel_count = 0  als name een negatie bevat ("geen kegel", "0 kegel", "0-kegel")
              2  als "TWO_BLUE_LIGHTS" in signalling, of "2 kegel" / "twee kegel" in name
              1  als "ONE_BLUE_LIGHT" in signalling, of "kegel" in name
              0  anders
```

```text
is_kegel = (kegel_count > 0)
```

---

### 2.3 Effectieve Kadelengte, Aantal Rijen (`number_of_rows` vs. `width`) en Scheepscapaciteit
De kadelengte `L` komt uit het veld `length` (waarbij `L > 0`, anders 0,0 m).

#### Afleiding van het aantal rijen naast elkaar (`N_rijen`)
In FIS is `number_of_rows` slechts bij ~4% van de overnachtingsligplaatsen expliciet ingevuld, terwijl `width` (totale vakbreedte in het water) bij ~97% van de overnachtingsligplaatsen wél is ingevuld (bijv. 35--40 m bij de grote RWS-overnachtingshavens IJzendoorn en Haaften). 

Het aantal effectieve rijen wordt gedifferentieerd naar type ligplaats:

1. **Standaard kades en afmeersteigers**: Wanneer `number_of_rows` ontbreekt (`NaN`) of `<= 0` is, wordt het aantal breedterijen afgeleid uit `width` op basis van de standaard CEMT Va/Vb scheepsbreedte `B_schip = 11,40 m`, begrensd op maximaal 3 rijen. **Afwijking van de RVW:** RVW 2020 § 6.4.2 (p. 147) noemt voor een oever of kade ten hoogste twee breed en voor een steiger hooguit drie breed; het model past 3 toe op zowel kades als steigers, omdat FIS ze niet onderscheidt. Voor kades kan het aantal rijen daardoor te hoog uitvallen (open punt):
   ```text
   N_rijen_kade = number_of_rows                                als number_of_rows > 0
                = min(3, max(1, floor(width / 11,40)))          als number_of_rows ontbreekt en width > 0
                = 1                                             anders
   ```

2. **Open-water ankerplaatsen en reeden** (`name` bevat `"anker"` of `"reede"`, of `category == "ANCHORAGE"`):
   Op open water (zoals Westerschelde, Hollandsch Diep, Oosterschelde, Krammer, Waddenzee) is er geen vaste infrastructuur. Schepen liggen solitair achter hun boeganker en zwaaien vrij rond onder invloed van wind en getijdestroom (draaicirkels van AIS-transponders). De breedte van de polygoon (200 tot 450 m) representeert de zwaairuimte van één file schepen (*D*<sub>zwaai</sub> ≈ 300 m conform **PIANC WG 121**). Rijen worden derhalve berekend op basis van de zwaaidiameter:
   ```text
   N_rijen_anker = max(1, floor(width / 300,0))   (= 1 voor reguliere vaarwegcorridors)
   ```
   Hierdoor worden onrealistische vermenigvuldigingen (zoals 28 rijen schepen en 44,8 km kadelengte bij Terneuzen) voorkomen.

De totale **effectieve ligplaatslengte** (`effective_length_m`) is:

```text
L_eff = L * N_rijen   [m]
```

#### Discrete scheepscapaciteit per scheepslengtescenario (85 m, 110 m, 135 m)
Conform **RVW 2020 § 6.4.2** (p. 146, onderlinge afstand `s` tussen afgemeerde schepen) passen op een kade van lengte `L` per rij `floor( (L + s) / (L_schip + s) )` schepen achter elkaar, mits de ligplaats het betreffende schip toestaat (`maximal_ship_length` is `NaN` of `>= L_schip`):

| Scenario | CEMT-klasse | Maatgevend schip | Scheepslengte `L_schip` | Tussenruimte `s` | Formule per rij |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`capacity_ships_85m`** | Klasse IV | Johann Welker | 85,0 m | 10,0 m | `floor( (L + 10) / 95 )` |
| **`capacity_ships_110m`** | Klasse Va | Groot Rijnschip | 110,0 m | 10,0 m | `floor( (L + 10) / 120 )` |
| **`capacity_ships_135m`** | Klasse Vb | Verlengd Groot Rijnschip | 135,0 m | 10,0 m | `floor( (L + 10) / 145 )` |

**Afwijking van de RVW:** de RVW noemt `s` = 5 m voor klasse I-IV en 10 m voor klasse V en hoger. Het model gebruikt `s` = 10 m voor alle scenario's, ook voor 85 m (klasse IV); dat geeft voor dat scenario een iets lagere capaciteit dan met 5 m.

```text
capacity_ships_{L_schip} = floor((L + s) / (L_schip + s)) * N_rijen
                             als L > 0 EN (maximal_ship_length ontbreekt OF maximal_ship_length >= L_schip)
                         = 0 anders
```

---

### 2.4 Clustering van Overnachtingslocaties en Autoafzetplaatstest (RVW 2020 § 6.6)
Overnachtingsligplaatsen op dezelfde `route_id` die binnen `Delta km <= 0,5 km` (500 m) van het startpunt van een cluster liggen, worden samengevoegd tot één samengestelde overnachtingslocatie (`berth_clusters.geojson`).

**Ruimtelijke koppeling van autoafzetplaatsen (`has_cardropoff` op clusterniveau)**:
Omdat autoafzetsteigers in FIS vaak als apart kortverblijf-record (`maximum_stay_duration_h = 1.0` u, `category = "OTHER"`) naast de overnachtingssteigers staan geregistreerd, beschikt een cluster over een autoafzetplaats wanneer:

1. Ten minste één overnachtingsligplaats in het cluster zelf `has_cardropoff == True` heeft, **óf**
2. Er een ligplaats met `has_cardropoff == True` (ook met kort verblijf `< 12 u`) of een record uit `cardropoff.geoparquet` binnen `<= 500 m` (`Delta km <= 0,5 km` op dezelfde route of `<= 500 m` Euclidische afstand in RD New `EPSG:28992`) van het cluster ligt.

Conform **RVW 2020 § 6.6** moet elke grote overnachtingslocatie met een capaciteit van `>= 15` schepen over een autoafzetplaats beschikken:

```text
req_cardropoff_{L_schip}  <=>  total_capacity_ships_{L_schip} >= 15
```

```text
cardropoff_compliant_{L_schip}  <=>  NIET req_cardropoff_{L_schip}  OF  has_cardropoff
```

---

### 2.5 Ruimtelijke Netwerkdekking op Routetussenstukken (RVW 2020 § 6.4.2)
Volgens RVW 2020 § 6.4.2 geldt langs hoofdtransportassen en hoofdvaarwegen:

* **Reguliere overnachtingshavens**: onderlinge afstand maximaal `Delta L_max = 30 km`. Een schip bevindt zich overal op een gedekt traject op ten hoogste de **halve tussenafstand** (`r_overnacht = 15,0 km`) van de dichtstbijzijnde overnachtingsligplaats.
* **Kegelligplaatsen**: onderlinge afstand maximaal `Delta L_max = 60 km`, oftewel een maximale afstand tot de dichtstbijzijnde kegelligplaats van `r_kegel = 30,0 km`.

Voor elk routetussenstuk (edge) `e = (u, v)` met lengte `L = length_km` worden met **multi-source Dijkstra** op de vaarweggraaf `G` de kortste netwerkafstanden `d_u` en `d_v` bepaald vanaf de eindknopen `u` en `v` tot de dichtstbijzijnde ligplaats:

1. **Maximale afstand op het segment (`dist_to_overnight_km` / `dist_to_kegel_km`)**:
   Het verste punt op het segment `[u, v]` tot een ligplaats ligt óf op een van de eindknopen, óf op het interne punt waar de afstanden via `u` en via `v` aan elkaar gelijk zijn:

   ```text
   d_max(d_u, d_v, L) = min( (d_u + d_v + L) / 2,  d_u + L,  d_v + L )   [km]
   ```

2. **Exacte gedekte lengte op het segment (`compliant_30km_overnight_km` / `compliant_60km_kegel_km`)**:
   Met drempelstraal `r in {15,0 km, 30,0 km}` bedraagt de lengte van het segment die binnen netwerkafstand `r` van een ligplaats ligt:

   ```text
   L_compliant(d_u, d_v, L, r) = min( L,  max(0, r - d_u) + max(0, r - d_v) )   [km]
   ```

3. **Volledige dekking (`is_fully_compliant_*`)**:
   ```text
   is_fully_compliant  <=>  d_max(d_u, d_v, L) <= r
   ```

---

### 2.6 Verwachte Overnachtingsstops, Bedienbare Verkeersintensiteit en Corridorcapaciteit (≤ 15 km) per Route (RVW 2020 § 6.4.2)
Voor een vaarwegroute met totale lengte `L_route = sum L_e` (`total_length_km`) wordt gerekend met een vaartijd van `T_dag = 14 uur/etmaal` (exploitatievorm A1: ten hoogste 14 uur varen per 24 uur; Binnenvaartregeling art. 23.10, zie Corporate uitvoeringskader ligplaatsen RWS 2011, p. 41) bij een gemiddelde snelheid van `v_gem = 12 km/u` (projectaanname; RVW 2020 § 6.3 geeft circa 30 km per 2 uur varen, dus 15 km/u). De dagelijkse vaarafstand bedraagt daarmee:

```text
D_dag = v_gem * T_dag = 12 * 14 = 168,0 km/etmaal
```

1. **Verwachte overnachtingsstops per passage (`expected_overnight_stops`)**:
   ```text
   P_stop = L_route / 168,0                       [percentage schepen dat overnacht, RVW]
   P_stop,gecorrigeerd = f_overnacht * P_stop     [= expected_overnight_stops]
   f_overnacht = P_stop,gecorrigeerd / P_stop = 1 - c = 0,82
   ```

   `P_stop` is het "percentage schepen dat overnacht" uit RVW 2020 § 6.4.2 (p. 146): gemiddelde reistijd (`L_route / v_gem`) gedeeld door de maximale vaartijd (`T_dag`). `f_overnacht` corrigeert dit voor continuvaart en is **geen onderdeel van de RVW**: `c` is het aandeel continuvaart (scheepvaart tussen 23.00 en 06.00 uur), 18% op de Waal in 2012 (RWS WVL, *Toekomstige Ligplaatsbehoefte Overnachtingshaven Lobith 2013*, p. 10), dus `f_overnacht = 0,82`. De kolom `expected_overnight_stops` bevat `P_stop,gecorrigeerd`; alle `max_*`-kolommen hieronder gebruiken die waarde. De parameters `f_overnacht`, `sailing_speed_kmh` (12 km/u, projectaanname) en `sailing_hours_per_day` (14 u) staan als kolommen in de uitvoer, zodat stap 3 met dezelfde waarden rekent.

2. **Jaarlijkse kademeter-nachten (`overnight_meter_nights_yr` / `kegel_meter_nights_yr`)**:
   ```text
   M_jaar = L_kade * 365   [m*nachten / jaar]
   ```

3. **Maximale jaarlijkse bedienbare verkeersintensiteit in geaccumuleerde scheepslengte (`max_overnight_traffic_volume_km_yr`)**:
   ```text
   max_overnight_traffic_volume_km_yr = (L_kade / 1000) * 365 / P_stop
                                      = (L_kade / 1000) * 365 / (L_route / 168,0)   [km schip / jaar]
   ```

4. **Maximale jaarlijkse scheepspassages per scheepslengtescenario (`max_overnight_passages_{85m,110m,135m}_yr`)**:
   ```text
   max_overnight_passages_{L_schip}_yr = C_route(L_schip) * 365 / P_stop   [passages / jaar]
   ```

5. **Corridorcapaciteit (≤ 15 km) (`corridor_overnight_*` & `corridor_kegel_*`)**:
   Omdat ligplaatsen vaak in direct aantakkende havenbekkens of nevenroutes liggen (met een eigen `route_id`), worden via Dijkstra op de vaarweggraaf `G` voor elke route `r` alle unieke ligplaatsen verzameld waarvan de netwerkafstand tot ten minste één knoop van route `r` kleiner of gelijk is aan `r_overnacht = 15,0 km` (resp. `r_kegel = 30,0 km`):

   ```text
   B_corridor(r, R) = { b in B  |  min over u in V(r) van d_G(u, v_b) <= R }
   ```

   Waaruit analoog aan de lokale capaciteit worden berekend:

   * `corridor_overnight_berths` `= |B_corridor(r, 15 km)|`
   * `corridor_overnight_berth_length_m` `= sum[b in B_corridor] L_eff(b)`
   * `corridor_overnight_capacity_ships_{85m,110m,135m}` `= sum[b in B_corridor] C_b(L_schip)`
   * `corridor_max_overnight_traffic_volume_km_yr` `= (corridor_overnight_berth_length_m / 1000) * 365 / P_stop`

---

## 3. Volledige Veldtoelichting per Bestand (Data Dictionary)

### 3.1 `berths_analyzed.geoparquet`

#### A. Berekende analysevelden (toegevoegd door `analyze_spatial_capacity.py`)

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `has_cardropoff` | `bool` | - | `True` als `"CAR_DROPOFF" in facility` óf als `id` voorkomt als `berth_id` in `cardropoff.geoparquet`. |
| `ov_by_facility` | `bool` | - | `True` als `"OVERNIGHT_ACCOMODATION" in facility` en niet uitgesloten (`not_excl`). |
| `ov_by_stay` | `bool` | - | `True` als `maximum_stay_duration_h >= 12.0` en niet uitgesloten (`not_excl`). |
| `ov_by_category` | `bool` | - | `True` als `category` in `{"ALL_VESSELS", "PUSH_TOW_VESSELS", "NON_PUSH_TOW_VESSELS"}` en niet uitgesloten (`not_excl`). |
| `ov_by_long_quay` | `bool` | - | `True` als `length >= 100.0` m, `category == "OTHER"`, `maximum_stay_duration_h` is `NaN`, en niet uitgesloten (`not_excl`). |
| `is_overnight` | `bool` | - | Eindclassificatie overnachtingsligplaats: `ov_by_facility \| ov_by_stay \| ov_by_category \| ov_by_long_quay`. |
| `kegel_count` | `int64` | kegels | Aantal toegestane ADN-kegels (`0`, `1` of `2`) afgeleid uit `signalling` en `name`. |
| `is_kegel` | `bool` | - | Eindclassificatie kegelligplaats: `kegel_count > 0`. |
| `effective_rows` | `float64` | rijen | Effectief aantal scheepsrijen naast elkaar: `number_of_rows` indien `> 0`. Voor ankerplaatsen/reeden: afgeleid uit zwaaidiameter `max(1, floor(width / 300.0))` (= 1,0 file). Voor kades/steigers: afgeleid uit `min(3, max(1, floor(width / 11.4)))` (RVW § 6.4.2, p. 147; model past 3 breed toe op kades en steigers), anders `1.0`. |
| `effective_length_m` | `float64` | m | Totale effectieve afmeerlengte: `length * effective_rows`. |
| `capacity_ships_85m` | `float64` | schepen | Aantal schepen van **85 m (CEMT IV)** dat gelijktijdig kan afmeren: `floor((length + 10) / 95) * effective_rows` (indien toegestaan door `maximal_ship_length`). |
| `capacity_ships_110m` | `float64` | schepen | Aantal schepen van **110 m (CEMT Va)** dat gelijktijdig kan afmeren: `floor((length + 10) / 120) * effective_rows` (indien toegestaan door `maximal_ship_length`). |
| `capacity_ships_135m` | `float64` | schepen | Aantal schepen van **135 m (CEMT Vb)** dat gelijktijdig kan afmeren: `floor((length + 10) / 145) * effective_rows` (indien toegestaan door `maximal_ship_length`). |

#### B. Genormaliseerde FIS- en ISRS-bronvelden in `berths_analyzed.geoparquet`

| Veldnaam | Type | Toelichting |
| :--- | :--- | :--- |
| `id` | `str` | Unieke FIS-identificatiecode van de ligplaats (als string). |
| `geo_type` | `str` | FIS objecttype (`berth`). |
| `geo_generation` | `int64` | FIS publicatiegeneratie-nummer. |
| `name` | `str` | Naam van de ligplaats of overnachtingshaven in FIS. |
| `vin_code` | `str` | Vaarweginformatie (VIN) locatiecode. |
| `route_id` | `str` | FIS-identificatiecode van de vaarwegroute waarop de ligplaats ligt. |
| `route_km` | `float64` | Start-kilometrering van de ligplaats langs de gekoppelde vaarwegroute (km). |
| `route_km_end` | `float64` | Eind-kilometrering van de ligplaats langs de gekoppelde vaarwegroute (km). |
| `city` | `str` | Plaatsnaam. |
| `community` | `str` | Gemeentenaam. |
| `address` | `str` | Straat-/locatieadres indien bekend. |
| `note` | `str` | Vrije toelichting/opmerkingen uit FIS (bevat o.a. bijzondere afmeerregels). |
| `administration_id` | `float64` | Identificatiecode van de vaarwegbeheerder in FIS. |
| `section_id` | `str` | FIS vaarwegvak-ID (`fairway section`). |
| `fairway_id` | `str` | FIS vaarweg-ID. |
| `isrs_id` | `str` | Gekoppelde ISRS (International Ship Reporting Standard) locatiecode-ID. |
| `un_location_code` | `str` | VN-locatiecode (UN/LOCODE, 5 tekens). |
| `terminal_id` | `float64` | Gekoppeld terminal-ID in FIS. |
| `port_area_id` | `float64` | Gekoppeld havengebied-ID in FIS. |
| `condition` | `str` | Fysieke/operationele staat van de ligplaats. |
| `status` | `object` | Administratieve/juridische statuslijst uit FIS. |
| `category` | `str` | Hoofdcategorie van de ligplaats (`ALL_VESSELS`, `PUSH_TOW_VESSELS`, `NON_PUSH_TOW_VESSELS`, `WAITING_AREA`, `LOADING_AND_UNLOADING`, `OTHER`, etc.). |
| `facility` | `object` | Lijst van aanwezige voorzieningen (`OVERNIGHT_ACCOMODATION`, `CAR_DROPOFF`, `DRINKING_WATER`, `ELECTRICITY`, `WASTE_COLLECTION`, etc.). |
| `signalling` / `signaling` | `str` | Kegelregime-aanduiding (`ONE_BLUE_LIGHT`, `TWO_BLUE_LIGHTS`, etc.). |
| `length` | `float64` | Lengte van het ligplaatsvak in meters (m). |
| `width` | `float64` | Totale breedte van het ligplaatsvak in het water in meters (m). |
| `available_length` | `float64` | Beschikbare afmeerlengte voor schepen in meters (m). |
| `available_width` | `float64` | Beschikbare afmeerbreedte voor schepen in meters (m). |
| `available_height` | `float64` | Beschikbare vrije doorvaarthoogte bij de ligplaats (m). |
| `number_of_rows` | `float64` | Expliciet geregistreerd maximaal aantal schepen naast elkaar (in breedterijen). |
| `maximum_stay_duration_h` | `float64` | Maximale toegestane verblijfsduur in uren (u). |
| `minimal_ship_length` | `float64` | Minimale toegestane scheepslengte (m). |
| `maximal_ship_length` | `float64` | Maximale toegestane individuele scheepslengte (m). |
| `maximum_ship_width` | `float64` | Maximale toegestane individuele scheepsbreedte (m). |
| `maximum_ship_height` | `float64` | Maximale toegestane scheepshoogte (m). |
| `maximum_convoy_length` | `float64` | Maximale toegestane lengte voor samenstellen/koppelverbanden (m). |
| `maximum_convoy_width` | `float64` | Maximale toegestane breedte voor samenstellen/koppelverbanden (m). |
| `maximum_convoy_height` | `float64` | Maximale toegestane hoogte voor samenstellen/koppelverbanden (m). |
| `maximum_draught` | `float64` | Maximale toegestane diepgang ter plaatse van de ligplaats (m). |
| `width_position` | `str` | Positie ten opzichte van de vaarwegas (bijv. linker-/rechteroever). |
| `rotation` | `float64` | Oriëntatiehoek van het ligplaatssymbool in graden. |
| `has_crane` | `float64` | Aanwezigheid van een overslagkraan (`0` / `1` / `NaN`). |
| `ship_categories` | `object` | Lijst van toegestane scheepscategorieën. |
| `transshipment_goods` | `object` | Lijst van toegestane overslaggoederen. |
| `isps_code` | `float64` | ISPS-beveiligingscode (indien van toepassing). |
| `mooring_facilities_desc` | `str` | Beschrijving van het type afmeervoorziening (bolders, dukdalven, steiger, etc.). |
| `number_of_mooring_facilities` | `float64` | Aantal aanwezige afmeervoorzieningen (palen/bolders). |
| `shoreline_category` | `str` | Oevercategorie (kade, damwand, talud, steiger). |
| `construction_material` | `str` | Constructiemateriaal van de afmeervoorziening. |
| `quay_quality` | `str` | Kwaliteitsaanduiding van de kade. |
| `usage_conditions` | `str` | Gebruiksvoorwaarden van de ligplaats. |
| `accessibility` | `str` | Bereikbaarheid vanaf de wal (bijv. openbare weg, afgesloten terrein). |
| `transport_possibilities_desc` | `str` | Beschrijving van vervoersmogelijkheden vanaf de ligplaats. |
| `foreign_code` | `str` | Externe referentiecode. |
| `id_isrs`, `geo_type_isrs`, `geo_generation_isrs`, `name_isrs`, `geometry_isrs`, `code`, `country_code`, `fairway_route_id`, `function`, `hectometer`, `object_name`, `terminal_code`, `un_location_code_isrs`, `position_code` | divers | Gekoppelde ISRS-locatieattributen uit de FIS-export. |
| `geometry` | `geometry` | Punt- of polygoongeometrie van de ligplaats in `EPSG:4326` (WGS84). |

---

### 3.2 `berth_clusters.geojson`
Dit bestand bevat de geclusterde overnachtingslocaties (overnachtingsligplaatsen op dezelfde `route_id` binnen `Delta km <= 0,5 km`):

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `route_id` | `str` | - | FIS `route_id` waarop het cluster ligt. |
| `route_km_begin` | `float64` | km | Laagste `route_km` van de ligplaatsen in het cluster. |
| `route_km_end` | `float64` | km | Hoogste `route_km` van de ligplaatsen in het cluster. |
| `cluster_name` | `str` | - | Samengevoegde naam (maximaal de eerste 3 unieke namen in het cluster). |
| `num_berth_records` | `int64` | aantal | Aantal individuele `berth`-records dat in dit cluster is samengevoegd. |
| `total_length_m` | `float64` | m | Som van `effective_length_m` van alle ligplaatsen in het cluster. |
| `total_capacity_ships_85m` | `float64` | schepen | Totale clustercapaciteit voor **85 m (CEMT IV)** schepen. |
| `total_capacity_ships_110m` | `float64` | schepen | Totale clustercapaciteit voor **110 m (CEMT Va)** schepen. |
| `total_capacity_ships_135m` | `float64` | schepen | Totale clustercapaciteit voor **135 m (CEMT Vb)** schepen. |
| `has_cardropoff` | `bool` | - | `True` als een ligplaats in het cluster óf een nabijgelegen autoafzetsteiger / `cardropoff`-record binnen `<= 500 m` een autoafzetplaats heeft. |
| `req_cardropoff_85m` | `bool` | - | `True` als `total_capacity_ships_85m >= 15` (RVW 2020 § 6.6 normdrempel). |
| `cardropoff_compliant_85m` | `bool` | - | `True` als `not req_cardropoff_85m` of `has_cardropoff == True`. |
| `req_cardropoff_110m` | `bool` | - | `True` als `total_capacity_ships_110m >= 15`. |
| `cardropoff_compliant_110m` | `bool` | - | `True` als `not req_cardropoff_110m` of `has_cardropoff == True`. |
| `req_cardropoff_135m` | `bool` | - | `True` als `total_capacity_ships_135m >= 15`. |
| `cardropoff_compliant_135m` | `bool` | - | `True` als `not req_cardropoff_135m` of `has_cardropoff == True`. |
| `geometry` | `geometry` | - | Representatieve geometrie van het cluster (`EPSG:4326`). |

---

### 3.3 `route_segments_compliance.geoparquet`
Elk record in dit bestand vertegenwoordigt een **routetussenstuk (edge)** op de topologische vaarweggraaf tussen twee opeenvolgende splitsingsknopen (`SectionJunction`) of ligplaatsprojectiepunten op een route.

* `route_segments_compliance.geoparquet` bevat uitsluitend de segmenten op het **Hoofdvaarwegnet (`is_hvwn == True`)**.

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `source` | `str` | - | Startknoop-ID van het segment in de routegraaf (`sj_<section_junction_id>` voor kruispunten, `berth_<berth_id>` voor ligplaatsen, of `route_<route_id>_km_<km>` voor route-uiteinden). |
| `target` | `str` | - | Eindknoop-ID van het segment in de routegraaf. |
| `weight` | `float64` | km | Netwerkgewicht van de edge (gelijk aan `length_km`). |
| `route_id` | `str` | - | FIS `route_id` waartoe het segment behoort. |
| `route_name` | `str` | - | Officiële naam van de FIS-route. |
| `route_code` | `str` | - | Routecode in FIS (bijv. `101` voor de Waal, `225` voor het Amsterdam-Rijnkanaal). |
| `network_type` | `str` | - | Vaarwegnetwerkcategorie uit `fairwayclassification.geoparquet`: `Hoofdtransportas`, `Hoofdvaarweg`, `Overige vaarweg`, `Overig` of `Niet geclassificeerd / Zee`. |
| `km_from` | `float64` | km | Start-kilometrering van het segment op de route (`<= km_to`). |
| `km_to` | `float64` | km | Eind-kilometrering van het segment op de route (`>= km_from`). |
| `length_km` | `float64` | km | Lengte van het routetussenstuk: `km_to - km_from` (km). |
| `geometry` | `geometry` | - | Geknipte `LineString`-geometrie van het routetussenstuk (`EPSG:4326`). |
| `is_hvwn` | `bool` | - | `True` als `network_type` gelijk is aan `Hoofdtransportas` of `Hoofdvaarweg`. |
| `is_reachable_overnight` | `bool` | - | `True` als het segment via de vaarweggraaf verbonden is met ten minste één overnachtingsligplaats (`d_max < inf`). |
| `dist_to_overnight_km` | `float64` | km | Maximale netwerkafstand (`d_max`, zie § 2.5) vanaf het ongunstigste punt op dit segment tot de dichtstbijzijnde overnachtingsligplaats. |
| `compliant_30km_overnight_km` | `float64` | km | Exacte lengte van dit segment (in km, `0 <= L_compliant <= length_km`) die binnen `<= 15,0 km` van een overnachtingsligplaats ligt (voldoet aan de 30 km tussenafstandsnorm). |
| `is_fully_compliant_overnight` | `bool` | - | `True` als het **gehele** segment binnen `<= 15,0 km` van een overnachtingsligplaats ligt (`dist_to_overnight_km <= 15.0`). |
| `is_reachable_kegel` | `bool` | - | `True` als het segment via de vaarweggraaf verbonden is met ten minste één kegelligplaats (`d_max < inf`). |
| `dist_to_kegel_km` | `float64` | km | Maximale netwerkafstand (`d_max`, zie § 2.5) vanaf het ongunstigste punt op dit segment tot de dichtstbijzijnde kegelligplaats. |
| `compliant_60km_kegel_km` | `float64` | km | Exacte lengte van dit segment (in km) die binnen `<= 30,0 km` van een kegelligplaats ligt (voldoet aan de 60 km tussenafstandsnorm). |
| `is_fully_compliant_kegel` | `bool` | - | `True` als het **gehele** segment binnen `<= 30,0 km` van een kegelligplaats ligt (`dist_to_kegel_km <= 30.0`). |

---

### 3.4 `route_berth_capacity.geoparquet` (`.csv`, `.geojson`)
Elk record in dit bestand vertegenwoordigt één **volledige FIS-vaarwegroute** (`route_id`) met zowel de **lokale ligplaatscapaciteit** (ligplaatsen met exact dezelfde `route_id`), de **ruimtelijke dekkingsgraad**, als de **corridorcapaciteit (≤ 15 km)** (alle ligplaatsen die over de vaarweggraaf binnen `<= 15 km` resp. `<= 30 km` vanaf de route bereikbaar zijn).

* `route_berth_capacity.geoparquet` bevat de **HVWN-routes** (`is_hvwn == True`).

#### A. Algemene route-eigenschappen

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `route_id` | `str` | - | Unieke FIS `route_id` (als string). |
| `route_code` | `str` | - | Routecode uit FIS (`code`). |
| `route_name` | `str` | - | Officiële naam van de route uit FIS (`name`). |
| `network_type` | `str` | - | Vaarwegnetwerkcategorie: `Hoofdtransportas`, `Hoofdvaarweg`, `Overige vaarweg`, `Overig` of `Niet geclassificeerd / Zee`. |
| `is_hvwn` | `bool` | - | `True` als `network_type in ("Hoofdtransportas", "Hoofdvaarweg")`. |
| `total_length_km` | `float64` | km | Totale routelengte `L_route = sum length_km` van alle segmenten op de route. |
| `expected_overnight_stops` | `float64` | stops/passage | Verwachte overnachtingsfractie `P_stop,gecorrigeerd = f_overnacht * L_route / 168,0` bij 14-uursvaart à 12 km/u (zie § 2.6). |
| `f_overnacht` | `float64` | - | Aandeel passanten dat een overnachtingsplaats nodig heeft: `1 - aandeel continuvaart` = 0,82 (zelfde waarde op alle routes). |
| `sailing_speed_kmh` | `float64` | km/u | Gemiddelde vaarsnelheid `v` in de berekening van `P_stop` (12 km/u, projectaanname). |
| `sailing_hours_per_day` | `float64` | uur | Maximale vaartijd per etmaal `T` in de berekening van `P_stop` (14 u, exploitatievorm A1). |
| `geometry` | `geometry` | - | Lijngeometrie (`LineString` / `MultiLineString`) van de gehele route in `EPSG:4326`. |

#### B. Reguliere Overnachtingsligplaatsen — Lokale Capaciteit, Dekking & Verzorgingsgebied (`<= 15` km)

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `overnight_compliant_km` | `float64` | km | Som van `compliant_30km_overnight_km` over alle segmenten van de route (km route met afstand `<= 15 km` tot een overnachtingsligplaats). |
| `overnight_coverage_pct` | `float64` | % | Percentage van de routelengte dat voldoet aan de 30 km-norm: `(overnight_compliant_km / total_length_km) * 100`. |
| `num_overnight_berths` | `int64` | aantal | Aantal overnachtingsligplaatsen dat **lokaal** op deze `route_id` geregistreerd staat. |
| `overnight_berth_length_m` | `float64` | m | Totale effectieve kadelengte `L_kade = sum L_eff` van de lokale overnachtingsligplaatsen op deze `route_id`. |
| `overnight_capacity_ships_85m` | `float64` | schepen | Lokale scheepscapaciteit op deze `route_id` voor **85 m (CEMT IV)** schepen. |
| `overnight_capacity_ships_110m` | `float64` | schepen | Lokale scheepscapaciteit op deze `route_id` voor **110 m (CEMT Va)** schepen. |
| `overnight_capacity_ships_135m` | `float64` | schepen | Lokale scheepscapaciteit op deze `route_id` voor **135 m (CEMT Vb)** schepen. |
| `has_cardropoff` | `bool` | - | `True` als ten minste één lokale overnachtingsligplaats op deze `route_id` een gekoppelde autoafzetplaats heeft. |
| `overnight_meter_nights_yr` | `float64` | m·nacht/jr | Jaarlijks lokaal aanbod aan kademeter-nachten: `overnight_berth_length_m * 365`. |
| `max_overnight_traffic_volume_km_yr` | `float64` | km schip/jr | Maximale jaarlijkse bedienbare verkeersintensiteit op basis van lokale kadelengte: `((overnight_berth_length_m / 1000) * 365) / (expected_overnight_stops)`. |
| `max_overnight_passages_85m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse passages van **85 m (CEMT IV)** schepen op basis van lokale capaciteit: `(overnight_capacity_ships_85m * 365) / (expected_overnight_stops)`. |
| `max_overnight_passages_110m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse passages van **110 m (CEMT Va)** schepen op basis van lokale capaciteit: `(overnight_capacity_ships_110m * 365) / (expected_overnight_stops)`. |
| `max_overnight_passages_135m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse passages van **135 m (CEMT Vb)** schepen op basis van lokale capaciteit: `(overnight_capacity_ships_135m * 365) / (expected_overnight_stops)`. |
| `corridor_overnight_berths` | `int64` | aantal | Aantal unieke overnachtingsligplaatsen binnen het **netwerk-verzorgingsgebied (`<= 15 km`)** vanaf de knopen van deze route (inclusief zijhavens en aansluitende routes). |
| `corridor_overnight_berth_length_m` | `float64` | m | Totale effectieve kadelengte (m) van alle overnachtingsligplaatsen binnen het netwerk-verzorgingsgebied (`<= 15 km`). |
| `corridor_overnight_capacity_ships_85m` | `float64` | schepen | Totale scheepscapaciteit binnen het netwerk-verzorgingsgebied (`<= 15 km`) voor **85 m (CEMT IV)** schepen. |
| `corridor_overnight_capacity_ships_110m` | `float64` | schepen | Totale scheepscapaciteit binnen het netwerk-verzorgingsgebied (`<= 15 km`) voor **110 m (CEMT Va)** schepen. |
| `corridor_overnight_capacity_ships_135m` | `float64` | schepen | Totale scheepscapaciteit binnen het netwerk-verzorgingsgebied (`<= 15 km`) voor **135 m (CEMT Vb)** schepen. |
| `corridor_max_overnight_traffic_volume_km_yr` | `float64` | km schip/jr | Maximale jaarlijkse bedienbare verkeersintensiteit op basis van het `<= 15 km` verzorgingsgebied: `((corridor_overnight_berth_length_m / 1000) * 365) / (expected_overnight_stops)`. |

#### C. Kegelligplaatsen — Lokale Capaciteit, Dekking & Verzorgingsgebied (`<= 30` km straal / 60 km norm)

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `kegel_compliant_km` | `float64` | km | Som van `compliant_60km_kegel_km` over alle segmenten van de route (km route met afstand `<= 30 km` tot een kegelligplaats). |
| `kegel_coverage_pct` | `float64` | % | Percentage van de routelengte dat voldoet aan de 60 km-norm voor kegelligplaatsen: `(kegel_compliant_km / total_length_km) * 100`. |
| `num_kegel_berths` | `int64` | aantal | Aantal kegelligplaatsen dat **lokaal** op deze `route_id` geregistreerd staat. |
| `kegel_berth_length_m` | `float64` | m | Totale effectieve kadelengte `L_kade` van de lokale kegelligplaatsen op deze `route_id`. |
| `kegel_capacity_ships_85m` | `float64` | schepen | Lokale kegel-scheepscapaciteit op deze `route_id` voor **85 m (CEMT IV)** schepen. |
| `kegel_capacity_ships_110m` | `float64` | schepen | Lokale kegel-scheepscapaciteit op deze `route_id` voor **110 m (CEMT Va)** schepen. |
| `kegel_capacity_ships_135m` | `float64` | schepen | Lokale kegel-scheepscapaciteit op deze `route_id` voor **135 m (CEMT Vb)** schepen. |
| `kegel_meter_nights_yr` | `float64` | m·nacht/jr | Jaarlijks lokaal aanbod aan kegel-kademeternachten: `kegel_berth_length_m * 365`. |
| `max_kegel_traffic_volume_km_yr` | `float64` | km schip/jr | Maximale jaarlijkse kegel-bedienbare verkeersintensiteit op basis van lokale kadelengte: `((kegel_berth_length_m / 1000) * 365) / (expected_overnight_stops)`. |
| `max_kegel_passages_85m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse kegelpassages van **85 m (CEMT IV)** schepen (lokaal). |
| `max_kegel_passages_110m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse kegelpassages van **110 m (CEMT Va)** schepen (lokaal). |
| `max_kegel_passages_135m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse kegelpassages van **135 m (CEMT Vb)** schepen (lokaal). |
| `corridor_kegel_berths` | `int64` | aantal | Aantal unieke kegelligplaatsen binnen het **netwerk-verzorgingsgebied (`<= 30 km`)** vanaf de knopen van deze route. |
| `corridor_kegel_berth_length_m` | `float64` | m | Totale effectieve kadelengte (m) van alle kegelligplaatsen binnen het netwerk-verzorgingsgebied (`<= 30 km`). |
| `corridor_kegel_capacity_ships_85m` | `float64` | schepen | Totale kegel-scheepscapaciteit binnen het netwerk-verzorgingsgebied (`<= 30 km`) voor **85 m (CEMT IV)** schepen. |
| `corridor_kegel_capacity_ships_110m` | `float64` | schepen | Totale kegel-scheepscapaciteit binnen het netwerk-verzorgingsgebied (`<= 30 km`) voor **110 m (CEMT Va)** schepen. |
| `corridor_kegel_capacity_ships_135m` | `float64` | schepen | Totale kegel-scheepscapaciteit binnen het netwerk-verzorgingsgebied (`<= 30 km`) voor **135 m (CEMT Vb)** schepen. |
| `corridor_max_kegel_traffic_volume_km_yr` | `float64` | km schip/jr | Maximale jaarlijkse kegel-bedienbare verkeersintensiteit op basis van het `<= 30 km` verzorgingsgebied: `((corridor_kegel_berth_length_m / 1000) * 365) / (expected_overnight_stops)`. |

---

## 4. Oplevering aan stap 2 en 3 (interface)

Stap 2 en 3 lezen alleen `route_berth_capacity.geoparquet` en beperken zich tot het **Hoofdvaarwegennet**: de 49 routes met `network_type` Hoofdtransportas of Hoofdvaarweg (`is_hvwn`). `route_berth_capacity_all.geoparquet` (alle routes) is hiervoor niet nodig.

| Aspect | Afspraak |
| :--- | :--- |
| Sleutel | `route_id` (string, uniek). Gebruik niet `route_code`: die is niet in alle FIS-routes uniek. |
| Niveau | Eén rij per FIS-vaarwegroute. Aanbod is lokaal (`overnight_*`, `kegel_*`) en per corridor ≤ 15 km resp. ≤ 30 km (`corridor_overnight_*`, `corridor_kegel_*`). |
| Lengte | `total_length_km` (km): de lengte `L` in de formule voor `P_stop`. |
| Aanbod voor de toetsing | `*_berth_length_m` (m kade) en `*_capacity_ships_{85m,110m,135m}` (aantal schepen per lengtescenario). Dezelfde 85/110/135 m-scenario's gebruikt stap 2 voor de scheepsklassen. |
| Percentage dat overnacht | `expected_overnight_stops` = `P_stop,gecorrigeerd` (inclusief `f_overnacht` = 0,82, 12 km/u, 14 u). Stap 3 gebruikt deze kolom en de kolommen `f_overnacht`, `sailing_speed_kmh`, `sailing_hours_per_day`, zodat vraag en aanbod met dezelfde parameters zijn berekend: `Id = passages_eq_day_p95 * expected_overnight_stops`. |
| Formaat | Ruimtelijk bestand: `route_berth_capacity.geoparquet` is de interface (de `.csv` bevat geen geometrie en is geen invoer voor de volgende stappen). |
| Koppelen | Primair op `route_id`, terugval ruimtelijk op de routegeometrie. Daarom blijven zowel `route_id` als de geometrie in alle uitvoerbestanden van de stappen behouden en ongewijzigd (niet vereenvoudigd). |
| Geometrie | Eén `LineString` per route in **EPSG:4326**. BIVAS en de rest van het project werken in EPSG:28992 (`PROJECTED_CRS`): stap 2 moet herprojecteren voor ruimtelijke koppeling. |
| HVWN-routes zonder BIVAS-data | Blijven in stap 3 staan met onbekende intensiteit (niet nul). |

De uitvoer van stap 2 (`output/bivas-intensity/`) gebruikt dezelfde `route_id`, `route_code`, `total_length_km` en dezelfde routeselectie (`is_hvwn`), zodat stap 3 een directe join op `route_id` kan doen.
