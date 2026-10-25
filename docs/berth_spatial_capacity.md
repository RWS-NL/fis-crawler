# Ligplaatsen Ruimtelijke Capaciteits- en Netwerkdekkingsanalyse (`berth-analysis`)

Dit document beschrijft de rekenmethodiek, de gebruikte formules (conform **Richtlijnen Vaarwegen 2020**, § 6.3–6.6) en het volledige gegevensschema (alle velden) van de uitvoerbestanden die worden gegenereerd door [`analyze_spatial_capacity.py`](../scripts/berth_analysis/analyze_spatial_capacity.py) en [`route_network_graph.py`](../scripts/berth_analysis/route_network_graph.py).

---

## 1. Overzicht en Uitvoerbestanden

De pijplijn voert een landelijke netwerkanalyse uit op de FIS-ligplaatsendata (`output/fis-export/berth.geoparquet`) gekoppeld aan de FIS-vaarwegroutes (`route.geoparquet`, `routejunction.geoparquet`, `cardropoff.geoparquet` en `fairwayclassification.geoparquet`).

Uitvoeren van de analyse:

```bash
uv run python -m scripts.berth_analysis.analyze_spatial_capacity \
  --export-dir output/fis-export \
  --output-dir output/berth-analysis
```

De analyse genereert de volgende bestanden in `output/berth-analysis/`:

| Bestand | Inhoud |
| :--- | :--- |
| `berths_analyzed.geoparquet` | Alle FIS-ligplaatsen verrijkt met classificatievlaggen, kegelstatus, rij-afleiding uit `width` en scheepscapaciteit (85m / 110m / 135m) |
| `berth_clusters.geojson` | Geclusterde overnachtingslocaties ($\Delta \text{km} \le 0{,}5$ km) met toetsing op aanwezigheid van een autoafzetplaats (RVW 2020 § 6.6) |
| `route_segments_compliance.geoparquet` (`.geojson`) | Ruimtelijke dekkingsanalyse per routetussenstuk op het **Hoofdvaarwegnet (HVWN)** (Hoofdtransportassen + Hoofdvaarwegen) |
| `route_segments_compliance_all.geoparquet` | Ruimtelijke dekkingsanalyse per routetussenstuk over **alle** FIS-vaarwegroutes |
| `route_berth_capacity.geoparquet` (`.csv`, `.geojson`) | Geaggregeerde ligplaatscapaciteit, corridorcapaciteit (≤ 15 km) en maximale jaarlijkse bedienbare verkeersintensiteit per **HVWN-route** |
| `route_berth_capacity_all.geoparquet` | Geaggregeerde ligplaatscapaciteit, corridorcapaciteit (≤ 15 km) en maximale jaarlijkse bedienbare verkeersintensiteit over **alle** gekoppelde FIS-routes |
| `berth_spatial_report.md` | Samenvattend rapport in Markdown met landelijke tabellen, knelpunten, autoafzetplaatstest en rekenvoorbeelden |

---

## 2. Formules en Rekenmethodiek (RVW 2020 § 6.3–6.6)

### 2.1 Classificatie van Overnachtingsligplaatsen (`is_overnight`)
Omdat de ruwe FIS-export het vlaggetje `OVERNIGHT_ACCOMODATION` in `facility` slechts bij een deel van de ligplaatsen invult, wordt een ligplaats geclassificeerd als overnachtingsligplaats (`is_overnight = True`) wanneer deze **niet is uitgesloten** (`not_excl`) én aan ten minste één van vier inclusiecriteria voldoet:

$$\text{not\_excl} = \big(\text{maximum\_stay\_duration\_h is NaN} \lor \text{maximum\_stay\_duration\_h} \ge 12{,}0\big) \land \big(\text{category} \notin \{\text{"WAITING\_AREA"},\; \text{"LOADING\_AND\_UNLOADING"}\}\big) \land \neg \text{is\_recreation}$$

waarbij recreatie- en pleziervaartvoorzieningen (`is_recreation`) hard worden uitgesloten van de commerciële beroepsvaartcapaciteit:
* Ligplaatsen met `recreatie`, `pleziervaart`, `passanten` of `jacht` in de naam of toelichting (tenzij expliciet bestemd voor `duwvaart` of `beroepsvaart`);
* Ligplaatsen gelegen op routes van **jachtensluizen** of jachthavens (zoals *Ankerplaats 1 Krammer* in de westelijke voorhaven van de Krammer Jachtsluizen).

$$\text{is\_overnight} = \text{ov\_by\_facility} \lor \text{ov\_by\_stay} \lor \text{ov\_by\_category} \lor \text{ov\_by\_long\_quay}$$

waarbij (elk gecombineerd met $\text{not\_excl}$):

1. **Expliciete overnachtingsfaciliteit (`ov_by_facility`)**: $\text{"OVERNIGHT\_ACCOMODATION"} \in \text{facility} \land \text{not\_excl}$
2. **Verblijfsduur $\ge 12$ uur (`ov_by_stay`)**: $\text{maximum\_stay\_duration\_h} \ge 12{,}0 \land \text{not\_excl}$
3. **Commerciële binnenvaartcategorie (`ov_by_category`)**: $\text{category} \in \{\text{"ALL\_VESSELS"},\; \text{"PUSH\_TOW\_VESSELS"},\; \text{"NON\_PUSH\_TOW\_VESSELS"}\} \land \text{not\_excl}$
4. **Algemene lange kade zonder verblijfsbeperking (`ov_by_long_quay`)**: $\text{length} \ge 100{,}0\text{ m} \land \text{category} = \text{"OTHER"} \land \text{maximum\_stay\_duration\_h is NaN} \land \text{not\_excl}$

---

### 2.2 Classificatie van Kegelligplaatsen (`is_kegel` & `kegel_count`)
Voor schepen met gevaarlijke stoffen (ADN: 1 of 2 blauwe kegels / lichten) wordt het aantal toegestane kegels bepaald uit het veld `signalling` en de naam `name`:

$$\text{kegel\_count} = \begin{cases} 0 & \text{als } \text{name} \text{ een negatie bevat ("geen kegel", "0 kegel", "0-kegel")} \\ 2 & \text{als } \text{"TWO\_BLUE\_LIGHTS"} \in \text{signalling} \text{ of "2 kegel" / "twee kegel" in } \text{name} \\ 1 & \text{als } \text{"ONE\_BLUE\_LIGHT"} \in \text{signalling} \text{ of "kegel" in } \text{name} \\ 0 & \text{anders} \end{cases}$$

$$\text{is\_kegel} = (\text{kegel\_count} > 0)$$

---

### 2.3 Effectieve Kadelengte, Aantal Rijen (`number_of_rows` vs. `width`) en Scheepscapaciteit
De kadelengte $L$ komt uit het veld `length` (waarbij $L > 0$, anders $0{,}0\text{ m}$).

#### Afleiding van het aantal rijen naast elkaar ($N_{\text{rijen}}$)
In FIS is `number_of_rows` slechts bij ~4% van de overnachtingsligplaatsen expliciet ingevuld, terwijl `width` (totale vakbreedte in het water) bij ~97% van de overnachtingsligplaatsen wél is ingevuld (bijv. $35\text{--}40\text{ m}$ bij de grote RWS-overnachtingshavens IJzendoorn en Haaften). 

Het aantal effectieve rijen wordt gedifferentieerd naar type ligplaats:

1. **Standaard kades en afmeersteigers**: Wanneer `number_of_rows` ontbreekt (`NaN`) of $\le 0$ is, wordt het aantal breedterijen afgeleid uit `width` op basis van de standaard CEMT Va/Vb scheepsbreedte $B_{\text{schip}} = 11{,}40\text{ m}$, begrensd op maximaal 3 rijen conform **RVW 2020 § 6.4.2** en **BPR art. 7.05** (enkel, dubbel of 3-dik afmeren):
   $$N_{\text{rijen, kade}} = \begin{cases} \text{number\_of\_rows} & \text{als } \text{number\_of\_rows} > 0 \\[6pt] \min\left(3,\; \max\left(1,\; \left\lfloor \dfrac{\text{width}}{11{,}40} \right\rfloor\right)\right) & \text{als } \text{number\_of\_rows is NaN} \land \text{width} > 0 \\[8pt] 1 & \text{anders} \end{cases}$$

2. **Open-water ankerplaatsen en reeden** (`name` bevat `"anker"` of `"reede"`, of `category == "ANCHORAGE"`):
   Op open water (zoals Westerschelde, Hollandsch Diep, Oosterschelde, Krammer, Waddenzee) is er geen vaste infrastructuur. Schepen liggen solitair achter hun boeganker en zwaaien vrij rond onder invloed van wind en getijdestroom (draaicirkels van AIS-transponders). De breedte van de polygoon (200 tot 450 m) representeert de zwaairuimte van één file schepen (*D*<sub>zwaai</sub> ≈ 300 m conform **PIANC WG 121**). Rijen worden derhalve berekend op basis van de zwaaidiameter:
   $$N_{\text{rijen, anker}} = \max\left(1,\; \left\lfloor \dfrac{\text{width}}{300{,}0} \right\rfloor\right) \quad (= 1{,}0\text{ voor reguliere vaarwegcorridors})$$
   Hierdoor worden onrealistische vermenigvuldigingen (zoals 28 rijen schepen en 44,8 km kadelengte bij Terneuzen) voorkomen.

De totale **effectieve ligplaatslengte** (`effective_length_m`) is:

$$L_{\text{eff}} = L \times N_{\text{rijen}} \quad [\text{m}]$$

#### Discrete scheepscapaciteit per scheepslengtescenario (85 m, 110 m, 135 m)
Conform **RVW 2020 § 6.4.2** (p. 146, onderlinge lengte-afstand $s = 10{,}0\text{ m}$ tussen afgemeerde schepen) passen op een kade van lengte $L$ per rij $\lfloor (L + s) / (L_{\text{schip}} + s) \rfloor$ schepen achter elkaar, mits de ligplaats het betreffende schip toestaat (`maximal_ship_length` is `NaN` of $\ge L_{\text{schip}}$):

| Scenario | CEMT-klasse | Maatgevend schip | Scheepslengte $L_{\text{schip}}$ | Tussenruimte $s$ | Formule per rij |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`capacity_ships_85m`** | Klasse IV | Johann Welker | $85{,}0\text{ m}$ | $10{,}0\text{ m}$ | $\lfloor (L + 10) / 95 \rfloor$ |
| **`capacity_ships_110m`** | Klasse Va | Groot Rijnschip | $110{,}0\text{ m}$ | $10{,}0\text{ m}$ | $\lfloor (L + 10) / 120 \rfloor$ |
| **`capacity_ships_135m`** | Klasse Vb | Verlengd Groot Rijnschip | $135{,}0\text{ m}$ | $10{,}0\text{ m}$ | $\lfloor (L + 10) / 145 \rfloor$ |

$$\text{capacity\_ships\_}\{L_{\text{schip}}\} = \begin{cases} \left\lfloor \dfrac{L + s}{L_{\text{schip}} + s} \right\rfloor \times N_{\text{rijen}} & \text{als } L > 0 \land (\text{maximal\_ship\_length is NaN} \lor \text{maximal\_ship\_length} \ge L_{\text{schip}}) \\[10pt] 0 & \text{anders} \end{cases}$$

---

### 2.4 Clustering van Overnachtingslocaties en Autoafzetplaatstest (RVW 2020 § 6.6)
Overnachtingsligplaatsen op dezelfde `route_id` die binnen $\Delta \text{km} \le 0{,}5\text{ km}$ ($500\text{ m}$) van het startpunt van een cluster liggen, worden samengevoegd tot één samengestelde overnachtingslocatie (`berth_clusters.geojson`).

**Ruimtelijke koppeling van autoafzetplaatsen (`has_cardropoff` op clusterniveau)**:
Omdat autoafzetsteigers in FIS vaak als apart kortverblijf-record (`maximum_stay_duration_h = 1.0` u, `category = "OTHER"`) naast de overnachtingssteigers staan geregistreerd, beschikt een cluster over een autoafzetplaats wanneer:

1. Ten minste één overnachtingsligplaats in het cluster zelf `has_cardropoff == True` heeft, **óf**
2. Er een ligplaats met `has_cardropoff == True` (ook met kort verblijf $< 12\text{ u}$) of een record uit `cardropoff.geoparquet` binnen $\le 500\text{ m}$ ($\Delta \text{km} \le 0{,}5\text{ km}$ op dezelfde route of $\le 500\text{ m}$ Euclidische afstand in RD New `EPSG:28992`) van het cluster ligt.

Conform **RVW 2020 § 6.6** moet elke grote overnachtingslocatie met een capaciteit van $\ge 15$ schepen over een autoafzetplaats beschikken:

$$\text{req\_cardropoff\_}\{L_{\text{schip}}\} \iff \text{total\_capacity\_ships\_}\{L_{\text{schip}}\} \ge 15$$

$$\text{cardropoff\_compliant\_}\{L_{\text{schip}}\} \iff \neg \text{req\_cardropoff\_}\{L_{\text{schip}}\} \lor \text{has\_cardropoff}$$

---

### 2.5 Ruimtelijke Netwerkdekking op Routetussenstukken (RVW 2020 § 6.4.2)
Volgens RVW 2020 § 6.4.2 geldt langs hoofdtransportassen en hoofdvaarwegen:

* **Reguliere overnachtingshavens**: onderlinge afstand maximaal $\Delta L_{\text{max}} = 30\text{ km}$. Een schip bevindt zich overal op een gedekt traject op ten hoogste de **halve tussenafstand** ($r_{\text{overnacht}} = 15{,}0\text{ km}$) van de dichtstbijzijnde overnachtingsligplaats.
* **Kegelligplaatsen**: onderlinge afstand maximaal $\Delta L_{\text{max}} = 60\text{ km}$, oftewel een maximale afstand tot de dichtstbijzijnde kegelligplaats van $r_{\text{kegel}} = 30{,}0\text{ km}$.

Voor elk routetussenstuk (edge) $e = (u, v)$ met lengte $L = \text{length\_km}$ worden met **multi-source Dijkstra** op de vaarweggraaf $G$ de kortste netwerkafstanden $d_u$ en $d_v$ bepaald vanaf de eindknopen $u$ en $v$ tot de dichtstbijzijnde ligplaats:

1. **Maximale afstand op het segment (`dist_to_overnight_km` / `dist_to_kegel_km`)**:
   Het verste punt op het segment $[u, v]$ tot een ligplaats ligt óf op een van de eindknopen, óf op het interne punt waar de afstanden via $u$ en via $v$ aan elkaar gelijk zijn:

   $$d_{\text{max}}(d_u, d_v, L) = \min\left(\frac{d_u + d_v + L}{2},\; d_u + L,\; d_v + L\right) \quad [\text{km}]$$

2. **Exacte gedekte lengte op het segment (`compliant_30km_overnight_km` / `compliant_60km_kegel_km`)**:
   Met drempelstraal $r \in \{15{,}0\text{ km},\; 30{,}0\text{ km}\}$ bedraagt de lengte van het segment die binnen netwerkafstand $r$ van een ligplaats ligt:

   $$L_{\text{compliant}}(d_u, d_v, L, r) = \min\big(L,\; \max(0,\; r - d_u) + \max(0,\; r - d_v)\big) \quad [\text{km}]$$

3. **Volledige dekking (`is_fully_compliant_*`)**:
   $$\text{is\_fully\_compliant} \iff d_{\text{max}}(d_u, d_v, L) \le r$$

---

### 2.6 Verwachte Overnachtingsstops, Bedienbare Verkeersintensiteit en Corridorcapaciteit (≤ 15 km) per Route (RVW 2020 § 6.4.2)
Voor een vaarwegroute met totale lengte $L_{\text{route}} = \sum L_e$ (`total_length_km`) hanteert RVW 2020 § 6.3–6.4 voor 14-uursvaart (regime A1) een gemiddelde vaartijd van $T_{\text{dag}} = 14\text{ uur/etmaal}$ bij een gemiddelde snelheid van $v_{\text{gem}} = 12\text{ km/u}$. De dagelijkse vaarafstand bedraagt daarmee:

$$D_{\text{dag}} = v_{\text{gem}} \times T_{\text{dag}} = 12 \times 14 = 168{,}0\text{ km/etmaal}$$

1. **Verwachte overnachtingsstops per passage (`expected_overnight_stops`)**:
   $$P_{\text{stop}} = \frac{L_{\text{route}}}{168{,}0} \quad [\text{stops / passage}]$$

2. **Jaarlijkse kademeter-nachten (`overnight_meter_nights_yr` / `kegel_meter_nights_yr`)**:
   $$M_{\text{jaar}} = L_{\text{kade}} \times 365 \quad [\text{m}\cdot\text{nachten / jaar}]$$

3. **Maximale jaarlijkse bedienbare verkeersintensiteit in geaccumuleerde scheepslengte (`max_overnight_traffic_volume_km_yr`)**:
   $$\text{max\_overnight\_traffic\_volume\_km\_yr} = \frac{(L_{\text{kade}} / 1000) \times 365}{P_{\text{stop}}} = \frac{(L_{\text{kade}} / 1000) \times 365}{L_{\text{route}} / 168{,}0} \quad [\text{km schip / jaar}]$$

4. **Maximale jaarlijkse scheepspassages per scheepslengtescenario (`max_overnight_passages_{85m,110m,135m}_yr`)**:
   $$\text{max\_overnight\_passages\_}\{L_{\text{schip}}\}\_\text{yr} = \frac{C_{\text{route}}(L_{\text{schip}}) \times 365}{P_{\text{stop}}} \quad [\text{passages / jaar}]$$

5. **Corridorcapaciteit (≤ 15 km) (`corridor_overnight_*` & `corridor_kegel_*`)**:
   Omdat ligplaatsen vaak in direct aantakkende havenbekkens of nevenroutes liggen (met een eigen `route_id`), worden via Dijkstra op de vaarweggraaf $G$ voor elke route $r$ alle unieke ligplaatsen verzameld waarvan de netwerkafstand tot ten minste één knoop van route $r$ kleiner of gelijk is aan $r_{\text{overnacht}} = 15{,}0\text{ km}$ (resp. $r_{\text{kegel}} = 30{,}0\text{ km}$):

   $$\mathcal{B}_{\text{corridor}}(r, R) = \left\{ b \in \mathcal{B} \;\middle|\; \min_{u \in V(r)} d_G(u, v_b) \le R \right\}$$

   Waaruit analoog aan de lokale capaciteit worden berekend:

   * `corridor_overnight_berths` $= |\mathcal{B}_{\text{corridor}}(r, 15\text{ km})|$
   * `corridor_overnight_berth_length_m` $= \sum_{b \in \mathcal{B}_{\text{corridor}}} L_{\text{eff}}(b)$
   * `corridor_overnight_capacity_ships_{85m,110m,135m}` $= \sum_{b \in \mathcal{B}_{\text{corridor}}} C_b(L_{\text{schip}})$
   * `corridor_max_overnight_traffic_volume_km_yr` $= \dfrac{\big(\text{corridor\_overnight\_berth\_length\_m} / 1000\big) \times 365}{P_{\text{stop}}}$

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
| `effective_rows` | `float64` | rijen | Effectief aantal scheepsrijen naast elkaar: `number_of_rows` indien $> 0$. Voor ankerplaatsen/reeden: afgeleid uit zwaaidiameter `max(1, floor(width / 300.0))` (= 1,0 file). Voor kades/steigers: afgeleid uit `min(3, max(1, floor(width / 11.4)))` (RVW § 6.4.2 / BPR art. 7.05), anders `1.0`. |
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
Dit bestand bevat de geclusterde overnachtingslocaties (overnachtingsligplaatsen op dezelfde `route_id` binnen $\Delta \text{km} \le 0{,}5\text{ km}$):

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
| `has_cardropoff` | `bool` | - | `True` als een ligplaats in het cluster óf een nabijgelegen autoafzetsteiger / `cardropoff`-record binnen $\le 500\text{ m}$ een autoafzetplaats heeft. |
| `req_cardropoff_85m` | `bool` | - | `True` als `total_capacity_ships_85m >= 15` (RVW 2020 § 6.6 normdrempel). |
| `cardropoff_compliant_85m` | `bool` | - | `True` als `not req_cardropoff_85m` of `has_cardropoff == True`. |
| `req_cardropoff_110m` | `bool` | - | `True` als `total_capacity_ships_110m >= 15`. |
| `cardropoff_compliant_110m` | `bool` | - | `True` als `not req_cardropoff_110m` of `has_cardropoff == True`. |
| `req_cardropoff_135m` | `bool` | - | `True` als `total_capacity_ships_135m >= 15`. |
| `cardropoff_compliant_135m` | `bool` | - | `True` als `not req_cardropoff_135m` of `has_cardropoff == True`. |
| `geometry` | `geometry` | - | Representatieve geometrie van het cluster (`EPSG:4326`). |

---

### 3.3 `route_segments_compliance.geoparquet` & `route_segments_compliance_all.geoparquet`
Elk record in deze bestanden vertegenwoordigt een **routetussenstuk (edge)** op de topologische vaarweggraaf tussen twee opeenvolgende splitsingsknopen (`SectionJunction`) of ligplaatsprojectiepunten op een route.

* `route_segments_compliance.geoparquet` bevat uitsluitend de segmenten op het **Hoofdvaarwegnet (`is_hvwn == True`)**.
* `route_segments_compliance_all.geoparquet` bevat de segmenten van **alle** FIS-routes.

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `source` | `str` | - | Startknoop-ID van het segment in de routegraaf (`sj_<section_junction_id>` voor kruispunten, `berth_<berth_id>` voor ligplaatsen, of `route_<route_id>_km_<km>` voor route-uiteinden). |
| `target` | `str` | - | Eindknoop-ID van het segment in de routegraaf. |
| `weight` | `float64` | km | Netwerkgewicht van de edge (gelijk aan `length_km`). |
| `route_id` | `str` | - | FIS `route_id` waartoe het segment behoort. |
| `route_name` | `str` | - | Officiële naam van de FIS-route. |
| `route_code` | `str` | - | Routecode in FIS (bijv. `101` voor de Waal, `225` voor het Amsterdam-Rijnkanaal). |
| `network_type` | `str` | - | Vaarwegnetwerkcategorie uit `fairwayclassification.geoparquet`: `Hoofdtransportas`, `Hoofdvaarweg`, `Overige vaarweg`, `Overig` of `Niet geclassificeerd / Zee`. |
| `km_from` | `float64` | km | Start-kilometrering van het segment op de route ($\le \text{km\_to}$). |
| `km_to` | `float64` | km | Eind-kilometrering van het segment op de route ($\ge \text{km\_from}$). |
| `length_km` | `float64` | km | Lengte van het routetussenstuk: $\text{km\_to} - \text{km\_from}$ (km). |
| `geometry` | `geometry` | - | Geknipte `LineString`-geometrie van het routetussenstuk (`EPSG:4326`). |
| `is_hvwn` | `bool` | - | `True` als `network_type` gelijk is aan `Hoofdtransportas` of `Hoofdvaarweg`. |
| `is_reachable_overnight` | `bool` | - | `True` als het segment via de vaarweggraaf verbonden is met ten minste één overnachtingsligplaats ($d_{\text{max}} < \infty$). |
| `dist_to_overnight_km` | `float64` | km | Maximale netwerkafstand ($d_{\text{max}}$, zie § 2.5) vanaf het ongunstigste punt op dit segment tot de dichtstbijzijnde overnachtingsligplaats. |
| `compliant_30km_overnight_km` | `float64` | km | Exacte lengte van dit segment (in km, $0 \le L_{\text{compliant}} \le \text{length\_km}$) die binnen $\le 15{,}0\text{ km}$ van een overnachtingsligplaats ligt (voldoet aan de 30 km tussenafstandsnorm). |
| `is_fully_compliant_overnight` | `bool` | - | `True` als het **gehele** segment binnen $\le 15{,}0\text{ km}$ van een overnachtingsligplaats ligt (`dist_to_overnight_km <= 15.0`). |
| `is_reachable_kegel` | `bool` | - | `True` als het segment via de vaarweggraaf verbonden is met ten minste één kegelligplaats ($d_{\text{max}} < \infty$). |
| `dist_to_kegel_km` | `float64` | km | Maximale netwerkafstand ($d_{\text{max}}$, zie § 2.5) vanaf het ongunstigste punt op dit segment tot de dichtstbijzijnde kegelligplaats. |
| `compliant_60km_kegel_km` | `float64` | km | Exacte lengte van dit segment (in km) die binnen $\le 30{,}0\text{ km}$ van een kegelligplaats ligt (voldoet aan de 60 km tussenafstandsnorm). |
| `is_fully_compliant_kegel` | `bool` | - | `True` als het **gehele** segment binnen $\le 30{,}0\text{ km}$ van een kegelligplaats ligt (`dist_to_kegel_km <= 30.0`). |

---

### 3.4 `route_berth_capacity.geoparquet` (`.csv`, `.geojson`) & `route_berth_capacity_all.geoparquet`
Elk record in deze bestanden vertegenwoordigt één **volledige FIS-vaarwegroute** (`route_id`) met zowel de **lokale ligplaatscapaciteit** (ligplaatsen met exact dezelfde `route_id`), de **ruimtelijke dekkingsgraad**, als de **corridorcapaciteit (≤ 15 km)** (alle ligplaatsen die over de vaarweggraaf binnen $\le 15\text{ km}$ resp. $\le 30\text{ km}$ vanaf de route bereikbaar zijn).

* `route_berth_capacity.geoparquet` bevat de **HVWN-routes** (`is_hvwn == True`).
* `route_berth_capacity_all.geoparquet` bevat **alle gekoppelde FIS-routes**.

#### A. Algemene route-eigenschappen

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `route_id` | `str` | - | Unieke FIS `route_id` (als string). |
| `route_code` | `str` | - | Routecode uit FIS (`code`). |
| `route_name` | `str` | - | Officiële naam van de route uit FIS (`name`). |
| `network_type` | `str` | - | Vaarwegnetwerkcategorie: `Hoofdtransportas`, `Hoofdvaarweg`, `Overige vaarweg`, `Overig` of `Niet geclassificeerd / Zee`. |
| `is_hvwn` | `bool` | - | `True` als `network_type in ("Hoofdtransportas", "Hoofdvaarweg")`. |
| `total_length_km` | `float64` | km | Totale routelengte $L_{\text{route}} = \sum \text{length\_km}$ van alle segmenten op de route. |
| `expected_overnight_stops` | `float64` | stops/passage | Verwachte overnachtingsfractie $P_{\text{stop}} = L_{\text{route}} / 168{,}0$ bij 14-uursvaart à $12\text{ km/u}$ (zie § 2.6). |
| `geometry` | `geometry` | - | Lijngeometrie (`LineString` / `MultiLineString`) van de gehele route in `EPSG:4326`. |

#### B. Reguliere Overnachtingsligplaatsen — Lokale Capaciteit, Dekking & Verzorgingsgebied ($\le 15$ km)

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `overnight_compliant_km` | `float64` | km | Som van `compliant_30km_overnight_km` over alle segmenten van de route (km route met afstand $\le 15\text{ km}$ tot een overnachtingsligplaats). |
| `overnight_coverage_pct` | `float64` | % | Percentage van de routelengte dat voldoet aan de 30 km-norm: $(\text{overnight\_compliant\_km} / \text{total\_length\_km}) \times 100$. |
| `num_overnight_berths` | `int64` | aantal | Aantal overnachtingsligplaatsen dat **lokaal** op deze `route_id` geregistreerd staat. |
| `overnight_berth_length_m` | `float64` | m | Totale effectieve kadelengte $L_{\text{kade}} = \sum L_{\text{eff}}$ van de lokale overnachtingsligplaatsen op deze `route_id`. |
| `overnight_capacity_ships_85m` | `float64` | schepen | Lokale scheepscapaciteit op deze `route_id` voor **85 m (CEMT IV)** schepen. |
| `overnight_capacity_ships_110m` | `float64` | schepen | Lokale scheepscapaciteit op deze `route_id` voor **110 m (CEMT Va)** schepen. |
| `overnight_capacity_ships_135m` | `float64` | schepen | Lokale scheepscapaciteit op deze `route_id` voor **135 m (CEMT Vb)** schepen. |
| `has_cardropoff` | `bool` | - | `True` als ten minste één lokale overnachtingsligplaats op deze `route_id` een gekoppelde autoafzetplaats heeft. |
| `overnight_meter_nights_yr` | `float64` | m·nacht/jr | Jaarlijks lokaal aanbod aan kademeter-nachten: $\text{overnight\_berth\_length\_m} \times 365$. |
| `max_overnight_traffic_volume_km_yr` | `float64` | km schip/jr | Maximale jaarlijkse bedienbare verkeersintensiteit op basis van lokale kadelengte: $\dfrac{(\text{overnight\_berth\_length\_m} / 1000) \times 365}{\text{expected\_overnight\_stops}}$. |
| `max_overnight_passages_85m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse passages van **85 m (CEMT IV)** schepen op basis van lokale capaciteit: $\dfrac{\text{overnight\_capacity\_ships\_85m} \times 365}{\text{expected\_overnight\_stops}}$. |
| `max_overnight_passages_110m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse passages van **110 m (CEMT Va)** schepen op basis van lokale capaciteit: $\dfrac{\text{overnight\_capacity\_ships\_110m} \times 365}{\text{expected\_overnight\_stops}}$. |
| `max_overnight_passages_135m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse passages van **135 m (CEMT Vb)** schepen op basis van lokale capaciteit: $\dfrac{\text{overnight\_capacity\_ships\_135m} \times 365}{\text{expected\_overnight\_stops}}$. |
| `corridor_overnight_berths` | `int64` | aantal | Aantal unieke overnachtingsligplaatsen binnen het **netwerk-verzorgingsgebied ($\le 15\text{ km}$)** vanaf de knopen van deze route (inclusief zijhavens en aansluitende routes). |
| `corridor_overnight_berth_length_m` | `float64` | m | Totale effectieve kadelengte (m) van alle overnachtingsligplaatsen binnen het netwerk-verzorgingsgebied ($\le 15\text{ km}$). |
| `corridor_overnight_capacity_ships_85m` | `float64` | schepen | Totale scheepscapaciteit binnen het netwerk-verzorgingsgebied ($\le 15\text{ km}$) voor **85 m (CEMT IV)** schepen. |
| `corridor_overnight_capacity_ships_110m` | `float64` | schepen | Totale scheepscapaciteit binnen het netwerk-verzorgingsgebied ($\le 15\text{ km}$) voor **110 m (CEMT Va)** schepen. |
| `corridor_overnight_capacity_ships_135m` | `float64` | schepen | Totale scheepscapaciteit binnen het netwerk-verzorgingsgebied ($\le 15\text{ km}$) voor **135 m (CEMT Vb)** schepen. |
| `corridor_max_overnight_traffic_volume_km_yr` | `float64` | km schip/jr | Maximale jaarlijkse bedienbare verkeersintensiteit op basis van het $\le 15\text{ km}$ verzorgingsgebied: $\dfrac{(\text{corridor\_overnight\_berth\_length\_m} / 1000) \times 365}{\text{expected\_overnight\_stops}}$. |

#### C. Kegelligplaatsen — Lokale Capaciteit, Dekking & Verzorgingsgebied ($\le 30$ km straal / 60 km norm)

| Veldnaam | Type | Eenheid | Formule / Toelichting |
| :--- | :--- | :--- | :--- |
| `kegel_compliant_km` | `float64` | km | Som van `compliant_60km_kegel_km` over alle segmenten van de route (km route met afstand $\le 30\text{ km}$ tot een kegelligplaats). |
| `kegel_coverage_pct` | `float64` | % | Percentage van de routelengte dat voldoet aan de 60 km-norm voor kegelligplaatsen: $(\text{kegel\_compliant\_km} / \text{total\_length\_km}) \times 100$. |
| `num_kegel_berths` | `int64` | aantal | Aantal kegelligplaatsen dat **lokaal** op deze `route_id` geregistreerd staat. |
| `kegel_berth_length_m` | `float64` | m | Totale effectieve kadelengte $L_{\text{kade}}$ van de lokale kegelligplaatsen op deze `route_id`. |
| `kegel_capacity_ships_85m` | `float64` | schepen | Lokale kegel-scheepscapaciteit op deze `route_id` voor **85 m (CEMT IV)** schepen. |
| `kegel_capacity_ships_110m` | `float64` | schepen | Lokale kegel-scheepscapaciteit op deze `route_id` voor **110 m (CEMT Va)** schepen. |
| `kegel_capacity_ships_135m` | `float64` | schepen | Lokale kegel-scheepscapaciteit op deze `route_id` voor **135 m (CEMT Vb)** schepen. |
| `kegel_meter_nights_yr` | `float64` | m·nacht/jr | Jaarlijks lokaal aanbod aan kegel-kademeternachten: $\text{kegel\_berth\_length\_m} \times 365$. |
| `max_kegel_traffic_volume_km_yr` | `float64` | km schip/jr | Maximale jaarlijkse kegel-bedienbare verkeersintensiteit op basis van lokale kadelengte: $\dfrac{(\text{kegel\_berth\_length\_m} / 1000) \times 365}{\text{expected\_overnight\_stops}}$. |
| `max_kegel_passages_85m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse kegelpassages van **85 m (CEMT IV)** schepen (lokaal). |
| `max_kegel_passages_110m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse kegelpassages van **110 m (CEMT Va)** schepen (lokaal). |
| `max_kegel_passages_135m_yr` | `float64` | passages/jr | Maximaal aantal jaarlijkse kegelpassages van **135 m (CEMT Vb)** schepen (lokaal). |
| `corridor_kegel_berths` | `int64` | aantal | Aantal unieke kegelligplaatsen binnen het **netwerk-verzorgingsgebied ($\le 30\text{ km}$)** vanaf de knopen van deze route. |
| `corridor_kegel_berth_length_m` | `float64` | m | Totale effectieve kadelengte (m) van alle kegelligplaatsen binnen het netwerk-verzorgingsgebied ($\le 30\text{ km}$). |
| `corridor_kegel_capacity_ships_85m` | `float64` | schepen | Totale kegel-scheepscapaciteit binnen het netwerk-verzorgingsgebied ($\le 30\text{ km}$) voor **85 m (CEMT IV)** schepen. |
| `corridor_kegel_capacity_ships_110m` | `float64` | schepen | Totale kegel-scheepscapaciteit binnen het netwerk-verzorgingsgebied ($\le 30\text{ km}$) voor **110 m (CEMT Va)** schepen. |
| `corridor_kegel_capacity_ships_135m` | `float64` | schepen | Totale kegel-scheepscapaciteit binnen het netwerk-verzorgingsgebied ($\le 30\text{ km}$) voor **135 m (CEMT Vb)** schepen. |
| `corridor_max_kegel_traffic_volume_km_yr` | `float64` | km schip/jr | Maximale jaarlijkse kegel-bedienbare verkeersintensiteit op basis van het $\le 30\text{ km}$ verzorgingsgebied: $\dfrac{(\text{corridor\_kegel\_berth\_length\_m} / 1000) \times 365}{\text{expected\_overnight\_stops}}$. |
