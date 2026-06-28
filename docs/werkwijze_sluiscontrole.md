---
title: "Werkwijze sluiscontrole — RWS vaarweginformatie"
subtitle: "Deel A: Methodiek en verantwoording"
date: "2026-06-28"
lang: nl
---

# Werkwijze sluiscontrole — Methodiek en verantwoording

**Project:** FIS lock-validatie · Issue #58  
**Status:** afgerond (eerste controle-cyclus)  
**Grondgebied:** 49 sluiskolken in 20 sluiscomplexen, Nederlandse rijkswateren

---

## 1  Terminologie en zijde-aanduiding

| Term | Definitie |
|---|---|
| **Bo** | Bovenhoofd — instroom aan de hoge (upstream) zijde (riviersluizen) |
| **Be** | Benedenhoofd — uitstroom aan de lage (downstream) zijde (riviersluizen) |
| **Bi** | Binnenhoofd — aan de kanaalzijde (zeesluizen) |
| **Bu** | Buitenhoofd — aan de zeezijde (zeesluizen) |
| **Drempelkruin** | Hoogste punt van de kolk-drempel (bodem van de doorvaartopening), t.o.v. NAP |
| **Schutlengte** | Bruikbare kolk-lengte voor schepen |
| **Constructieve lengte** | Totale lengte van de sluiskolk inclusief muurdikte |
| **Kolkbreedte** | Inwendige breedte van de sluiskolk |
| **Deuropening** | Vrije breedte van de sluispoort |
| **OBB** | Oriented Bounding Box — minimum-oppervlakte rechthoek rondom de FIS-kolkpolygoon; gebruikt als geometrische referentie |
| **Streefpeil** | Beoogd waterpeil op de aansluitende waterweg (m NAP) |

**Bepaling Bo/Be:**  
De boven/beneden-zijde wordt per sluiscomplex vastgesteld op basis van domeinkennis (waterhuishoudkundige situatie). Dit is **niet** automatisch afleidbaar uit de FIS/aimedlevel-geometrie zonder het volledige vaarwegroutenetwerk te traceren. De toewijzing is opgeslagen in `get_waterway_levels()` in `validate_lock_dimensions.py` (regels 474-539) en geldt als leidende bron.

---

## 2  Databronnen

| Bron | Inhoud | Koppeling |
|---|---|---|
| **FIS** (vaarweginformatie.nl) | Kolk-geometrieën, afmetingen, drempeldieptes, streefpeilen | Primaire bron |
| **EURIS** (European RIS) | Europese kolkattributen (breedte, schutlengte) | Left-join op ISRS-code |
| **BIVAS** (Binnenvaart Analyse Systeem) | Netwerk-lengtes en -breedtes | Ruimtelijke koppeling (150 m buffer) |
| **Enquête / DISK** | Handmatig ingevulde technische specificaties (via GitHub issue #58) | Naam-matching |
| **RWS bodemhoogte_1mtr** | 1 m-resolutie bodemhoogte raster (NAP), RD New | WMS/REST identify-service |
| **FIS aimedlevel** | Streefpeilen langs vaarwegtrajecten | Ruimtelijke sjoin <=500 m |

---

## 3  Grootheden — werkproces per grootheid

### 3.1  Breedte

**Kolkbreedte** (inwendige breedte) en **deuropening** (poortbreedte).

```
FIS GateWidth
      |- aanwezig → gebruik FIS waarde
      `- ontbreekt → gebruik EURIS cl_width (in cm → m)

Validatie:
  • OBB-breedte (geometrisch, FIS-polygoon): gemeten via minimum_rotated_rectangle
  • Verschil FIS vs OBB enkele % → FIS als waar aannemen (inclusief muurdikte)
  • Verschil FIS vs Enquête/BIVAS < 0,5 m → bevestigd
  • Verschil > 5 % → [!] markering in rapport
```

**Beslisregel:** FIS is leidend. Geometrische OBB-meting dient ter verificatie; een systematische afwijking van enkele % is normaal omdat de FIS-polygoon ook de wanden bevat.

### 3.2  Lengte

**Constructieve lengte** (polygon-lengte) en **schutlengte** (bruikbare lengte).

```
Schutlengte:
  FIS SchutLengteEb
        |- aanwezig → gebruik FIS waarde
        `- ontbreekt → gebruik EURIS mlengthecm (in cm → m)

Validatie-hiërarchie (selectieregel, regels 2056-2072):
  1. FIS ~= Enquête (< 0,5 m verschil) → gebruik FIS; label "Overeenstemming FIS & Enquête"
  2. FIS ~= BIVAS (< 0,5 m verschil) → gebruik FIS; label "Overeenstemming FIS & BIVAS"
  3. Geen overeenstemming → [!] markering; handmatig controleren

Constructieve lengte:
  FIS Length
        `- geometrische OBB-lengte als kruiscontrole
         • verschil > 15 % → [!] markering
```

**Beslisregel:** FIS is leidend voor schutlengte; meerdere bronnen en afwijking enkele % → FIS als waar aannemen. De OBB-lengte is systematisch ~5-15% groter door wanddikte en meet-definitie-verschillen.

### 3.3  Drempelhoogte (Bo/Be en Bi/Bu)

De drempelkruin-hoogte (m NAP) wordt bepaald via een vaste prioriteitsvolgorde.

```
FIS Note-veld (meest betrouwbaar)
      |- bevat "NAP+X,XX m" of "NAP-X,XX m" → parse_note_sill_nap() → gebruik direct
      |- bevat "X,XX m+NAP" (Born-variant) → parse_note_sill_nap() → gebruik direct
      |- bevat "Drempels SP-X,XX m=NAP+Y,YY m" → gebruik NAP-waarde
      `- geen bruikbare NAP-waarde →

FIS HeightReferenceLevel = 'NAP'
      |- aanwezig → sill-waarde is al in NAP → gebruik direct
      `- ontbreekt →

FIS HeightReferenceLevel = 'KP' of 'SP' + streefpeil
      |- diepte t.o.v. KP/SP bekerd → NAP = streefpeil − drempeldiepte
      `- streefpeil niet beschikbaar →

FIS waarde < 0 (vermoedelijk NAP)
      |- gebruik met [!] (onzeker)
      `- geen bruikbare waarde → NULL
```

**Kruiscontrole — RWS bodemhoogte_1mtr:**  
Voor elke kolk worden twee drempelpunten (Bo en Be) handmatig in QGIS gepositioneerd op de correcte drempellocatie (`reference/measurements.gpkg`, laag `drempelkruin`). De bodemhoogte wordt per punt opgevraagd via de RWS MapServer identify-service (EPSG:28992). De 1m-meting dient als onafhankelijke verificatie van de FIS-waarde.

**Bekende geen-data gebieden:**
- Prinses Beatrixsluizen (alle 3 kolken): geen RWS 1m-dekking
- Krammer Noordkolk (Be-zijde): Oosterschelde, buiten RWS-beheergebied 1m-kaart

**Beslisregel:** Note-veld is meest betrouwbaar (woordelijke omschrijving van de drempelhoogte). Afwijking 1m-meting vs FIS < 0,3 m → bevestigd; > 0,5 m → nader onderzoek.

### 3.4  Boven/Beneden zijde (Bo/Be, Bi/Bu)

De zijde-toewijzing wordt **per sluiscomplex handmatig vastgesteld** op basis van de waterhuishoudkundige situatie (welke vaarweg is upstream/benedenstrooms, wat zijn de streefpeilen).

**Huidige implementatie:** `get_waterway_levels()` in `validate_lock_dimensions.py` (regels 474-539) geeft per complex:
- naam van de aansluitende waterweg aan Bo/Be zijde
- streefpeil van die waterweg (m NAP)

**Automatische bepaling is niet mogelijk** vanuit FIS/aimedlevel-geometrie alleen, omdat de aimedlevel-segmenten de kolk doorkruisen en beide poorten dezelfde vaarwegsectie vinden. Een datagedreven aanpak vereist het traceren van het FIS routenetwerk (toekomstig werk).

**Beslisregel:** de hardcoded toewijzing is de gezaghebbende bron. Aanpassing is vereist als een sluiscomplex een nieuwe operationele situatie krijgt (bijv. peilwijziging, nieuwbouw).

### 3.5  Referentiehoogtes en streefpeilen

```
Streefpeil Bo/Be:
  hardcoded per complex in get_waterway_levels() → primaire bron voor rapport

FIS AimedLevel (sjoin <=500 m op kolkcentroïde):
  → target_water_level_nap: één waarde per kolk (nearest feature)
  → gebruik: converter drempeldiepte → NAP bij HeightReferenceLevel = KP of SP

FairwayDepth.ReferenceLevel (sjoin <=500 m):
  → per vaarwegtraject: KP, SP, of NAP
  → gebruik: bepaalt welke rekenmethode voor resolve_sill_nap() geldt
```

**Verantwoording:** de aimedlevel (single-value per kolk) is te grof voor per-zijde onderscheid. De hardcoded waarden in `get_waterway_levels()` zijn correct en zijn gevalideerd tegen de bekende waterpeilen van het Nederlandse vaarwegennet.

---

## 4  OBB-as (profiel-as)

De navigatie-as door de sluiskolk wordt bepaald via de **minimum rotated rectangle** (OBB) van de FIS-kolkpolygoon:
1. Bereken MRR van de kolkpolygoon
2. Identificeer de twee korte zijden (poortzijden)
3. Verbind de middens van de twee korte zijden → navigatie-as
4. Verleng de as 15 m voorbij beide poorten

**Validatie (issue #58):** voor alle 49 kolken is de OBB-as geometrisch correct (hoek as <-> MRR lange zijde = 0°). De handmatig gecorrigeerde lijnen in `reference/measurements.gpkg` (laag `profiel_as`) zijn identiek aan de algoritmische uitvoer (Hausdorff-afstand = 0 m voor 47/49 kolken). De OBB-assen worden opnieuw berekend bij elke rapport-run; de reference-kolommen dienen als grondwaarheid voor visuele verificatie.

**Beperking:** de OBB-as werkt goed voor rechte, eenvoudige kolkpolygonen. Bij L-vormige of samengestelde polygonen kan de MRR scheef staan t.o.v. de vaarrichting.

---

## 5  Bekende beperkingen en openstaande punten

| Punt | Status |
|---|---|
| Bo/Be automatisch bepalen | Open — geen datagedreven methode gevonden |
| Prinses Beatrix: geen 1m-bathymetrie | Gedocumenteerd, NULL in measurements.gpkg |
| Krammer Noordkolk Be: geen 1m-bathymetrie | Gedocumenteerd, NULL |
| Belfeld Oostkolk Bo (eerder NoData) | Opgelost — 6.88 m NAP via ZN_zuid_oost laag |
| FIS-polygonen met voorhaven/samengesteld | Niet geïdentificeerd; OBB werkt voor alle 49 kolken |
| EURIS-koppeling voor 26 kolken niet gevonden | Inner→Left join opgelost; FIS-waarden behouden |
| Scheve OBB-lijn in measurements.gpkg | Oorsprong niet meer traceerbaar; huidige code correct |

---

## 6  Reproduceerbaarheid

```bash
# Stap 1: crawl FIS/EURIS/DISK data
uv run scrapy crawl dataservice -L INFO
uv run scrapy crawl euris -L INFO
uv run scrapy crawl disk -L INFO

# Stap 2: drempelpunten samplen (vanuit reference/measurements.gpkg)
uv run scripts/lock_validation/sample_drempel_points.py

# Stap 3: rapport genereren (Deel B)
uv run scripts/lock_validation/validate_lock_dimensions.py

# Stap 4: PDF samenstellen (Deel A + Deel B)
make lock-validation-pdf
```

**Reference-data:** `reference/measurements.gpkg` bevat de handmatig gevalideerde drempelpunten en kolk-assen (git-tracked, commit in issue #58 branch). Anders dan de overige reference-bestanden (download via `make download-reference`) is dit bestand in de repository opgeslagen.
