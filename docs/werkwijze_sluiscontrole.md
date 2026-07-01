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

**Definitie.** Boven/beneden (en binnen/buiten bij zeesluizen) is een **vaste,
per-sluiscomplex toegekende naam**, gebaseerd op streefpeil en positie in het
watersysteem — **niet** een momentopname-vergelijking van actuele waterstanden:
- "Beneden"/"buiten" = de zijde die dichter bij zee ligt / de natuurlijke
  afvoerrichting volgt (rivier of getijgebied), ongeacht welke kant op een
  gegeven moment toevallig hoger staat.
- "Boven"/"binnen" = de zijde die verder van zee ligt: een hoger gereguleerd
  pand, of de kanaalzijde bij een kanaal-naar-rivier/zee-overgang.

Onderbouwing: het `Handboek voor het Ontwerpen van Schutsluizen` (RWS) hanteert
het begrip **"negatief verval"** juist om aan te geven dat de zijden een vaste
naam hebben die niet meebeweegt met de waterstand: *"Negatief verval wil zeggen
het verval dat enkele draai- of puntdeuren kan doen openen. Het treedt
bijvoorbeeld op bij extreem laag water aan de zeezijde of extreem hoog water aan
de binnenzijde van een sluis."* Als de waterstand toevallig "verkeerd om" staat,
heet dat een uitzonderingssituatie — de zijden worden niet herbenoemd.

**Eenheid streefpeil (m NAP):** bevestigd door RWS-document *"Vragen en antwoorden
over doorvaarthoogten bij bruggen op www.vaarweginformatie.nl"* (vaarweginformatie.nl,
versie april 2024): *"Voor kanalen: StreefPeil (SP): Het StreefPeil is het
gewenste peil dat wordt nagestreefd in een kanaal onder normale omstandigheden
[...] ten opzichte van NAP."* `aimedlevel.Value` in FIS komt overeen met dit
StreefPeil-begrip.

**Automatische bepaling (geïmplementeerd, `fis/lock/levels.py`):**

1. `aimedlevel` (streefpeil, m NAP) wordt op de fis-graaf-edges geprojecteerd
   (`enrich_edges_with_streefpeil()`, ruimtelijke overlap-join op RouteId/RouteKm,
   analoog aan de bestaande dataset-verrijkingen in `fis/graph/enrich_fis.py`).
2. Per sluiscomplex wordt vanaf de twee echte graafknopen die de eigen vaarweg van
   de sluis begrenzen (`start_junction_id`/`end_junction_id`, al berekend in
   `fis/lock/core.py::_resolve_fairway_data`) de graaf afgelopen
   (`walk_to_streefpeil()`) tot een edge met een streefpeil gevonden is.
3. De wandeling blijft daarbij **op de eigen `RouteId` van de sluis** — empirisch
   is gebleken dat een onbeperkte wandeling binnen 1-2 stappen een nabijgelegen
   haven of zijkanaal met een eigen, niet-gerelateerd streefpeil kan bereiken
   vóórdat de juiste pandgrens op de eigen route gevonden wordt (geconstateerd bij
   Sluis Belfeld/Sambeek). Zonder streefpeil binnen het gezochte aantal stappen op
   de eigen route → onopgelost (géén silent fallback naar een andere route).
4. Hogere gevonden streefpeil = boven, lagere = beneden. Dit resultaat wordt
   gelabeld op de al bestaande `lock_split`/`lock_merge`-knopen (sluiscomplex-
   niveau, gedeeld door alle kolken) en overgeërfd naar `chamber_start`/
   `chamber_end` (per kolk) in `fis/lock/graph.py` — `output/lock-schematization/
   nodes.geoparquet` krijgt de kolommen `side`, `streefpeil_nap`,
   `streefpeil_source`.

**Empirische validatie** (`scripts/lock_validation/cross_validate_boven_beneden.py`,
output in `output/lock-schematization/boven_beneden_cross_validation.csv`) tegen
de handmatige tabel (nu `fis.lock.levels.MANUAL_WATERWAY_LEVELS`):
- **MATCH** (zijde én waarde correct): Sluis Born, Houtribsluizen, Oranjesluizen,
  Prins Bernhardsluizen, Prinses Irenesluizen, Prinses Margrietsluis.
- **PARTIAL** (kanaalzijde correct automatisch bepaald, rivier-/getijzijde heeft
  bewust geen streefpeil — verwacht, geen fout): Sluis Belfeld, Sluis Sambeek.
- **VALUE_MISMATCH** (automatisch vindt een streefpeil één pand te ver):
  Sluis Maasbracht, Gaarkeukensluis — bekende beperking, zie §5.
- **UNRESOLVED** (verwacht: getijsluizen/rivieren zonder streefpeil, of route-
  ambiguïteit zoals Sluis Heel, Sluis Eefde): overige geteste sluizen.
- Géén enkele SIDE_MISMATCH (automatisch de verkeerde kant boven/beneden noemen)
  in de kruisvalidatie — dat was de belangrijkste faalmodus om uit te sluiten.

**Beslisregel:** de automatische bepaling (`streefpeil_source == "resolved"`) is
leidend waar beschikbaar; de handmatige tabel (`MANUAL_WATERWAY_LEVELS`) blijft
de gezaghebbende bron voor alle overige gevallen (`single_side_aimedlevel`,
`ambiguous`, `no_streefpeil_found`) en voor bekende afwijkingen (§5). Weurt/Heumen
(samenvloeiing van twee rivieren) zijn structureel geen 2-zijdig geval en blijven
volledig op de handmatige tabel steunen — zie §5.

### 3.5  Referentiehoogtes en streefpeilen

```
Streefpeil Bo/Be:
  1. automatisch bepaald via graaf-topologie (fis/lock/levels.py,
     resolve_boven_beneden) — leidend waar streefpeil_source == "resolved"
  2. handmatig, MANUAL_WATERWAY_LEVELS (voorheen get_waterway_levels()) →
     fallback/override voor rivier-, getij- en overige niet-opgeloste gevallen

FIS AimedLevel (sjoin <=500 m op kolkcentroïde):
  → target_water_level_nap: één waarde per kolk (nearest feature)
  → gebruik: converter drempeldiepte → NAP bij HeightReferenceLevel = KP of SP
  (deze centroïde-join blijft te grof voor per-zijde onderscheid — zie §3.4 voor
  de per-zijde bepaling via de graaf)

FairwayDepth.ReferenceLevel (sjoin <=500 m):
  → per vaarwegtraject: KP, SP, of NAP
  → gebruik: bepaalt welke rekenmethode voor resolve_sill_nap() geldt
```

**Verantwoording:** de aimedlevel-centroïde-join (single-value per kolk) blijft te
grof voor per-zijde onderscheid en wordt uitsluitend gebruikt voor de
KP/SP→NAP-conversie van drempelhoogtes (§3.3), niet voor boven/beneden. De
per-zijde streefpeilen komen uit de graaf-gebaseerde bepaling (§3.4) met de
handmatige tabel als gevalideerde override.

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
| Bo/Be automatisch bepalen | Deels opgelost — zie §3.4. Graaf-topologische bepaling (`fis/lock/levels.py`), gevalideerd (geen SIDE_MISMATCH) tegen `MANUAL_WATERWAY_LEVELS` in `boven_beneden_cross_validation.csv`. |
| Sluis Maasbracht, Gaarkeukensluis: automatische streefpeil één pand te ver | Open — de graafwandeling vindt op de eigen route een streefpeil dat bij het volgende pand hoort i.p.v. het aangrenzende. Waarschijnlijk ontbreekt een aimedlevel-match op de sectie(s) direct naast de sluis. Handmatige tabel blijft hier leidend (`VALUE_MISMATCH` in de kruisvalidatie). |
| Weurt/Heumen: samenvloeiing van twee rivieren (Maas + Waal) | Open — geen 2-zijdige boven/beneden-structuur; niet geautomatiseerd in deze iteratie, blijft op de handmatige tabel steunen. |
| `Bo`/`Be`-volgorde in `bathymetry.py::gate_centres()` (CCW-heuristiek) koppelen aan de nieuwe `side`-labels | Open — vervolgstap; de node-labeling (§3.4) is klaar, de consumptie in `validate_lock_dimensions.py`/`bathymetry.py` is nog niet aangepast. |
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
