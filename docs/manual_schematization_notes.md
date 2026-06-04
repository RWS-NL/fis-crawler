# Manual Schematization & Topology Notes for Complex Locks

This document records the user-verified manual schematization details, coordinates, bridge-chamber mappings, and special navigation structures (such as weirs) extracted from QGIS inspection. These rules guide Stage 4 (Lock Complex Integration) and Stage 5 (Fairway Splicing).

---

## 1. Volkeraksluizen (Lock 42863 & 52078)
* **Status**: Confirmed
* **Boundary Nodes**: `FIS_8860743` (North) and `FIS_8866727` (South).
* **Topology**: These nodes correctly enclose the split/merge boundary containing both the professional locks (commercial) and the recreational lock (Jachtensluis).

---

## 2. Oranjesluizen
* **Bridge Placement**: Bridge openings are located on the approach edge between `FIS_30985116` and `FIS_59275858`.
* **Key Bridge**: Bridge opening **`53836`** serves as the entrance to the lock complex and must be explicitly included in the integration graph, despite automatic spatial proximity logic sometimes missing it.

---

## 3. Sluis Weurt (Lock 49032)
* **Bridge-Chamber Mappings**:
  * **Bridge Opening `5835`** corresponds to **Chamber `47538`** (Oostkolk).
  * **Bridge Opening `25111`** corresponds to **Chamber `40927`** (Westkolk).
* **Edge Splitting & Routing**:
  * The edge from `FIS_8865102` to `FIS_8864190` must be split into two parallel approach routes at coordinate `(51.85576807, 5.82172072)` (North split point).
  * **West Route**: Diverges to Bridge Opening `25111` and enters Westkolk.
  * **East Route**: Diverges to Eastkolk (`chamber_47538`), passes through Bridge Opening `5835`, and connects to internal node `FIS_8864190`.
  * Both lanes converge back to the shared fairway at coordinate `(51.85064351, 5.82029552)` (South merge point).

---

## 4. Lorentzsluizen
* **Boundary Nodes**: Verified correct.
* **Chamber-Bridge Routing**:
  * **Chamber `15772`**: Route must pass sequentially through bridge openings **`40168`** and then **`20927`**.
  * **Chamber `28501`**: Route must pass sequentially through bridge openings **`53890`** and then **`44027`**.
* **Splits & Waypoints**:
  * **North Split Point**: Coordinate `(53.07508413, 5.33457122)` before the bridge openings.
  * **South Merge Point**: Coordinate `(53.06465534, 5.33916480)`.
  * **Chamber `28501` Waypoint**: Passage must route through waypoint `(53.06604214, 5.33901687)`.

---

## 5. Sluis Grave & Exceptional Structures (Weirs)
* **Complex Structure**: This site contains both a lock and a weir.
* **Weir Data Source**: We need to parse and load the `exceptionalnavigationalstructure` dataset to capture weirs.
* **Weir Grave (Id: 46058)**:
  * Location: `POINT (5.73579871342707 51.7687924650035)`
  * FairwaySectionId: `22638175`
  * StructureType: `STW` (Stuw / Weir)
  * Note: "De stuw is geen doorgang tbv de scheepvaart!" (The weir is not a passage for shipping, unless it is fully open in exceptional water-level conditions).
