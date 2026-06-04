# Gold Standard Topological Paths for Selected Lock Complexes

This document defines the reference (gold-standard) topological node sequences, structure associations, and berth mappings for three complex lock structures in the network. These paths serve as validation criteria for the Stage 4 (Lock Complex Integration) and Stage 5 (Fairway Splicing) pipeline implementation.

---

## 1. Sluis Weurt (Lock 49032)
* **Topology Pattern**: Serial (Multi-section / staggered chambers with embedded bridge)
* **Entry Node**: `FIS_8864666` (South/East approach)
* **Exit Node**: `FIS_8865102` (North/West approach)

### Expected Sequence of Nodes (South to North)
1. **Approach Junction**: `FIS_8864666`
2. **Lock Split**: `lock_49032_split`
3. **West Chamber Lane**:
   - Chamber Start: `chamber_40927_start`
   - Internal Lock Node: `FIS_8864190`
   - Chamber End: `chamber_40927_end`
4. **East Chamber Lane (Staggered)**:
   - Chamber Start: `chamber_47538_start`
   - Embedded Bridge Opening 5835 (Bridge 24080):
     - Bridge Node Start: `opening_5835_start`
     - Bridge Node End: `opening_5835_end`
   - Chamber End: `chamber_47538_end`
5. **Lock Merge**: `lock_49032_merge`
6. **Exit Junction**: `FIS_8865102`

---

## 2. Volkeraksluizen (Lock 42863 & 52078)
* **Topology Pattern**: Parallel (2 main complexes: Commercial locks + Jachtensluis)
* **Common Entry Node**: `FIS_8860743` (North)
* **Common Exit Node**: `FIS_8866727` (South)

### Commercial Lock Complex (Lock 42863)
* **Chambers**: 
  - Westkolk (`chamber_6428`)
  - Oostkolk (`chamber_7083`)
  - Middenkolk (`chamber_24817`)
* **Bridges**: Embedded Bridge openings (43247 -> 6428, 9802 -> 24817, 39854 -> 7083)
* **Berths**: 11 wait berths (e.g., `54137`, `48871`, `38895`, `18167`, `23764` after; and `19631546`, `19631549`, `19631552`, `34400937`, `19631543`, `51756` before)

### Jachtensluis (Lock 52078)
* **Chambers**: Sluiskolk Jachtensluis (`chamber_18373`)
* **Bridges**: Associated Bridge opening 9689

### Expected Topological Flow
Both the Commercial Complex and the Jachtensluis must converge to the same external boundary nodes to ensure valid undirected routing.
- **Divergence Point**: `FIS_8860743` splits into commercial approach and yacht approach paths.
- **Commercial Path**:
  - Commercial Split: `lock_42863_split`
  - West Lane: `chamber_6428_start` → `opening_43247_start` → `opening_43247_end` → `chamber_6428_end`
  - Midden Lane: `chamber_24817_start` → `opening_9802_start` → `opening_9802_end` → `chamber_24817_end`
  - Oost Lane: `chamber_7083_start` → `opening_39854_start` → `opening_39854_end` → `chamber_7083_end`
  - Commercial Merge: `lock_42863_merge`
- **Jachtensluis Path**:
  - Jachtensluis Split: `lock_52078_split`
  - Lane: `chamber_18373_start` → `opening_9689_start` → `opening_9689_end` → `chamber_18373_end`
  - Jachtensluis Merge: `lock_52078_merge`
- **Convergence Point**: Both paths merge back at `FIS_8866727`.

---

## 3. Oranjesluizen (Lock 50750 & 16178)
* **Topology Pattern**: Parallel (2 main branches: Prins Willem-Alexandersluis and Oranjesluizen)
* **Common Entry Node**: `FIS_8864384` (East/South)
* **Common Exit Node**: `FIS_59275858` (West/North)

### Expected Sequence of Nodes (East to West)
1. **Divergence Node (Junction)**: `FIS_8864384`
2. **Left Branch (Prins Willem-Alexandersluis)**:
   - Chamber: `chamber_11446` (Prins Willem-Alexandersluis)
   - Chamber nodes: `chamber_11446_start` → `chamber_11446_end`
3. **Right Branch (Oranjesluizen - Lock 50750)**:
   - Split: `lock_50750_split`
   - Zuidkolk: `chamber_3127_start` → `chamber_3127_end`
   - Middenkolk: `chamber_55419_start` → `chamber_55419_end`
   - Noordkolk: `chamber_21002_start` → `chamber_21002_end`
   - Merge: `lock_50750_merge`
4. **Convergence Node (Junction)**: `FIS_59275858`
