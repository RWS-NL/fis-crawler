import logging
import pathlib
import json
import math
import pickle
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx
from scipy.spatial import KDTree
from shapely import wkt
from shapely.geometry import Point, LineString, shape
from collections import defaultdict
from tqdm import tqdm
from dask.distributed import Client, LocalCluster

from fis import utils

logger = logging.getLogger("fis.ivs.assign")

ZENODO_URL = "https://zenodo.org/records/11191511/files/unlo-geocoded-v0.1.gpkg?download=1"

def is_valid(val):
    if val is None:
        return False
    if isinstance(val, float) and math.isnan(val):
        return False
    if str(val) == "nan":
        return False
    return True

def download_zenodo_data(reference_dir: pathlib.Path) -> pathlib.Path:
    """Downloads Zenodo geocoded UN/LOCODE database if not present locally."""
    reference_dir.mkdir(parents=True, exist_ok=True)
    local_path = reference_dir / "unlo-geocoded-v0.1.gpkg"
    if not local_path.exists():
        logger.info(f"Downloading Zenodo reference UN/LOCODE data from {ZENODO_URL} to {local_path}...")
        r = requests.get(ZENODO_URL, timeout=120)
        r.raise_for_status()
        local_path.write_bytes(r.content)
        logger.info("Zenodo data downloaded successfully.")
    return local_path

def load_shiptypes(reference_dir: pathlib.Path) -> dict:
    """Loads and indexes the DTV ship types database."""
    dtv_path = reference_dir / "DTV_shiptypes_database.json"
    if not dtv_path.exists():
        logger.info(f"DTV shiptypes database not found at {dtv_path}. Downloading...")
        url = "https://raw.githubusercontent.com/SiggyF/digitaltwin-waterway/refs/heads/master/dtv_backend/data/DTV_shiptypes_database.json"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        dtv_path.parent.mkdir(parents=True, exist_ok=True)
        dtv_path.write_text(r.text, encoding="utf-8")
        
    with open(dtv_path, "r", encoding="utf-8") as f:
        ships = json.load(f)
        
    indexed = {}
    for ship in ships:
        rws_class = ship.get("RWS-class")
        if rws_class:
            indexed[rws_class.upper().strip()] = ship
            
    return indexed

def normalize_class_code(code: str) -> str:
    """Normalizes the IVS ship class code to match the DTV class naming convention."""
    if not isinstance(code, str):
        return ""
    code = code.upper().strip()
    if code.startswith("B0"):
        code = "BO" + code[2:]
    return code

def get_ship_dimensions(sk_code: str, cargo_weight: float, capacity: float, dtv_db: dict) -> dict:
    """Computes dynamic vessel dimensions (including loaded/empty draft interpolation)."""
    normalized = normalize_class_code(sk_code)
    ship = dtv_db.get(normalized)
    
    if not ship:
        return {
            "beam": 11.4,
            "length": 110.0,
            "height": 6.0,
            "draft": 2.5
        }
        
    beam = float(ship.get("Beam [m]", 11.4))
    length = float(ship.get("Length [m]", 110.0))
    
    h_avg = ship.get("Height average [m]")
    height = float(h_avg) if is_valid(h_avg) else 6.0
    
    d_loaded = ship.get("Draught loaded [m]")
    d_empty = ship.get("Draught empty [m]")
    d_avg = ship.get("Draught average [m]")
    
    loaded_draft = float(d_loaded) if is_valid(d_loaded) else (float(d_avg) if is_valid(d_avg) else 3.5)
    empty_draft = float(d_empty) if is_valid(d_empty) else (0.4 * loaded_draft)
    
    if capacity > 0 and cargo_weight > 0:
        ratio = cargo_weight / capacity
        ratio = max(0.0, min(1.0, ratio))
        draft = empty_draft + ratio * (loaded_draft - empty_draft)
    else:
        draft = empty_draft
        
    return {
        "beam": beam,
        "length": length,
        "height": height,
        "draft": draft
    }

def get_edge_key(d: dict) -> str:
    """Extracts the lookup ID key from a merged graph edge."""
    val = d.get("fis_id") or d.get("code")
    return str(val) if is_valid(val) else None

def build_edge_structures_lookup() -> dict:
    """
    Builds a unified lookup mapping FIS fairway section IDs and EURIS fairway section codes
    to their respective lock chambers and bridge openings from the detailed databases.
    """
    lookup = defaultdict(lambda: {"chambers": [], "openings": []})
    
    fis_ch_path = pathlib.Path("output/dropins-fis-detailed/chambers.geoparquet")
    if fis_ch_path.exists():
        df_fis_ch = pd.read_parquet(fis_ch_path)
        for _, r in df_fis_ch.iterrows():
            sid = r.get("fairway_section_id")
            if is_valid(sid):
                lookup[str(sid)]["chambers"].append(r.to_dict())
                
    fis_op_path = pathlib.Path("output/dropins-fis-detailed/openings.geoparquet")
    if fis_op_path.exists():
        df_fis_op = pd.read_parquet(fis_op_path)
        for _, r in df_fis_op.iterrows():
            sid = r.get("fairway_section_id")
            if is_valid(sid):
                lookup[str(sid)]["openings"].append(r.to_dict())
                
    eur_edges_path = pathlib.Path("output/dropins-euris-detailed/edges.geoparquet")
    eur_ch_path = pathlib.Path("output/dropins-euris-detailed/chambers.geoparquet")
    eur_op_path = pathlib.Path("output/dropins-euris-detailed/openings.geoparquet")
    
    if eur_edges_path.exists() and eur_ch_path.exists() and eur_op_path.exists():
        df_eur_edges = pd.read_parquet(eur_edges_path)
        euris_sec_locks = defaultdict(set)
        euris_sec_bridges = defaultdict(set)
        for _, r in df_eur_edges.iterrows():
            sid = r.get("section_id")
            lid = r.get("lock_id")
            bid = r.get("bridge_id")
            if is_valid(sid):
                if is_valid(lid):
                    euris_sec_locks[str(sid)].add(str(lid))
                if is_valid(bid):
                    euris_sec_bridges[str(sid)].add(str(bid))
                    
        df_eur_ch = pd.read_parquet(eur_ch_path)
        euris_ch_by_lock = defaultdict(list)
        for _, r in df_eur_ch.iterrows():
            lid = r.get("lock_id")
            if is_valid(lid):
                euris_ch_by_lock[str(lid)].append(r.to_dict())
                
        df_eur_op = pd.read_parquet(eur_op_path)
        euris_op_by_bridge = defaultdict(list)
        for _, r in df_eur_op.iterrows():
            bid = r.get("bridge_id")
            if is_valid(bid):
                euris_op_by_bridge[str(bid)].append(r.to_dict())
                
        for sid, lids in euris_sec_locks.items():
            for lid in lids:
                lookup[sid]["chambers"].extend(euris_ch_by_lock[lid])
                
        for sid, bids in euris_sec_bridges.items():
            for bid in bids:
                lookup[sid]["openings"].extend(euris_op_by_bridge[bid])
                
    return lookup

def check_edge_constraints_soft(d_edge: dict, struct_data: dict, ship_dims: dict):
    """
    Evaluates physical constraints of the edge and outputs heuristic penalties and violations.
    """
    penalties = 0.0
    violations = []
    
    chambers = struct_data.get("chambers", [])
    openings = struct_data.get("openings", [])
    
    min_width = 999.0
    limiting_struct = None
    
    for ch in chambers:
        w = ch.get("dim_gate_width", ch.get("width"))
        if is_valid(w) and float(w) < min_width:
            min_width = float(w)
            limiting_struct = (ch.get("id"), ch.get("name", "Lock Chamber"))
            
    for op in openings:
        w = op.get("dim_structural_width", op.get("width_convoy", op.get("width")))
        if is_valid(w) and float(w) < min_width:
            min_width = float(w)
            limiting_struct = (op.get("id"), op.get("name", "Bridge Opening"))
            
    edge_w = d_edge.get("dim_structural_width", d_edge.get("mwidthcm"))
    if is_valid(edge_w):
        edge_w = float(edge_w) / 100.0 if "mwidthcm" in d_edge else float(edge_w)
        if edge_w < min_width:
            min_width = edge_w
            limiting_struct = (get_edge_key(d_edge), d_edge.get("name", "Fairway"))
            
    if ship_dims["beam"] > min_width:
        penalties += 1000.0
        violations.append({
            "type": "beam",
            "ship_value": ship_dims["beam"],
            "limit_value": min_width,
            "struct_id": limiting_struct[0] if limiting_struct else None,
            "struct_name": limiting_struct[1] if limiting_struct else None
        })
        
    min_len = 999.0
    limiting_ch = None
    for ch in chambers:
        l = ch.get("dim_usable_length", ch.get("length"))
        if is_valid(l) and float(l) < min_len:
            min_len = float(l)
            limiting_ch = (ch.get("id"), ch.get("name", "Lock Chamber"))
            
    if limiting_ch and ship_dims["length"] > min_len:
        penalties += 1000.0
        violations.append({
            "type": "length",
            "ship_value": ship_dims["length"],
            "limit_value": min_len,
            "struct_id": limiting_ch[0],
            "struct_name": limiting_ch[1]
        })
        
    min_height = 999.0
    limiting_op = None
    for op in openings:
        b_type = op.get("type")
        h_closed = op.get("height_closed", op.get("dim_height", op.get("height")))
        h_opened = op.get("height_opened", op.get("clearance_height_opened"))
        
        is_fixed = (b_type == "VST") or (not is_valid(h_opened) and b_type not in ("BB", "BEW", "DBC", "OPH", "ROL", "KLP", "DDR"))
        if is_fixed:
            if is_valid(h_closed) and float(h_closed) < min_height:
                min_height = float(h_closed)
                limiting_op = (op.get("id"), op.get("name", "Bridge Opening"))
        else:
            if b_type == "HEF" and is_valid(h_opened) and float(h_opened) < min_height:
                min_height = float(h_opened)
                limiting_op = (op.get("id"), op.get("name", "Vertical Lift Bridge"))
                
    edge_h = d_edge.get("dim_height")
    if is_valid(edge_h) and float(edge_h) < min_height:
        min_height = float(edge_h)
        limiting_op = (get_edge_key(d_edge), d_edge.get("name", "Fairway"))
        
    if ship_dims["height"] > min_height:
        penalties += 1000.0
        violations.append({
            "type": "height",
            "ship_value": ship_dims["height"],
            "limit_value": min_height,
            "struct_id": limiting_op[0] if limiting_op else None,
            "struct_name": limiting_op[1] if limiting_op else None
        })
        
    min_depth = 999.0
    limiting_depth = None
    for ch in chambers:
        d = ch.get("sill_depth", ch.get("sill_depth_bo_bi", ch.get("sill_depth_be_bu")))
        if is_valid(d) and abs(float(d)) < min_depth:
            min_depth = abs(float(d))
            limiting_depth = (ch.get("id"), ch.get("name", "Lock Sill"))
            
    for df in ["dim_depth", "mindepth_lower", "tidedep"]:
        if is_valid(d_edge.get(df)) and abs(float(d_edge[df])) < min_depth:
            min_depth = abs(float(d_edge[df]))
            limiting_depth = (get_edge_key(d_edge), d_edge.get("name", "Fairway Depth"))
            
    if min_depth < 999.0 and (ship_dims["draft"] + 0.2) > min_depth:
        penalties += 1000.0
        violations.append({
            "type": "depth",
            "ship_value": ship_dims["draft"] + 0.2,
            "limit_value": min_depth,
            "struct_id": limiting_depth[0] if limiting_depth else None,
            "struct_name": limiting_depth[1] if limiting_depth else None
        })
        
    # 5. Sea route check
    water_name = str(d_edge.get("water_name", "")).strip().lower()
    if "noordzee" in water_name or "sea" in water_name:
        penalties += 100000.0
        violations.append({
            "type": "sea",
            "ship_value": 1.0,
            "limit_value": 0.0,
            "struct_id": get_edge_key(d_edge),
            "struct_name": d_edge.get("name", "Sea Route")
        })
        
    return penalties, violations

def get_edge_weight_soft(d_edge: dict, struct_data: dict, ship_dims: dict) -> float:
    """Calculates Dijkstra cost: travel time + lock delays + soft penalties."""
    length_km = d_edge.get("length_m", d_edge.get("length", 1.0)) / 1000.0
    
    speed_km_h = 10.0
    for k in ["maxspeed_up", "maxspeed_down", "maxspeed", "calspeed_up", "calspeed_down"]:
        if is_valid(d_edge.get(k)):
            try:
                if isinstance(d_edge[k], str):
                    import re
                    m = re.search(r"[-+]?\d*\.\d+|\d+", d_edge[k])
                    if m:
                        val = float(m.group(0))
                        if val > 0.0:
                            speed_km_h = val
                            break
                else:
                    val = float(d_edge[k])
                    if val > 0.0:
                        speed_km_h = val
                        break
            except ValueError:
                pass
                
    speed_km_h = max(1.0, speed_km_h)
    travel_time = length_km / speed_km_h
    
    lock_delay = 0.0
    chambers = struct_data.get("chambers", [])
    for ch in chambers:
        passage_m = ch.get("passage_duration_m", 30.0)
        if not is_valid(passage_m):
            passage_m = 30.0
        lock_delay += float(passage_m) / 60.0
        
    penalties, _ = check_edge_constraints_soft(d_edge, struct_data, ship_dims)
    return travel_time + lock_delay + penalties

def route_batch_voyages_dask(
    batch_df: pd.DataFrame,
    dtv_db: dict,
    lookup: dict,
    G_merged: nx.Graph
) -> list:
    """Task executed on a Dask worker to route a batch of voyage groups locally."""
    batch_results = []
    
    for _, row in batch_df.iterrows():
        o_node = row["o_node"]
        d_node = row["d_node"]
        
        if not o_node or not d_node:
            batch_results.append({"status": "geocode_fail"})
            continue
            
        avg_cargo = (row["cargo_weight"] / 1000.0) / row["trips"]
        ship_dims = get_ship_dimensions(row["sk_code"], avg_cargo, row["vessel_capacity"], dtv_db)
        
        def weight_func(u, v, d):
            edge_id = get_edge_key(d)
            struct_data = lookup.get(edge_id, {"chambers": [], "openings": []})
            return get_edge_weight_soft(d, struct_data, ship_dims)
            
        try:
            path = nx.shortest_path(G_merged, o_node, d_node, weight=weight_func)
            
            edges_traversed = []
            violations_log = []
            
            for u, v in zip(path[:-1], path[1:]):
                d = G_merged[u][v]
                edge_id = get_edge_key(d)
                if edge_id:
                    edges_traversed.append(edge_id)
                    struct_data = lookup.get(edge_id, {"chambers": [], "openings": []})
                    _, violations = check_edge_constraints_soft(d, struct_data, ship_dims)
                    for viol in violations:
                        violations_log.append({
                            "edge_id": edge_id,
                            "type": viol["type"],
                            "ship_class": row["sk_code"],
                            "ship_value": viol["ship_value"],
                            "limit_value": viol["limit_value"],
                            "struct_id": viol["struct_id"],
                            "struct_name": viol["struct_name"],
                            "trips": int(row["trips"]),
                            "cargo_weight": float(row["cargo_weight"])
                        })
                        
            batch_results.append({
                "status": "success",
                "path": edges_traversed,
                "violations": violations_log,
                "trips": int(row["trips"]),
                "cargo_weight": float(row["cargo_weight"])
            })
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            batch_results.append({"status": "no_path"})
            
    return batch_results

def assign_traffic(
    graph_path: pathlib.Path,
    base_graph: pathlib.Path,
    ivs_dir: pathlib.Path,
    output_dir: pathlib.Path,
    year: int,
    month: int
):
    """Assigns IVS voyages using Dask LocalCluster with optimized batch routing."""
    output_dir.mkdir(parents=True, exist_ok=True)
    reference_dir = pathlib.Path("reference")
    
    # 1. Load Reference Data
    zenodo_path = download_zenodo_data(reference_dir)
    zenodo_gdf = gpd.read_file(zenodo_path)
    zenodo_gdf["key"] = zenodo_gdf["country_code"] + zenodo_gdf["location_code"]
    zenodo_coords = {row["key"]: row["geometry"] for _, row in zenodo_gdf.iterrows() if row["geometry"]}
    
    dtv_db = load_shiptypes(reference_dir)
    
    # 2. Load merged graph
    logger.info(f"Loading base merged graph from {base_graph}...")
    with open(base_graph, "rb") as f:
        G_merged = pickle.load(f)
        
    # 3. Cache structures
    logger.info("Caching lock/bridge structures lookup...")
    lookup = build_edge_structures_lookup()
    
    # 4. Spatial index for geocoding snaps
    node_list = []
    for n_id, n_data in G_merged.nodes(data=True):
        d = dict(n_data)
        d["node_id_key"] = n_id
        node_list.append(d)
        
    nodes_gdf = gpd.GeoDataFrame(
        node_list, index=[d["node_id_key"] for d in node_list], geometry="geometry", crs="EPSG:4326"
    )
    nodes_gdf = nodes_gdf[nodes_gdf.geometry.notnull() & ~nodes_gdf.geometry.is_empty]
    node_ids = list(nodes_gdf.index)
    coords = np.array([(p.x, p.y) for p in nodes_gdf.geometry])
    tree = KDTree(coords)
    
    node_locode_map = defaultdict(list)
    for n_id, n_data in G_merged.nodes(data=True):
        loc = n_data.get("locode")
        if is_valid(loc):
            node_locode_map[str(loc).strip().upper()].append(n_id)
            
    def geocode_unlocode(unlocode: str):
        unlocode = str(unlocode).strip().upper()
        if unlocode in node_locode_map:
            return node_locode_map[unlocode][0]
        geom = zenodo_coords.get(unlocode)
        if geom:
            dist, idx = tree.query((geom.x, geom.y))
            return node_ids[idx]
        return None
        
    # 5. Load and geocode IVS voyages
    ivs_file = ivs_dir / f"year={year}" / f"month={month}" / "part.0.parquet"
    if not ivs_file.exists():
        fallback_files = list(ivs_dir.glob(f"year={year}/month={month}/*.parquet"))
        if not fallback_files:
            raise FileNotFoundError(f"IVS data not found at {ivs_file}")
        ivs_file = fallback_files[0]
        
    logger.info(f"Reading IVS dataset from {ivs_file}...")
    df = pd.read_parquet(ivs_file)
    df = df.dropna(subset=["unlo_herkomst", "unlo_bestemming", "sk_code"])
    
    voyage_groups = (
        df.groupby(["unlo_herkomst", "unlo_bestemming", "sk_code"])
        .agg(
            cargo_weight=("v38_vervoerd_gewicht", "sum"),
            vessel_capacity=("v18_laadvermogen", "mean"),
            trips=("v05_06_begindt_evenement", "count")
        )
        .reset_index()
    )
    
    logger.info("Geocoding voyage origins and destinations on main thread...")
    o_nodes = []
    d_nodes = []
    for _, row in voyage_groups.iterrows():
        o_nodes.append(geocode_unlocode(row["unlo_herkomst"]))
        d_nodes.append(geocode_unlocode(row["unlo_bestemming"]))
        
    voyage_groups["o_node"] = o_nodes
    voyage_groups["d_node"] = d_nodes
    
    # Partition DataFrame into chunks of 1000 for Dask task grouping
    chunk_size = 1000
    chunks = [voyage_groups.iloc[i : i + chunk_size].copy() for i in range(0, len(voyage_groups), chunk_size)]
    
    # 6. Dask cluster initialization and scatter
    logger.info("Initializing Dask LocalCluster...")
    cluster = LocalCluster(n_workers=4, threads_per_worker=1)
    client = Client(cluster)
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")
    
    logger.info("Scattering large database variables to workers...")
    [dtv_db_future, lookup_future, G_merged_future] = client.scatter(
        [dtv_db, lookup, G_merged], broadcast=True
    )
    
    logger.info(f"Submitting {len(chunks)} batch routing tasks to Dask...")
    futures = []
    for chunk in chunks:
        futures.append(
            client.submit(
                route_batch_voyages_dask,
                chunk,
                dtv_db_future,
                lookup_future,
                G_merged_future
            )
        )
        
    logger.info("Routing batches in parallel...")
    batch_results = client.gather(futures)
    
    client.close()
    cluster.close()
    
    # Flatten batch results list
    results = [res for batch in batch_results for res in batch]
    
    # 7. Aggregate results
    edge_cargo = defaultdict(float)
    edge_trips = defaultdict(int)
    violations_tracker = defaultdict(list)
    
    success_count = 0
    no_path_count = 0
    geocode_fail_count = 0
    
    for res in results:
        status = res["status"]
        if status == "success":
            success_count += 1
            for edge_id in res["path"]:
                edge_cargo[edge_id] += float(res["cargo_weight"])
                edge_trips[edge_id] += int(res["trips"])
                
            for viol in res["violations"]:
                viol_key = (viol["edge_id"], viol["type"])
                violations_tracker[viol_key].append(viol)
        elif status == "geocode_fail":
            geocode_fail_count += 1
        else:
            no_path_count += 1
            
    logger.info(f"Dask parallel batch routing complete. Success: {success_count}, No Path: {no_path_count}, Geocode Fail: {geocode_fail_count}")
    
    # 8. Export Intensity Network
    intensity_data = []
    for u, v, d in G_merged.edges(data=True):
        edge_id = get_edge_key(d)
        if edge_id and (edge_id in edge_cargo or edge_id in edge_trips):
            intensity_data.append({
                "id": edge_id,
                "data_source": d.get("data_source", "unknown"),
                "name": d.get("name", "Unnamed"),
                "cargo_weight_kg": edge_cargo[edge_id],
                "trip_count": edge_trips[edge_id],
                "geometry": d["geometry"]
            })
            
    if intensity_data:
        intensity_gdf = gpd.GeoDataFrame(intensity_data, geometry="geometry", crs="EPSG:4326")
        intensity_gdf.to_file(output_dir / "intensity.geojson", driver="GeoJSON")
        intensity_gdf.to_parquet(output_dir / "intensity.geoparquet")
        
    # 9. Export Penalized Edges layer
    penalized_data = []
    for (edge_id, v_type), records in violations_tracker.items():
        edge_geom = None
        edge_name = "Unknown"
        edge_source = "Unknown"
        for u, v, d in G_merged.edges(data=True):
            if get_edge_key(d) == edge_id:
                edge_geom = d["geometry"]
                edge_name = d.get("name", "Unnamed")
                edge_source = d.get("data_source", "unknown")
                break
                
        if edge_geom:
            tot_trips = sum(r["trips"] for r in records)
            tot_cargo = sum(r["cargo_weight"] for r in records)
            worst_r = max(records, key=lambda x: x["trips"])
            
            penalized_data.append({
                "edge_id": edge_id,
                "name": edge_name,
                "data_source": edge_source,
                "violation_type": v_type,
                "ship_class": worst_r["ship_class"],
                "ship_value": worst_r["ship_value"],
                "limit_value": worst_r["limit_value"],
                "struct_id": worst_r["struct_id"],
                "struct_name": worst_r["struct_name"],
                "trips": tot_trips,
                "cargo_weight_kg": tot_cargo,
                "geometry": edge_geom
            })
            
    gpkg_path = output_dir / "routing_detailed_analysis.gpkg"
    if penalized_data:
        penalized_gdf = gpd.GeoDataFrame(penalized_data, geometry="geometry", crs="EPSG:4326")
        penalized_gdf.to_file(gpkg_path, layer="penalized_edges", driver="GPKG")
        logger.info(f"Saved penalized edges debug layer to {gpkg_path}.")
        
    if intensity_data:
        intensity_gdf.to_file(gpkg_path, layer="connected_routes", driver="GPKG")
        
    logger.info("Traffic assignment complete!")
