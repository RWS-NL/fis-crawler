import json
import logging
import geopandas as gpd
import pickle

from fis.lock.core import group_complexes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Loading data...")
df_locks = gpd.read_parquet("output/fis-export/lock.geoparquet")
df_chambers = gpd.read_parquet("output/fis-export/chamber.geoparquet")
df_sections = gpd.read_parquet("output/fis-export/section.geoparquet")
df_berths = gpd.read_parquet("output/fis-export/berth.geoparquet")

# Load Network Graph
logger.info("Loading graph...")
with open("output/fis-graph/graph.pickle", "rb") as f:
    network_graph = pickle.load(f)

l = df_locks[df_locks["Id"] == 51064].copy()

if l.empty:
    logger.error("Lock not found!")
    exit(1)

logger.info("Running group_complexes for lock %s...", l.iloc[0]['Name'])
# Just extract for this lock
result = group_complexes(
    locks=l,
    chambers=df_chambers, 
    isrs=None,
    ris_df=None,
    fairways=None,
    berths=df_berths,
    sections=df_sections,
    network_graph=network_graph
)
logger.info("Finished group_complexes.")

if not result:
    logger.warning("No result")
    exit(1)

lock_node = result[0]
logger.info("--- LOCK 51064 OUTPUT ---")
logger.info("Berths found: %d", len(lock_node['berths']))

for b in lock_node['berths']:
    if b['id'] == 27256967:
        logger.info("✅ Found Berth 27256967 (Relation: %s, Dist: %sm)", b['relation'], b['dist_m'])
        exit(0)

logger.error("❌ Berth 27256967 NOT FOUND!")
exit(1)
