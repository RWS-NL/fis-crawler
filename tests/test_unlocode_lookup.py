import unittest
import pickle
import pathlib
import geopandas as gpd
from pyproj import Geod


class TestUnlocodeLookup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        graph_path = pathlib.Path("output/merged-graph/graph.pickle")
        if not graph_path.exists():
            raise unittest.SkipTest(
                "Graph pickle not found. Run the full pipeline first."
            )

        with open(graph_path, "rb") as f:
            cls.graph = pickle.load(f)

        cls.nodes_gdf = gpd.GeoDataFrame(
            cls.graph.nodes.values(), index=cls.graph.nodes.keys()
        )

        # Load Zenodo reference data (fetch if not present)
        cls.zenodo_path = pathlib.Path("reference/unlo-geocoded-v0.1.gpkg")
        zenodo_url = "https://zenodo.org/records/11191511/files/unlo-geocoded-v0.1.gpkg?download=1"

        if not cls.zenodo_path.exists():
            try:
                # We don't want to force a 28MB download during every test run if not present
                # but for this specific validation we try to read it directly or skip
                cls.zenodo_gdf = gpd.read_file(zenodo_url)
            except Exception:
                cls.zenodo_gdf = None
        else:
            cls.zenodo_gdf = gpd.read_file(cls.zenodo_path)

    def test_top_unlocodes_in_network(self):
        """Verify that top Dutch UN/LOCODEs map to nodes in the network."""
        top_unlocodes = ["NLAMS", "NLRTM", "NLDOR", "NLUTC"]

        for loc in top_unlocodes:
            matches = self.nodes_gdf[
                self.nodes_gdf["locode"].str.startswith(loc, na=False)
            ]
            self.assertFalse(matches.empty, f"No nodes found for {loc}")
            self.assertGreater(len(matches), 0)

    def test_zenodo_proximity(self):
        """Verify that network nodes for a UN/LOCODE are near the Zenodo geocoded point."""
        if self.zenodo_gdf is None:
            self.skipTest(
                "Zenodo reference data not available (URL inaccessible or file missing)."
            )

        # Example: Rotterdam (NLRTM)
        loc = "NLRTM"
        zenodo_match = self.zenodo_gdf[
            (self.zenodo_gdf["country_code"] == "NL")
            & (self.zenodo_gdf["location_code"] == "RTM")
        ]
        self.assertFalse(zenodo_match.empty)
        zen_point = zenodo_match.geometry.iloc[0]

        # Find nearest node in network starting with NLRTM
        node_matches = self.nodes_gdf[
            self.nodes_gdf["locode"].str.startswith(loc, na=False)
        ]

        geod = Geod(ellps="WGS84")
        min_dist = float("inf")

        for _, node in node_matches.iterrows():
            if "x" in node and "y" in node:
                _, _, dist = geod.inv(zen_point.x, zen_point.y, node["x"], node["y"])
                min_dist = min(min_dist, dist)

        # Distance should be reasonably small (e.g. within 10km for a large port area)
        self.assertLess(
            min_dist,
            10000,
            f"Nearest node for {loc} is too far from Zenodo point ({min_dist:.1f}m)",
        )


if __name__ == "__main__":
    unittest.main()
