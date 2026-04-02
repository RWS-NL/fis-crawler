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

        # Load Zenodo reference data from local file ONLY (CI robustness)
        cls.zenodo_path = pathlib.Path("reference/unlo-geocoded-v0.1.gpkg")
        if cls.zenodo_path.exists():
            cls.zenodo_gdf = gpd.read_file(cls.zenodo_path)
        else:
            cls.zenodo_gdf = None

    def test_top_unlocodes_in_network(self):
        """Verify that top Dutch UN/LOCODEs map to nodes in the network."""
        # Fail-fast: ensure 'locode' column exists
        self.assertIn(
            "locode",
            self.nodes_gdf.columns,
            "Merged graph nodes missing 'locode' attribute",
        )

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
            self.skipTest("Zenodo reference data file not available locally.")

        # Example: Rotterdam (NLRTM)
        loc = "NLRTM"
        zenodo_match = self.zenodo_gdf[
            (self.zenodo_gdf["country_code"] == "NL")
            & (self.zenodo_gdf["location_code"] == "RTM")
        ]
        self.assertFalse(
            zenodo_match.empty, f"{loc} not found in Zenodo reference data"
        )
        zen_point = zenodo_match.geometry.iloc[0]

        # Find nearest node in network starting with NLRTM
        node_matches = self.nodes_gdf[
            self.nodes_gdf["locode"].str.startswith(loc, na=False)
        ]
        self.assertFalse(node_matches.empty, f"No nodes found in network for {loc}")

        geod = Geod(ellps="WGS84")
        distances = []

        for _, node in node_matches.iterrows():
            if (
                "x" in node
                and "y" in node
                and node["x"] is not None
                and node["y"] is not None
            ):
                _, _, dist = geod.inv(zen_point.x, zen_point.y, node["x"], node["y"])
                distances.append(dist)

        self.assertTrue(
            len(distances) > 0, f"No network nodes for {loc} have valid x/y coordinates"
        )
        min_dist = min(distances)

        # Distance should be reasonably small (e.g. within 10km for a large port area)
        self.assertLess(
            min_dist,
            10000,
            f"Nearest node for {loc} is too far from Zenodo point ({min_dist:.1f}m)",
        )


if __name__ == "__main__":
    unittest.main()
