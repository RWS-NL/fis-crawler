import unittest
import networkx as nx
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from fis.graph.enrich_fis import integrate_harbours


class TestHarbourIntegration(unittest.TestCase):
    def test_integrate_harbours_explicit_link(self):
        # Build simple graph with two junction nodes
        G = nx.Graph()
        G.add_node(100, geometry=Point(5.0, 52.0))
        G.add_node(200, geometry=Point(6.0, 53.0))

        # Mock vinharbour dataset
        harbour_df = pd.DataFrame(
            [
                {
                    "Id": 15,
                    "Name": "Test Harbour",
                    "Code": "NLTEST0001",
                    "UnLocationCode": "NLTE1",
                    "VinCode": "123",
                    "City": "TestCity",
                    "geometry": Point(5.01, 52.01),
                }
            ]
        )
        harbour_gdf = gpd.GeoDataFrame(harbour_df, geometry="geometry", crs="EPSG:4326")

        # Mock routejunction mapping harbour's Code to node 100
        rj_df = pd.DataFrame([{"Code": "NLTEST0001", "SectionJunctionId": 100}])
        rj_gdf = gpd.GeoDataFrame(rj_df)

        datasets = {"vinharbour": harbour_gdf, "routejunction": rj_gdf}

        # Run integrate
        G_enriched = integrate_harbours(G, datasets)

        # Assert harbour node was added
        self.assertTrue(G_enriched.has_node("harbour_15"))
        h_data = G_enriched.nodes["harbour_15"]
        self.assertEqual(h_data["name"], "Test Harbour")
        self.assertEqual(h_data["locode"], "NLTE1")
        self.assertEqual(h_data["node_type"], "harbour")
        self.assertEqual(h_data["isrs_id"], "NLTEST0001")

        # Assert explicit link edge was added to node 100
        self.assertTrue(G_enriched.has_edge("harbour_15", 100))
        edge_data = G_enriched.edges["harbour_15", 100]
        self.assertEqual(edge_data["segment_type"], "harbour_access")
        self.assertEqual(edge_data["data_source"], "vinharbour")

    def test_integrate_harbours_fallback_snap(self):
        # Build simple graph with two nodes
        G = nx.Graph()
        # Node 100 is closer to (5.0, 52.0)
        G.add_node(100, geometry=Point(5.0, 52.0))
        # Node 200 is further away
        G.add_node(200, geometry=Point(5.5, 52.5))

        # Mock vinharbour dataset with no routejunction match
        harbour_df = pd.DataFrame(
            [
                {
                    "Id": 25,
                    "Name": "Geometric Snap Harbour",
                    "Code": "NLOTHER",
                    "UnLocationCode": None,
                    "VinCode": "456",
                    "City": "SnapCity",
                    "geometry": Point(5.01, 52.01),  # Closer to node 100
                }
            ]
        )
        harbour_gdf = gpd.GeoDataFrame(harbour_df, geometry="geometry", crs="EPSG:4326")

        datasets = {
            "vinharbour": harbour_gdf,
            "routejunction": pd.DataFrame(columns=["Code", "SectionJunctionId"]),
        }

        # Run integrate
        G_enriched = integrate_harbours(G, datasets)

        # Assert harbour node was added
        self.assertTrue(G_enriched.has_node("harbour_25"))
        h_data = G_enriched.nodes["harbour_25"]
        self.assertEqual(h_data["locode"], "NLOTH")  # 5-char fallback from Code NLOTHER

        # Assert fallback snapped edge was added to node 100
        self.assertTrue(G_enriched.has_edge("harbour_25", 100))
        edge_data = G_enriched.edges["harbour_25", 100]
        self.assertEqual(edge_data["segment_type"], "harbour_access")


if __name__ == "__main__":
    unittest.main()
