import unittest
import networkx as nx
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from fis.graph.enrich_fis import integrate_harbours


class TestHarbourIntegration(unittest.TestCase):
    def test_integrate_harbour_splicing_single(self):
        # Build simple graph with two junction nodes connected by a fairway section
        G = nx.Graph()
        p1 = Point(5.0, 52.0)
        p2 = Point(5.02, 52.0)
        line = LineString([p1, p2])
        G.add_node(100, geometry=p1)
        G.add_node(200, geometry=p2)
        G.add_edge(100, 200, Id=1, geometry=line, length_m=1380.0, maxspeed=12.0)

        # Mock vinharbour dataset with FairwaySectionId=1
        harbour_df = pd.DataFrame(
            [
                {
                    "Id": 15,
                    "Name": "Test Harbour",
                    "FairwaySectionId": 1,
                    "Code": "NLTEST0001",
                    "UnLocationCode": "NLTE1",
                    "VinCode": "123",
                    "City": "TestCity",
                    "geometry": Point(5.01, 52.001),  # ~111m south of section middle
                }
            ]
        )
        harbour_gdf = gpd.GeoDataFrame(harbour_df, geometry="geometry", crs="EPSG:4326")

        sections_df = pd.DataFrame(
            [
                {
                    "Id": 1,
                    "StartJunctionId": 100,
                    "EndJunctionId": 200,
                    "geometry": line,
                }
            ]
        )
        sections_gdf = gpd.GeoDataFrame(
            sections_df, geometry="geometry", crs="EPSG:4326"
        )

        datasets = {"vinharbour": harbour_gdf, "section": sections_gdf}

        # Run integrate
        G_enriched = integrate_harbours(G, datasets)

        # Assert harbour node was added
        self.assertTrue(G_enriched.has_node("harbour_15"))
        h_data = G_enriched.nodes["harbour_15"]
        self.assertEqual(h_data["name"], "Test Harbour")
        self.assertEqual(h_data["locode"], "NLTE1")
        self.assertEqual(h_data["node_type"], "harbour")
        self.assertEqual(h_data["isrs_id"], "NLTEST0001")

        # Assert connection node was created on the fairway
        conn_id = "harbour_15_connection"
        self.assertTrue(G_enriched.has_node(conn_id))
        self.assertEqual(G_enriched.nodes[conn_id]["node_type"], "junction")

        # Assert original edge (100, 200) was spliced into (100, conn_id) and (conn_id, 200)
        self.assertFalse(G_enriched.has_edge(100, 200))
        self.assertTrue(G_enriched.has_edge(100, conn_id))
        self.assertTrue(G_enriched.has_edge(conn_id, 200))

        # Assert edge attributes like maxspeed were preserved on spliced segments
        self.assertEqual(G_enriched.edges[100, conn_id]["maxspeed"], 12.0)
        self.assertEqual(G_enriched.edges[conn_id, 200]["maxspeed"], 12.0)

        # Assert access edge was added between harbour and connection node
        self.assertTrue(G_enriched.has_edge("harbour_15", conn_id))
        access_edge = G_enriched.edges["harbour_15", conn_id]
        self.assertEqual(access_edge["segment_type"], "harbour_access")
        self.assertEqual(access_edge["data_source"], "vinharbour")
        # Access distance should be ~111m orthogonal to fairway, not kilometers to junction 100/200
        self.assertLess(access_edge["length_m"], 150.0)

    def test_integrate_harbour_splicing_multiple(self):
        # Build graph with a fairway section
        G = nx.Graph()
        p1 = Point(5.0, 52.0)
        p2 = Point(5.03, 52.0)
        line = LineString([p1, p2])
        G.add_node(100, geometry=p1)
        G.add_node(200, geometry=p2)
        G.add_edge(100, 200, Id=1, geometry=line, length_m=2070.0)

        # Two harbours on same section at different points
        harbour_df = pd.DataFrame(
            [
                {
                    "Id": 10,
                    "Name": "Harbour 1",
                    "FairwaySectionId": 1,
                    "Code": "NLHBR0001",
                    "geometry": Point(5.01, 52.0005),
                },
                {
                    "Id": 20,
                    "Name": "Harbour 2",
                    "FairwaySectionId": 1,
                    "Code": "NLHBR0002",
                    "geometry": Point(5.02, 52.0005),
                },
            ]
        )
        harbour_gdf = gpd.GeoDataFrame(harbour_df, geometry="geometry", crs="EPSG:4326")

        sections_df = pd.DataFrame(
            [{"Id": 1, "StartJunctionId": 100, "EndJunctionId": 200, "geometry": line}]
        )
        sections_gdf = gpd.GeoDataFrame(
            sections_df, geometry="geometry", crs="EPSG:4326"
        )

        datasets = {"vinharbour": harbour_gdf, "section": sections_gdf}
        G_enriched = integrate_harbours(G, datasets)

        conn1 = "harbour_10_connection"
        conn2 = "harbour_20_connection"

        self.assertTrue(G_enriched.has_node("harbour_10"))
        self.assertTrue(G_enriched.has_node("harbour_20"))
        self.assertTrue(G_enriched.has_node(conn1))
        self.assertTrue(G_enriched.has_node(conn2))

        # Check connectivity: 100 -> conn1 -> conn2 -> 200
        self.assertTrue(G_enriched.has_edge(100, conn1))
        self.assertTrue(G_enriched.has_edge(conn1, conn2))
        self.assertTrue(G_enriched.has_edge(conn2, 200))
        self.assertTrue(nx.has_path(G_enriched, 100, 200))

    def test_integrate_harbour_clustering_close_points(self):
        # Two harbours on opposite sides of the canal at the exact same location
        G = nx.Graph()
        p1 = Point(5.0, 52.0)
        p2 = Point(5.02, 52.0)
        line = LineString([p1, p2])
        G.add_node(100, geometry=p1)
        G.add_node(200, geometry=p2)
        G.add_edge(100, 200, Id=1, geometry=line, length_m=1380.0)

        harbour_df = pd.DataFrame(
            [
                {
                    "Id": 1,
                    "Name": "North Harbour",
                    "FairwaySectionId": 1,
                    "Code": "NLNHBR001",
                    "geometry": Point(5.01, 52.001),  # North
                },
                {
                    "Id": 2,
                    "Name": "South Harbour",
                    "FairwaySectionId": 1,
                    "Code": "NLSHBR002",
                    "geometry": Point(5.01, 51.999),  # South
                },
            ]
        )
        harbour_gdf = gpd.GeoDataFrame(harbour_df, geometry="geometry", crs="EPSG:4326")

        sections_df = pd.DataFrame(
            [{"Id": 1, "StartJunctionId": 100, "EndJunctionId": 200, "geometry": line}]
        )
        sections_gdf = gpd.GeoDataFrame(
            sections_df, geometry="geometry", crs="EPSG:4326"
        )

        datasets = {"vinharbour": harbour_gdf, "section": sections_gdf}
        G_enriched = integrate_harbours(G, datasets)

        # Both harbours should share the connection node on the fairway
        conn_node = "harbour_1_connection"
        self.assertTrue(G_enriched.has_edge("harbour_1", conn_node))
        self.assertTrue(G_enriched.has_edge("harbour_2", conn_node))
        self.assertTrue(G_enriched.has_edge(100, conn_node))
        self.assertTrue(G_enriched.has_edge(conn_node, 200))
        self.assertTrue(nx.has_path(G_enriched, 100, 200))


if __name__ == "__main__":
    unittest.main()
