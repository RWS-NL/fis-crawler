"""Validation logic for the merged FIS/EURIS graph."""

import logging
import pathlib
from typing import Dict, List, Any, Optional
import networkx as nx
from .schema import load_schema
from datetime import datetime
import jinja2

logger = logging.getLogger(__name__)

class GraphValidator:
    """Validator for FIS-EURIS merged graph."""

    def __init__(self, graph: nx.Graph, schema_path: Optional[pathlib.Path] = None):
        """Initialize validator.
        
        Args:
            graph: The merged networkx graph.
            schema_path: Path to schema.toml configuration.
        """
        self.graph = graph
        self.schema = load_schema(schema_path) if schema_path else {}
        self.results = {
            "statistics": {},
            "border_integrity": {},
            "schema_compliance": {},
            "critical_connections": {},
        }

    def check_statistics(self) -> Dict[str, Any]:
        """Calculate graph statistics."""
        logger.info("Running statistical checks...")
        
        # Node counts per source
        node_sources = {}
        for _, d in self.graph.nodes(data=True):
            src = d.get("data_source", "unknown")
            node_sources[src] = node_sources.get(src, 0) + 1
            
        # Edge counts per source
        edge_sources = {}
        fairway_ids = set()
        for _, _, d in self.graph.edges(data=True):
            src = d.get("data_source", "unknown")
            edge_sources[src] = edge_sources.get(src, 0) + 1
            if "fairway_id" in d and d["fairway_id"]:
                fairway_ids.add(d["fairway_id"])

        components = list(nx.connected_components(self.graph))
        components.sort(key=len, reverse=True)
        component_stats = []
        for i, comp in enumerate(components):
            if i < 10 or len(comp) > 1:
                component_stats.append({
                    "subgraph_id": i,
                    "nodes": len(comp),
                    "edges": self.graph.subgraph(comp).number_of_edges()
                })

        stats = {
            "total_nodes": self.graph.number_of_nodes(),
            "total_edges": self.graph.number_of_edges(),
            "nodes_by_source": node_sources,
            "edges_by_source": edge_sources,
            "connected_components": len(components),
            "largest_component_size": len(components[0]) if components else 0,
            "subgraphs": component_stats,
            "unique_fairway_sections": len(fairway_ids),
        }
        self.results["statistics"] = stats
        return stats

    def check_border_integrity(self) -> Dict[str, Any]:
        """Check integrity of border connections."""
        logger.info("Checking border integrity...")
        
        border_edges = []
        for u, v, d in self.graph.edges(data=True):
            if d.get("data_source") == "BORDER":
                border_edges.append((u, v, d))
        
        # Check gaps
        max_gap = 0.0
        min_gap = float("inf") if border_edges else 0.0
        gaps = []
        
        for *_, d in border_edges:
            dist = d.get("distance_gap", 0.0)
            gaps.append(dist)
            if dist > max_gap:
                max_gap = dist
            if dist < min_gap:
                min_gap = dist
                
        integrity = {
            "total_connections": len(border_edges),
            "expected_connections": 14, # Known baseline
            "status": "PASS" if len(border_edges) >= 14 else "WARNING",
            "max_gap_meters": max_gap,
            "avg_gap_meters": sum(gaps) / len(gaps) if gaps else 0.0,
            "connections": [{"u": u, "v": v, "gap": d.get("distance_gap")} for u, v, d in border_edges]
        }
        self.results["border_integrity"] = integrity
        return integrity

    def check_schema_compliance(self) -> Dict[str, Any]:
        """Check if attributes comply with schema and track completeness."""
        logger.info("Checking schema compliance...")
        
        node_schema = self.schema.get("attributes", {}).get("nodes", {})
        edge_schema = self.schema.get("attributes", {}).get("edges", {})
        
        canonical_node_attrs = set(node_schema.values()) | {"data_source", "geometry", "node_id", "countrycode"}
        canonical_edge_attrs = set(edge_schema.values()) | {"data_source", "geometry", "id", "bridgehead", "distance_gap", "connection_type"}
        
        non_compliant_node_keys = {}
        node_missing_counts = {k: 0 for k in canonical_node_attrs}
        node_attribute_docs = {k: "Mapped from " + str([old for old, new in node_schema.items() if new == k]) for k in canonical_node_attrs if k in node_schema.values()}
        for k in canonical_node_attrs:
            if k not in node_attribute_docs:
                node_attribute_docs[k] = "Standard/Base Attribute"
        
        for n, d in self.graph.nodes(data=True):
            for k in d.keys():
                if k not in canonical_node_attrs and k not in node_schema:
                    if any(x.isupper() for x in k) and k != "geometry": 
                        non_compliant_node_keys[k] = non_compliant_node_keys.get(k, 0) + 1
            for k in canonical_node_attrs:
                if k not in d or d[k] is None or d[k] == "":
                    node_missing_counts[k] += 1

        non_compliant_edge_keys = {}
        edge_missing_counts = {k: 0 for k in canonical_edge_attrs}
        edge_attribute_docs = {k: "Mapped from " + str([old for old, new in edge_schema.items() if new == k]) for k in canonical_edge_attrs if k in edge_schema.values()}
        for k in canonical_edge_attrs:
            if k not in edge_attribute_docs:
                edge_attribute_docs[k] = "Standard/Base Attribute"
        
        for u, v, d in self.graph.edges(data=True):
            for k in d.keys():
                if k not in canonical_edge_attrs and k not in edge_schema:
                    if any(x.isupper() for x in k) and k != "geometry": 
                        non_compliant_edge_keys[k] = non_compliant_edge_keys.get(k, 0) + 1
            for k in canonical_edge_attrs:
                if k not in d or d[k] is None or d[k] == "":
                    edge_missing_counts[k] += 1
                         
        compliance = {
            "nodes": {
                "non_standard_attributes_detected": list(non_compliant_node_keys.keys()),
                "attribute_counts": non_compliant_node_keys,
                "missing_counts": node_missing_counts,
                "expected_attributes": list(canonical_node_attrs),
                "attribute_docs": node_attribute_docs
            },
            "edges": {
                "non_standard_attributes_detected": list(non_compliant_edge_keys.keys()),
                "attribute_counts": non_compliant_edge_keys,
                "missing_counts": edge_missing_counts,
                "expected_attributes": list(canonical_edge_attrs),
                "attribute_docs": edge_attribute_docs
            }
        }
        self.results["schema_compliance"] = compliance
        return compliance

    def check_critical_connections(self) -> Dict[str, Any]:
        """Check specific critical connections known to be problematic."""
        logger.info("Checking critical connections...")
        
        checks = []
        lobith_found = False
        for u, v, d in self.graph.edges(data=True):
             if d.get("data_source") == "BORDER":
                 if "22638200" in u or "22638200" in v:
                     lobith_found = True
                     checks.append({"name": "Lobith Connection", "status": "PASS", "details": f"{u} <-> {v}"})
                     break
        
        if not lobith_found:
            checks.append({"name": "Lobith Connection", "status": "WARNING", "details": "FIS_22638200 not found in border connections"})

        self.results["critical_connections"] = {"checks": checks}
        return {"checks": checks}

    def generate_markdown_report(self) -> str:
        """Generate a Markdown report from results using Jinja2 template."""
        stats = self.results["statistics"]
        border = self.results["border_integrity"]
        schema = self.results["schema_compliance"]
        critical = self.results["critical_connections"]
        
        # Calculate sources for table
        sources = set(list(stats['nodes_by_source'].keys()) + list(stats['edges_by_source'].keys()))
        
        template_dir = pathlib.Path(__file__).parent / "templates"
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
        template = env.get_template("validation_report.md.j2")
        
        return template.render(
            timestamp=datetime.now().isoformat(),
            stats=stats,
            border=border,
            schema=schema,
            critical=critical,
            sources=list(sources)
        )

