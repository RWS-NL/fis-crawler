"""Validation logic for the merged FIS/EURIS graph."""

import logging
import pathlib
from typing import Dict, List, Any, Optional
import networkx as nx
from .schema import load_schema
from datetime import datetime

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
        for _, _, d in self.graph.edges(data=True):
            src = d.get("data_source", "unknown")
            edge_sources[src] = edge_sources.get(src, 0) + 1

        stats = {
            "total_nodes": self.graph.number_of_nodes(),
            "total_edges": self.graph.number_of_edges(),
            "nodes_by_source": node_sources,
            "edges_by_source": edge_sources,
            "connected_components": nx.number_connected_components(self.graph),
            "largest_component_size": len(max(nx.connected_components(self.graph), key=len)) if self.graph.number_of_nodes() > 0 else 0,
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
        """Check if attributes comply with schema."""
        logger.info("Checking schema compliance...")
        
        # Extract expected canonical names from schema
        # schema['attributes']['edges'] maps OLD -> NEW
        # We need the set of NEW names (values)
        edge_schema = self.schema.get("attributes", {}).get("edges", {})
        canonical_edge_attrs = set(edge_schema.values())
        
        # Add some standard known attributes
        canonical_edge_attrs.update({"data_source", "geometry", "id", "bridgehead", "distance_gap", "connection_type", "length"})
        
        non_compliant_keys = {} # key -> count
        
        for _, _, d in self.graph.edges(data=True):
            for k in d.keys():
                if k not in canonical_edge_attrs and k not in edge_schema:
                     # It might be a valid attribute that isn't in the mapping (e.g. geometry)
                     # But if it looks like a legacy attribute (CamelCase), flag it
                     if any(x.isupper() for x in k) and k != "geometry": 
                         non_compliant_keys[k] = non_compliant_keys.get(k, 0) + 1
                         
        compliance = {
            "non_standard_attributes_detected": list(non_compliant_keys.keys()),
            "attribute_counts": non_compliant_keys
        }
        self.results["schema_compliance"] = compliance
        return compliance

    def check_critical_connections(self) -> Dict[str, Any]:
        """Check specific critical connections known to be problematic."""
        logger.info("Checking critical connections...")
        
        # Critical nodes pairs to check (FIS -> EURIS)
        # Lobith: FIS_22638200 <-> EURIS_DE_J1144 (approximate, need to verify exact IDs in finding)
        # For now, we search for connections involving Lobith area
        
        checks = []
        
        # check Lobith (Rijn)
        lobith_found = False
        for u, v, d in self.graph.edges(data=True):
             if d.get("data_source") == "BORDER":
                 # Heuristic check for Lobith area
                 # This relies on knowing the IDs or inspecting the bridgehead
                 if "22638200" in u or "22638200" in v:
                     lobith_found = True
                     checks.append({"name": "Lobith Connection", "status": "PASS", "details": f"{u} <-> {v}"})
                     break
        
        if not lobith_found:
            # Try finding by EURIS side if known, or just warn
            checks.append({"name": "Lobith Connection", "status": "WARNING", "details": "FIS_22638200 not found in border connections"})

        self.results["critical_connections"] = {"checks": checks}
        return {"checks": checks}

    def generate_markdown_report(self) -> str:
        """Generate a Markdown report from results."""
        stats = self.results["statistics"]
        border = self.results["border_integrity"]
        schema = self.results["schema_compliance"]
        critical = self.results["critical_connections"]
        
        md = f"""# Validation Report: FIS-EURIS Merged Graph
**Generated at**: {datetime.now().isoformat()}
## 1. Graph Statistics
- **Total Nodes**: {stats['total_nodes']}
- **Total Edges**: {stats['total_edges']}
- **Connected Components**: {stats['connected_components']}
- **Largest Component**: {stats['largest_component_size']} nodes

### Composition
| Source | Nodes | Edges |
|--------|-------|-------|
"""
        for src in set(list(stats['nodes_by_source'].keys()) + list(stats['edges_by_source'].keys())):
            nodes = stats['nodes_by_source'].get(src, 0)
            edges = stats['edges_by_source'].get(src, 0)
            md += f"| {src} | {nodes} | {edges} |\n"

        md += f"""
## 2. Border Integrity
- **Status**: {border['status']}
- **Connections Found**: {border['total_connections']} (Expected: {border['expected_connections']})
- **Max Gap**: {border['max_gap_meters']:.2f} m
- **Avg Gap**: {border['avg_gap_meters']:.2f} m

### Connection List
| FIS Node | EURIS Node | Gap (m) |
|----------|------------|---------|
"""
        for c in border.get("connections", []):
            md += f"| {c['u']} | {c['v']} | {c['gap']:.2f} |\n"

        md += """
## 3. Schema Compliance
"""
        if schema['non_standard_attributes_detected']:
            md += "**WARNING**: Found potential non-standard attributes:\n"
            for k, v in schema['attribute_counts'].items():
                md += f"- `{k}`: {v} occurrences\n"
        else:
            md += "✅ No obvious schema violations found.\n"

        md += """
## 4. Critical Connections
| Location | Status | Details |
|----------|--------|---------|
"""
        for check in critical.get("checks", []):
            icon = "✅" if check['status'] == "PASS" else "⚠️"
            md += f"| {check['name']} | {icon} {check['status']} | {check['details']} |\n"

        return md
