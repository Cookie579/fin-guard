"""
Black Swan Simulator (The Oracle)
=================================

Simulates "What If" scenarios using the Knowledge Graph.
Propagates shocks through the dependency network.
"""

from typing import List, Dict
import networkx as nx

class StressSimulator:
    def __init__(self, knowledge_graph):
        self.kg = knowledge_graph

    def run_simulation(self, scenario: str, start_node: str) -> Dict[str, str]:
        """
        Simulate a shock event.
        scenario: Description (e.g., "China invades Taiwan")
        start_node: The node where the shock originates (e.g., "Taiwan")
        """
        print(f"\n🔮 Running Simulation: {scenario}...")
        
        # 1. Identify Impacted Nodes
        impacted_nodes = self.kg.get_downstream_impact(start_node)
        
        if not impacted_nodes:
            return {
                "scenario": scenario,
                "impact": "Low",
                "details": f"No direct dependencies found starting from '{start_node}' in the graph."
            }

        # 2. Generate Impact Report (Mock LLM Logic)
        # In a real system, we'd feed the subgraph to the LLM.
        impact_summary = f"Shock to '{start_node}' propagates to: {', '.join(impacted_nodes[:5])}..."
        
        return {
            "scenario": scenario,
            "impact": "Critical",
            "affected_nodes": impacted_nodes,
            "summary": impact_summary
        }
