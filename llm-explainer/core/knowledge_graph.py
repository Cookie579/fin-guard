"""
Knowledge Graph (The World Model)
=================================

Extracts entities and relationships to build a graph network.
Uses NetworkX for graph operations and PageRank for node importance.
"""

import networkx as nx
from typing import List, Dict, Tuple
import json

class KnowledgeGraph:
    def __init__(self):
        self.graph = nx.DiGraph()

    def add_triplet(self, subject: str, predicate: str, object_: str, weight: float = 1.0):
        """Add a relationship: (Subject) --[Predicate]--> (Object)"""
        self.graph.add_edge(subject, object_, relation=predicate, weight=weight)


    def build_from_text(self, text: str, llm_client=None):
        """
        Uses LLM to extract triplets from text and build the graph.
        Extracts entities and relationships in the format:
        [{'subject': 'NVDA', 'predicate': 'supplies_to', 'object': 'Tesla'}]
        """
        if not llm_client:
            print("  ⚠️  No LLM client provided to Knowledge Graph")
            return
        
        # Limit text to avoid token limits (first 3000 chars)
        text_sample = text[:3000]
        
        prompt = f"""
        Extract key business relationships from this text as a JSON array of triplets.
        Each triplet should have: "subject", "predicate", "object"
        
        Example format:
        [{{"subject": "NVIDIA", "predicate": "supplies_chips_to", "object": "Tesla"}},
         {{"subject": "NVIDIA", "predicate": "depends_on", "object": "TSMC"}}]
        
        Focus on:
        - Supply chain relationships
        - Customer relationships
        - Dependencies
        - Partnerships
        
        Text:
        {text_sample}
        
        Return ONLY the JSON array, no other text.
        """
        
        import time
        max_retries = 3
        base_delay = 6  # 6 seconds = 10 RPM max
        
        for attempt in range(max_retries):
            try:
                # Rate limiting: wait between requests (10 RPM = 6 seconds per request)
                if attempt > 0:
                    retry_delay = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"  ⏳ Retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(retry_delay)
                elif hasattr(self, '_last_api_call'):
                    elapsed = time.time() - self._last_api_call
                    if elapsed < base_delay:
                        time.sleep(base_delay - elapsed)
                
                self._last_api_call = time.time()
                
                response = llm_client.chat.completions.create(
                    model="gpt-5-nano",
                    messages=[
                        {"role": "system", "content": "You are a knowledge extraction assistant. Extract business relationships from text and return them as JSON."},
                        {"role": "user", "content": prompt}
                    ]
                )
                
                # Parse JSON response
                import json
                import re
                
                # Extract JSON from response (handle markdown code blocks)
                response_text = response.choices[0].message.content.strip()
                # Remove markdown code blocks if present
                response_text = re.sub(r'```json\s*', '', response_text)
                response_text = re.sub(r'```\s*', '', response_text)
                
                triplets = json.loads(response_text)
                
                # Add to graph
                for triplet in triplets:
                    if isinstance(triplet, dict) and all(k in triplet for k in ['subject', 'predicate', 'object']):
                        self.add_triplet(
                            triplet['subject'],
                            triplet['predicate'],
                            triplet['object']
                        )
                
                print(f"  🕸️  Extracted {len(triplets)} relationships")
                return  # Success, exit retry loop
                
            except Exception as e:
                error_str = str(e)
                if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str:
                    if attempt < max_retries - 1:
                        continue  # Retry
                    else:
                        print(f"  ⚠️  Rate limit exceeded after {max_retries} attempts, skipping...")
                else:
                    print(f"  ⚠️  Failed to extract knowledge graph: {e}")
                    return

    def get_critical_nodes(self, top_n: int = 5) -> List[Tuple[str, float]]:
        """Use PageRank to find the most important entities."""
        if not self.graph.nodes:
            return []
        
        try:
            ranks = nx.pagerank(self.graph)
            sorted_ranks = sorted(ranks.items(), key=lambda x: x[1], reverse=True)
            return sorted_ranks[:top_n]
        except Exception:
            return []

    def get_downstream_impact(self, start_node: str, max_depth: int = 2) -> List[str]:
        """Trace dependencies: If start_node fails, who is affected?"""
        if start_node not in self.graph:
            return []
        
        impacted = set()
        # BFS traversal
        successors = list(self.graph.successors(start_node))
        impacted.update(successors)
        
        if max_depth > 1:
            for succ in successors:
                impacted.update(self.get_downstream_impact(succ, max_depth - 1))
                
        return list(impacted)

    def export_mermaid(self) -> str:
        """Generate Mermaid.js chart definition."""
        lines = ["graph TD"]
        for u, v, data in self.graph.edges(data=True):
            # Sanitize IDs
            u_safe = u.replace(" ", "_").replace("-", "_")
            v_safe = v.replace(" ", "_").replace("-", "_")
            label = data.get("relation", "related_to")
            lines.append(f"    {u_safe} --{label}--> {v_safe}")
        return "\n".join(lines)

    def to_json(self) -> Dict:
        """Export for frontend visualization."""
        return nx.node_link_data(self.graph)
