"""
RAG Engine (The Cortex)
=======================

Handles document ingestion, chunking, embedding, and semantic search.
Uses ChromaDB for vector storage and SentenceTransformers for embeddings.
"""

import os
import uuid
from typing import List, Dict, Any, Optional
from pathlib import Path

try:
    import chromadb
    from chromadb.config import Settings
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("⚠️  RAG Engine dependencies missing. Install: chromadb sentence-transformers")
    chromadb = None

class RAGEngine:
    def __init__(self, collection_name: str = "esg_explainer"):
        if not chromadb:
            self.client = None
            return

        # Initialize persistent client
        self.db_path = os.path.join(os.path.dirname(__file__), "../chroma_db")
        self.client = chromadb.PersistentClient(path=self.db_path)
        
        # Initialize embedding model (local, fast)
        # 'all-MiniLM-L6-v2' is a good balance of speed/quality
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Get or create collection
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )

    def ingest_text(self, text: str, metadata: Dict[str, Any], chunk_size: int = 1000, overlap: int = 100):
        """Split text into chunks and index them."""
        if not self.client:
            return

        chunks = self._recursive_split(text, chunk_size, overlap)
        
        ids = [str(uuid.uuid4()) for _ in chunks]
        embeddings = self.model.encode(chunks).tolist()
        metadatas = [metadata for _ in chunks]  # Replicate metadata for each chunk

        self.collection.add(
            documents=chunks,
            embeddings=embeddings,
            metadatas=metadatas,
            ids=ids
        )
        print(f"  🧠 Indexed {len(chunks)} chunks for {metadata.get('source', 'unknown')}")

    def query(self, query_text: str, n_results: int = 5, filter_meta: Optional[Dict] = None) -> List[Dict]:
        """Retrieve most relevant chunks."""
        if not self.client:
            return []

        query_embedding = self.model.encode([query_text]).tolist()
        
        results = self.collection.query(
            query_embeddings=query_embedding,
            n_results=n_results,
            where=filter_meta  # e.g. {"ticker": "NVDA"}
        )
        
        # Format results
        output = []
        if results['documents']:
            for i, doc in enumerate(results['documents'][0]):
                meta = results['metadatas'][0][i]
                output.append({
                    "text": doc,
                    "metadata": meta,
                    "distance": results['distances'][0][i] if results['distances'] else 0
                })
        
        return output

    def _recursive_split(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        """Simple recursive character splitter."""
        if len(text) <= chunk_size:
            return [text]
        
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            
            # If we are not at the end, try to break at a newline or space
            if end < len(text):
                # Try to find last newline in the chunk
                last_newline = text.rfind('\n', start, end)
                if last_newline != -1:
                    end = last_newline + 1
                else:
                    # Try to find last space
                    last_space = text.rfind(' ', start, end)
                    if last_space != -1:
                        end = last_space + 1
            
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            
            start = end - overlap
            # Prevent infinite loop if overlap >= chunk_size (shouldn't happen with correct args)
            if start >= end:
                start = end
                
        return chunks

    def clear_collection(self):
        """Wipe the memory."""
        if self.client:
            self.client.delete_collection(self.collection.name)
