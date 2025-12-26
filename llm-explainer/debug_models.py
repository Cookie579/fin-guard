
import os
import json
from pathlib import Path
import sys

# Add current directory to path to find modules if needed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def load_config():
    """Load API keys from root config.json"""
    # Go up two levels from llm-explainer (where we will run this) to find config.json
    # Script location: fin-guard/llm-explainer/debug_models.py
    # Config location: fin-guard/config.json
    root_dir = Path(__file__).parent.parent
    config_path = root_dir / "config.json"
    
    if not config_path.exists():
        print(f"❌ Config not found at {config_path}")
        return {}
        
    with open(config_path, "r") as f:
        return json.load(f)

def list_models():
    try:
        from openai import OpenAI
    except ImportError:
        print("❌ openai library not found.")
        return

    config = load_config()
    api_key = config.get("openai_api_key")
    if not api_key:
        print("❌ Missing 'openai_api_key' in config.json")
        return

    print(f"🔑 Use API Key: {api_key[:5]}...{api_key[-4:]}")
    
    try:
        client = OpenAI(api_key=api_key)
        print("\n🔎 Listing available models...\n")
        
        # List available OpenAI models
        for model in client.models.list():
            print(f"ID: {model.id}")

    except Exception as e:
        print(f"❌ Error listing models: {e}")

if __name__ == "__main__":
    list_models()
