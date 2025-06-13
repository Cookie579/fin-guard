import subprocess

def run_go_scraper():
    try:
        result = subprocess.run(
            ["esg-scraper"],  # ✅ Just run the binary
            capture_output=True,
            text=True,
            cwd="/app"  # Optional: wherever you want the command to run
        )

        print("🌿 ESG Scraper Output:")
        print(result.stdout)
        if result.stderr:
            print("⚠️ ESG Scraper Errors:")
            print(result.stderr)
    except Exception as e:
        print(f"❌ Failed to run Go ESG scraper: {e}")
