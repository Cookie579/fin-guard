import subprocess

def run_go_scraper():
    try:
        result = subprocess.run(
            ["go", "run", "main.go", "models.go", "google.go", "s3.go", "kinesis.go"],
            cwd="python/preprocessing-esg",
            capture_output=True,
            text=True
        )
        print("🌿 ESG Scraper Output:")
        print(result.stdout)
        if result.stderr:
            print("⚠️ ESG Scraper Errors:")
            print(result.stderr)
    except Exception as e:
        print(f"❌ Failed to run Go ESG scraper: {e}")
