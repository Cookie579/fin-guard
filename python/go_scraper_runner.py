import subprocess

def run_go_scraper():
    try:
        result = subprocess.run(
            ["go", "run", "preprocessing-esg/main.go", "preprocessing-esg/models.go", "preprocessing-esg/google.go", "preprocessing-esg/s3.go", "preprocessing-esg/kinesis.go"],
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
