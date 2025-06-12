import subprocess

def run_go_scraper():
    try:
        result = subprocess.run(
            [
                "go", "run",
                "python/preprocessing-esg/main.go",
                "python/preprocessing-esg/domain.go",
                "python/preprocessing-esg/google.go",
                "python/preprocessing-esg/models.go",
                "python/preprocessing-esg/s3.go",
                "python/preprocessing-esg/kinesis.go"
            ],
            cwd="/app",  # absolute or relative from FastAPI's perspective
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
