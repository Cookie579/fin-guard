import subprocess
import pathlib

SCRAPER_PATHS = [
    "python/preprocessing-esg/main.go",
    "python/preprocessing-esg/domain_search.go",
    "python/preprocessing-esg/domain.go",
    "python/preprocessing-esg/serper.go",
    "python/preprocessing-esg/models.go",
    "python/preprocessing-esg/s3.go",
    "python/preprocessing-esg/kinesis.go",
    "python/preprocessing-esg/downloader.go",
]

PROJECT_ROOT = pathlib.Path("/app")

def _run_go_scraper(mode: str):
    try:
        cmd = ["go", "run", *SCRAPER_PATHS, f"-mode={mode}"]
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        print(f"🌿 GO scraper [{mode}] stdout:\n{result.stdout}")
        if result.stderr:
            print(f"⚠️ GO scraper [{mode}] stderr:\n{result.stderr}")
        if result.returncode != 0:
            print(f"❌ GO scraper exited with code {result.returncode}")
    except Exception as e:
        print(f"❌ Failed to run Go scraper ({mode}): {e}")

def run_esg_scraper():
    _run_go_scraper("esg")

def run_domain_scraper():
    _run_go_scraper("domain")

def run_download_scraper():
    _run_go_scraper("download")
