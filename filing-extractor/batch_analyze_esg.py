import re
import json
from pathlib import Path
from analyze_esg import main as run_analysis  

TEXTS_DIR = Path(__file__).resolve().parent / "texts"
OUTPUT_DIR = Path("esg-outputs")

def get_txt_files():
    print(f"🔎 Looking in: {TEXTS_DIR.resolve()}")
    files = list(TEXTS_DIR.rglob("*.txt"))
    for f in files:
        print(f"📄 Found file: {f}")
    return files


def parse_metadata(file_path: Path):
    filename = file_path.stem  
    try:
        parts = filename.split("__")
        company = parts[0]
        date = parts[1]
        form_type = parts[2]
        return company, date, form_type
    except Exception:
        raise ValueError(f"❌ Failed to parse metadata from: {file_path}")

def construct_output_path(company, form_type, date):
    safe_company = company.replace(" ", "_")
    return OUTPUT_DIR / safe_company / form_type / f"{safe_company}__{date}__{form_type}__Analyzed.json"

def already_analyzed(output_path):
    return output_path.exists()

def main():
    files = get_txt_files()
    print(f"📂 Found {len(files)} files to analyze.")
    for file in files:
        try:
            company, date, form_type = parse_metadata(file)
        except Exception as e:
            print(e)
            continue

        output_path = construct_output_path(company, form_type, date)

        if already_analyzed(output_path):
            print(f"✅ Already analyzed: {output_path.name}")
            continue

        # Override paths for analyze_esg
        print(f"🔍 Analyzing {file.name}")
        try:
            run_analysis(
                input_file=file,
                output_file=output_path
            )
        except Exception as e:
            print(f"❌ Failed on {file.name}: {e}")
            continue

if __name__ == "__main__":
    main()
