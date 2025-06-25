import re
import json
from pathlib import Path
from analyze_sec import main as run_sec_analysis
from analyze_esg import main as run_esg_analysis

# root texts directory
TEXTS_DIR = Path(__file__).resolve().parent / "texts"
# shared output directory
OUTPUT_DIR = Path("esg-outputs")


def get_txt_files():
    print(f"🔎 Scanning all text files under: {TEXTS_DIR}")
    return list(TEXTS_DIR.rglob("*.txt"))


def parse_sec_metadata(file_path: Path):
    """Parse metadata from filenames like company__date__form.txt"""
    parts = file_path.stem.split("__")
    if len(parts) != 3:
        raise ValueError(f"❌ Invalid SEC filename format: {file_path.stem}")
    return parts[0], parts[1], parts[2]


def construct_sec_output_path(company: str, form_type: str, date: str) -> Path:
    safe_company = company.replace(" ", "_")
    return OUTPUT_DIR / safe_company / form_type / f"{safe_company}__{date}__{form_type}__Analyzed.json"


def construct_esg_output_path(company: str, file_stem: str) -> Path:
    """
    For ESG:  
      input: texts/{Company}/ESG/{file_stem}.txt  
      output: esg-outputs/{Company}/ESG/{file_stem}__Analyzed.json
    """
    safe_company = company.replace(" ", "_")
    return OUTPUT_DIR / safe_company / "ESG" / f"{file_stem}__Analyzed.json"


def already_analyzed(path: Path) -> bool:
    if path.exists():
        return True
    # Fallback for legacy un‐suffixed outputs
    alt = path.with_name(path.name.replace("__Analyzed", ""))
    return alt.exists()


def main():
    files = get_txt_files()
    print(f"📂 Found {len(files)} files to analyze.\n")

    for file in files:
        is_esg = file.parent.name.upper() == "ESG"

        try:
            if is_esg:
                # derive company from folder, ignore filename format
                company = file.parent.parent.name
                output_path = construct_esg_output_path(company, file.stem)
            else:
                company, date, form_type = parse_sec_metadata(file)
                output_path = construct_sec_output_path(company, form_type, date)
        except ValueError as e:
            print(e)
            continue

        if already_analyzed(output_path):
            print(f"✅ Already analyzed: {output_path}")
            continue

        print(f"🔍 Processing {file.relative_to(TEXTS_DIR)} as "
              f"{'ESG' if is_esg else form_type} analysis...")
        try:
            if is_esg:
                run_esg_analysis(input_file=file, output_file=output_path)
            else:
                run_sec_analysis(input_file=file, output_file=output_path)
        except Exception as e:
            print(f"❌ Failed on {file.name}: {e}")
            continue

    print("\n🎉 All done.")


if __name__ == "__main__":
    main()
