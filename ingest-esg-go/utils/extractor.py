import sys
import pdfplumber
from bs4 import BeautifulSoup

# Force UTF-8 for all console writes, even on Windows
sys.stdout.reconfigure(encoding="utf-8", errors="ignore")

def extract_text(file_path):
    ext = file_path.lower().split(".")[-1]
    if ext == "pdf":
        with pdfplumber.open(file_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    elif ext in ["html", "htm", "aspx"]:
    # first try UTF-8
      try:
          with open(file_path, "r", encoding="utf-8") as f:
              html = f.read()
      except UnicodeDecodeError:
          # fallback to Windows-1252 but keep going if THAT fails
          with open(file_path, "r", encoding="cp1252", errors="ignore") as f:
              html = f.read()

    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator="\n", strip=True)


if __name__ == "__main__":
    input_path = sys.argv[1]
    try:
        text = extract_text(input_path)
        print(text)
    except Exception as e:
        print(f"Extraction failed for {input_path}: {e}", file=sys.stderr)
        sys.exit(1)
