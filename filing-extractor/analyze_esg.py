import re
import json
from pathlib import Path

import nltk
from nltk.corpus import wordnet as wn

import spacy
from tqdm import tqdm
from transformers import pipeline
from bertopic import BERTopic
import torch
from hdbscan import HDBSCAN
from umap import UMAP

# ────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────
INPUT_FILE           = "texts/10-k/Apple_Inc.__2014-10-27__10-K.txt"
OUTPUT_FILE          = "output-esg-advanced.json"
WINDOW_SIZE          = 5     # sentences per chunk
STRIDE               = 2     # overlap stride
MIN_CHARS            = 300
ESG_THRESH           = 0.5   # min confidence for ESG labels
RISK_THRESH          = 0.4   # min confidence for risk labels
RELEVANCE_THRESH     = 0.7   # min confidence for ESG relevance
TAG_RATIO_MAX        = 0.3   # max ratio of tag-like tokens
TOPIC_MIN_CLUSTER    = 2     # min cluster size for topic modeling
TOPIC_MIN_SAMPLES    = 1     # min samples for HDBSCAN


input_path = Path(INPUT_FILE)
filename   = input_path.stem  # "Apple_Inc.__2014-10-27__10-K"

try:
    parts = filename.split("__")
    SAFE_COMPANY = parts[0]
    FILING_DATE  = parts[1]
    FORM_TYPE    = parts[2]
except Exception:
    raise ValueError(f"❌ Could not parse metadata from filename: {filename}")

COMPANY_NAME = SAFE_COMPANY.replace("_", " ")  # for pretty printing

# Dynamic output path
OUTPUT_FILE = f"esg-outputs/10-k/{SAFE_COMPANY}__{FILING_DATE}__{FORM_TYPE}.json"


# seed keywords for ontology expansion
SEED_KEYWORDS = {
    "Environmental": [
        "climate","emissions","carbon","sustainability","energy","pollution",
        "greenhouse","biodiversity","waste","recycling","water","deforestation"
    ],
    "Social": [
        "labor","diversity","community","human rights","employees","inclusion",
        "wages","health","safety","education","equity","harassment"
    ],
    "Governance": [
        "compliance","board","audit","ethics","oversight","whistleblower",
        "transparency","corruption","shareholders","executive","risk","regulation"
    ],
}

# ────────────────────────────────────────────────────────────────
# ONTOLOGY / KEYWORD EXPANSION (WordNet)
# ────────────────────────────────────────────────────────────────
nltk.download("wordnet", quiet=True)

def expand_keywords(seed):
    expanded = {}
    for cat, kws in seed.items():
        syns = set(map(str.lower, kws))
        for kw in kws:
            for synset in wn.synsets(kw):
                for lemma in synset.lemmas():
                    name = lemma.name().replace("_", " ").lower()
                    if len(name) > 2:
                        syns.add(name)
        expanded[cat] = list(syns)
    return expanded

ESG_KEYWORDS = expand_keywords(SEED_KEYWORDS)

# ────────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────────

def load_text(path: str) -> str:
    text = Path(path).read_text(encoding="utf-8")
    # Preserve and isolate section headers (e.g., "ITEM 1.", "TABLE OF CONTENTS")
    header_pattern = r"^(ITEM\s+\d+[A-Z\-\. ]+|TABLE OF CONTENTS|PART\s+[I|II|III|IV])"
    return re.sub(header_pattern, r"\n\1\n", text, flags=re.IGNORECASE | re.MULTILINE)

def split_into_chunks(doc, window=WINDOW_SIZE, stride=STRIDE):
    sents = list(doc.sents)
    for i in range(0, len(sents), stride):
        chunk_sents = sents[i : i + window]
        chunk = " ".join(s.text.strip() for s in chunk_sents)
        if len(chunk) >= MIN_CHARS:
            yield chunk

def highlight_all(text: str) -> str:
    for kws in ESG_KEYWORDS.values():
        for kw in kws:
            text = re.sub(rf"\b({re.escape(kw)})\b", r"**\1**", text, flags=re.IGNORECASE)
    return text

def is_human_text(chunk: str, max_tag_ratio: float = TAG_RATIO_MAX) -> bool:
    tokens = re.findall(r"\S+", chunk)
    tag_tokens = [t for t in tokens if "<" in t or t.startswith("http")]
    return (len(tag_tokens) / max(1, len(tokens))) < max_tag_ratio

# ────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────
def main():
    print("📖 Loading 10-K …")
    raw = load_text(INPUT_FILE)

    print("🧠 Loading spaCy & segmenting …")
    nlp = spacy.load("en_core_web_sm")
    nlp.add_pipe("sentencizer")
    doc = nlp(raw)

    device = 0 if torch.cuda.is_available() else -1

    print("⚙️  Setting up pipelines …")
    relevance_pipe = pipeline(
        "zero-shot-classification",
        model="facebook/bart-large-mnli",
        tokenizer="facebook/bart-large-mnli",
        device=device,
        multi_label=False
    )
    rel_labels = ["about ESG topics", "not about ESG"]

    esg_pipe = pipeline(
        "zero-shot-classification",
        model="facebook/bart-large-mnli",
        tokenizer="facebook/bart-large-mnli",
        device=device,
        multi_label=True
    )
    esg_labels = list(ESG_KEYWORDS.keys())

    risk_pipe = pipeline(
        "zero-shot-classification",
        model="facebook/bart-large-mnli",
        tokenizer="facebook/bart-large-mnli",
        device=device,
        multi_label=True
    )
    risk_labels = ["positive", "neutral", "negative", "high risk"]

    results = []
    print("🔍 Scanning chunks …")
    for chunk in tqdm(split_into_chunks(doc)):
        if not is_human_text(chunk):
            continue

        rel_out = relevance_pipe(chunk, rel_labels)
        if rel_out["labels"][0] != rel_labels[0] or rel_out["scores"][0] < RELEVANCE_THRESH:
            continue

        esg_out = esg_pipe(chunk, esg_labels)
        cats = [lbl for lbl, sc in zip(esg_out["labels"], esg_out["scores"]) if sc >= ESG_THRESH]
        if not cats:
            continue

        risk_out = risk_pipe(chunk, risk_labels)
        risks = {lbl: round(sc, 3) for lbl, sc in zip(risk_out["labels"], risk_out["scores"]) if sc >= RISK_THRESH}

        ent_doc = nlp(chunk)
        entities = [{"text": ent.text, "label": ent.label_} 
                    for ent in ent_doc.ents 
                    if ent.label_ in {"PERSON","ORG","LAW"}]

        results.append({
            "text": highlight_all(chunk),
            "esg_categories": cats,
            "esg_confidences": {lbl: round(sc, 3) for lbl, sc in zip(esg_out["labels"], esg_out["scores"])},
            "risk_scores": risks,
            "entities": entities
        })

    print(f"✅ Found {len(results)} ESG-relevant chunks.")

    print("📊 Running topic modeling …")
    topic_info = {}
    for cat in ESG_KEYWORDS.keys():
        texts = [r["text"] for r in results if cat in r["esg_categories"]]
        if len(texts) < TOPIC_MIN_CLUSTER:
            continue
        umap_model = UMAP(n_components=5, random_state=42)
        cluster_model = HDBSCAN(min_samples=TOPIC_MIN_SAMPLES, 
                                min_cluster_size=TOPIC_MIN_CLUSTER)
        tm = BERTopic(umap_model=umap_model, hdbscan_model=cluster_model)
        topics, probs = tm.fit_transform(texts)
        freq_df = tm.get_topic_freq().rename(columns={"Topic":"topic","Count":"count"})
        freq_records = freq_df.to_dict(orient="records")
        terms = tm.get_topics()
        topic_info[cat] = {"frequencies": freq_records, "terms": terms}

    out = {
        "company": COMPANY_NAME,
        "filing_date": FILING_DATE,
        "form_type": FORM_TYPE,
        "esg_results": results,
        "topic_model": topic_info
    }
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_FILE).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("💾 Saved →", OUTPUT_FILE)


if __name__ == "__main__":
    main()
