#!/usr/bin/env python3
"""
ESG Analyzer — process any ESG .txt under texts/{Company}/ESG/
and emit a JSON to the exact output_file path supplied.
"""

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
WINDOW_SIZE            = 5     # sentences per chunk
STRIDE                 = 2     # overlap stride
MIN_CHARS              = 300
ESG_THRESH             = 0.5
RISK_THRESH            = 0.4
RELEVANCE_THRESH       = 0.7
TAG_RATIO_MAX          = 0.3
TOPIC_MIN_CLUSTER      = 2
TOPIC_MIN_SAMPLES      = 1
TOPIC_MODEL_MIN_CHUNKS = 5

SEED_KEYWORDS = {
    "Environmental": ["climate","emissions","carbon","sustainability","energy","pollution",
                      "greenhouse","biodiversity","waste","recycling","water","deforestation"],
    "Social":        ["labor","diversity","community","human rights","employees","inclusion",
                      "wages","health","safety","education","equity","harassment"],
    "Governance":    ["compliance","board","audit","ethics","oversight","whistleblower",
                      "transparency","corruption","shareholders","executive","risk","regulation"],
}

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
    header_pattern = r"^(ITEM\s+\d+[A-Z\-\. ]+|TABLE OF CONTENTS|PART\s+[I|II|III|IV])"
    return re.sub(header_pattern, r"\n\1\n", text,
                  flags=re.IGNORECASE | re.MULTILINE)

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
            text = re.sub(rf"\b({re.escape(kw)})\b", r"**\1**",
                          text, flags=re.IGNORECASE)
    return text

def is_human_text(chunk: str, max_tag_ratio: float = TAG_RATIO_MAX) -> bool:
    tokens = re.findall(r"\S+", chunk)
    tag_tokens = [t for t in tokens if "<" in t or t.startswith("http")]
    return (len(tag_tokens) / max(1, len(tokens))) < max_tag_ratio

# ────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────

def main(input_file=None, output_file=None):
    if not input_file or not output_file:
        raise ValueError("Must supply both input_file and output_file")

    input_path  = Path(input_file)
    output_path = Path(output_file)

    # Derive metadata
    SAFE_COMPANY = input_path.parent.parent.name.replace(" ", "_")
    FILING_DATE  = input_path.stem
    FORM_TYPE    = "ESG"

    # Prepare output directory
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("📖 Loading ESG data …")
    raw = load_text(str(input_path))

    print("🧠 Loading spaCy & segmenting …")
    nlp = spacy.load("en_core_web_sm")
    # increase spaCy's max document length to 26 million chars
    nlp.max_length = 26_000_000
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

    # Collect ESG-relevant chunks
    results = []
    print("🔍 Scanning chunks …")
    for chunk in tqdm(split_into_chunks(doc)):
        if not is_human_text(chunk):
            continue

        rel_out = relevance_pipe(chunk, rel_labels)
        if rel_out["labels"][0] != rel_labels[0] or \
           rel_out["scores"][0] < RELEVANCE_THRESH:
            continue

        esg_out = esg_pipe(chunk, esg_labels)
        cats = [lbl for lbl, sc in zip(esg_out["labels"], esg_out["scores"]) if sc >= ESG_THRESH]
        if not cats:
            continue

        risk_out = risk_pipe(chunk, risk_labels)
        risks = {lbl: round(sc, 3)
                 for lbl, sc in zip(risk_out["labels"], risk_out["scores"])
                 if sc >= RISK_THRESH}

        ent_doc = nlp(chunk)
        entities = [
            {"text": ent.text, "label": ent.label_}
            for ent in ent_doc.ents
            if ent.label_ in {"PERSON","ORG","LAW"}
        ]

        results.append({
            "text": highlight_all(chunk),
            "esg_categories": cats,
            "esg_confidences": {
                lbl: round(sc, 3)
                for lbl, sc in zip(esg_out["labels"], esg_out["scores"])
            },
            "risk_scores": risks,
            "entities": entities
        })

    print(f"✅ Found {len(results)} ESG-relevant chunks.")

    # If too few, skip topic modeling
    if len(results) < TOPIC_MODEL_MIN_CHUNKS:
        print(f"⚠️ Only {len(results)} ESG-relevant chunks. Skipping topic modeling.")
        topic_info = {}
    else:
        print("📊 Running topic modeling …")
        topic_info = {}
        for cat in ESG_KEYWORDS:
            texts = [r["text"] for r in results if cat in r["esg_categories"]]
            if len(texts) < TOPIC_MODEL_MIN_CHUNKS:
                print(f"⚠️ Skipping topic modeling for {cat} ({len(texts)} samples).")
                continue

            n_topics   = len(texts)
            n_components = min(5, max(2, n_topics - 1))
            n_neighbors  = min(10, max(2, n_topics - 2))
            umap_model   = UMAP(n_components=n_components,
                                n_neighbors=n_neighbors,
                                random_state=42)
            cluster_model = HDBSCAN(min_samples=TOPIC_MIN_SAMPLES,
                                    min_cluster_size=TOPIC_MIN_CLUSTER)

            tm = BERTopic(umap_model=umap_model,
                          hdbscan_model=cluster_model)
            try:
                tm_topics, _ = tm.fit_transform(texts)
            except Exception as e:
                print(f"❌ Topic modeling error for {cat}: {e}")
                continue

            freq_df      = tm.get_topic_freq().rename(
                                columns={"Topic":"topic","Count":"count"})
            topic_info[cat] = {
                "frequencies": freq_df.to_dict("records"),
                "terms":       tm.get_topics()
            }

    # Write final JSON
    out = {
        "company":     SAFE_COMPANY.replace("_", " "),
        "filing_date": FILING_DATE,
        "form_type":   FORM_TYPE,
        "esg_results": results,
        "topic_model": topic_info
    }
    output_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("💾 Saved →", output_path)


if __name__ == "__main__":
    import sys
    main(*sys.argv[1:])
