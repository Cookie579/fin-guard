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
from typing import Any

import spacy
from tqdm import tqdm
from transformers import pipeline
from bertopic import BERTopic
import torch
from hdbscan import HDBSCAN
from umap import UMAP

# ────────────────────────────────────────────────────────────────
# CONFIG
#
# Default configuration for ESG analysis. These constants define how text
# is chunked, filtered and analysed. ``WINDOW_SIZE`` and ``STRIDE``
# control chunk segmentation; ``MIN_CHARS`` requires a minimum number of
# characters per chunk.  ``ESG_THRESH`` and ``RISK_THRESH`` define
# confidence thresholds for ESG category and risk predictions, respectively.
# ``RELEVANCE_THRESH`` filters out chunks unlikely to be ESG-related.
# ``TAG_RATIO_MAX`` helps identify and skip boilerplate or markup.  The
# parameters ``TOPIC_MIN_CLUSTER`` and ``TOPIC_MIN_SAMPLES`` configure
# HDBSCAN clustering used by BERTopic.  Finally, ``TOPIC_MODEL_MIN_CHUNKS``
# sets the minimum number of ESG-relevant chunks required before topic
# modelling is attempted.  See the FinGuard architecture specification
# for further details.
WINDOW_SIZE            = 5
STRIDE                 = 2
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
    # Attempt to load the full spaCy model for maximum accuracy on smaller
    # documents.  If the document is very large and the full model
    # cannot be used due to memory constraints, we fall back to a
    # lightweight blank model with only a Sentencizer.  This dynamic
    # selection ensures high precision when possible while avoiding
    # crashes on huge inputs.
    # We'll attempt to use the full English model first.  If it fails
    # (e.g. due to memory constraints), we'll fall back to progressively
    # simpler strategies: a blank pipeline with sentencizer, and finally
    # a naive regex-based sentence splitter.  ``nlp`` will be set to
    # whichever model succeeds, or ``None`` if naive segmentation is used.
    nlp = None  # type: ignore[var‑annotated]
    doc = None  # type: ignore[var‑annotated]
    try:
        full_nlp = spacy.load("en_core_web_sm")
        full_nlp.max_length = max(full_nlp.max_length, len(raw) + 1)
        full_nlp.add_pipe("sentencizer")
        doc = full_nlp(raw)
        nlp = full_nlp
    except Exception as exc_full:
        # Full model failed; warn and try lightweight model
        print(f"⚠️  Falling back to lightweight sentence tokenizer: {exc_full}")
        try:
            lite_nlp = spacy.blank("en")
            lite_nlp.max_length = len(raw) + 1
            lite_nlp.add_pipe("sentencizer")
            doc = lite_nlp(raw)
            nlp = lite_nlp
        except Exception as exc_lite:
            # Even the lightweight pipeline failed (likely due to extreme length).
            # As a last resort, perform naive sentence segmentation using
            # punctuation.  This will disable named entity recognition.
            print(f"⚠️  Could not tokenize even with lightweight pipeline; using naive segmentation: {exc_lite}")
            # Split on sentence end punctuation followed by whitespace.  This is a
            # crude approximation but avoids building huge arrays in spaCy.
            naive_sents = re.split(r"(?<=[\.!?])\s+", raw)
            class _DummySpan:
                def __init__(self, text: str):
                    self.text = text
            class _DummyDoc:
                def __init__(self, sents: list):
                    self.sents = sents
            doc = _DummyDoc([_DummySpan(s) for s in naive_sents])  # type: ignore[assignment]
            nlp = None  # Disable entity extraction on naive mode
    # ``doc`` has already been created in the try/except block above.  Do
    # not recreate it here, as that would reprocess the text and may
    # trigger another memory error.  At this point, ``doc`` and ``nlp``
    # refer to the chosen pipeline and its output.

    # If ``nlp`` is None, this means we are using the naive segmentation
    # fallback due to extreme file size and failed spaCy tokenisation.
    # Running zero‑shot classification on millions of naive segments is
    # impractical and likely to exhaust memory or time budgets.  In this
    # case, we skip the ESG analysis entirely and return an empty
    # result with zero counts.  This avoids hangs and ensures the
    # pipeline continues processing other filings.
    if nlp is None:
        print("⚠️  Extremely large file; performing streaming ESG analysis due to memory constraints…")
        # Perform a streaming analysis on larger text units.  We split
        # the raw text by blank lines to create paragraph‑sized
        # segments.  Processing fewer, larger segments reduces the
        # number of calls to the zero‑shot classifier and lowers
        # memory usage compared to sentence‑level analysis.  Only the
        # first ``MAX_SEGMENTS`` segments will be analysed to ensure
        # reasonable runtime.
        MAX_SEGMENTS = 200  # increase or decrease based on desired coverage
        zero_shot = _load_zero_shot(multi_label=True)
        rel_labels = ["about ESG topics", "not about ESG"]
        esg_labels = list(ESG_KEYWORDS.keys())
        risk_labels = ["positive", "neutral", "negative", "high risk"]
        # Split raw text into paragraphs using two or more newlines
        paragraphs = re.split(r"\n{2,}", raw)
        num_chunks = 0
        risk_counts = {"positive": 0, "neutral": 0, "negative": 0, "high risk": 0}
        esg_counts = {cat: 0 for cat in ESG_KEYWORDS.keys()}
        for idx, para in enumerate(paragraphs):
            if idx >= MAX_SEGMENTS:
                break
            chunk = para.strip()
            if len(chunk) < MIN_CHARS:
                continue
            # Relevance
            rel_out = zero_shot(chunk, candidate_labels=rel_labels, multi_label=False)
            if rel_out["labels"][0] != rel_labels[0] or rel_out["scores"][0] < RELEVANCE_THRESH:
                continue
            # ESG categories
            esg_out = zero_shot(chunk, candidate_labels=esg_labels, multi_label=True)
            cats = [lbl for lbl, sc in zip(esg_out["labels"], esg_out["scores"]) if sc >= ESG_THRESH]
            if not cats:
                continue
            # Risk categories
            risk_out = zero_shot(chunk, candidate_labels=risk_labels, multi_label=True)
            for lbl, sc in zip(risk_out["labels"], risk_out["scores"]):
                if sc >= RISK_THRESH:
                    risk_counts[lbl] += 1
            for cat in cats:
                esg_counts[cat] += 1
            num_chunks += 1
        risk_proportions = {k: (risk_counts[k] / num_chunks if num_chunks else 0.0) for k in risk_counts}
        esg_proportions  = {k: (esg_counts[k]  / num_chunks if num_chunks else 0.0) for k in esg_counts}
        out = {
            "company": SAFE_COMPANY.replace("_", " "),
            "filing_date": FILING_DATE,
            "form_type": FORM_TYPE,
            "esg_results": [],
            "topic_model": {},
            "aggregated_metrics": {
                "num_chunks": num_chunks,
                "risk_counts": risk_counts,
                "risk_proportions": risk_proportions,
                "esg_counts": esg_counts,
                "esg_proportions": esg_proportions,
            },
        }
        output_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
        print("💾 Saved →", output_path)
        return

    # Prefer to run the zero‑shot classification pipeline on the GPU if one
    # is available.  For maximum performance we attempt to use cuda
    # device 0.  If the GPU is unavailable or loading the model fails due
    # to memory constraints, the `_load_zero_shot` helper will
    # gracefully fall back to CPU execution.
    device = 0 if torch.cuda.is_available() else -1

    print("⚙️  Setting up pipelines …")
    # Define a helper to load a zero‑shot classification pipeline.  If
    # loading the default ``facebook/bart-large-mnli`` model fails (e.g.
    # due to insufficient memory), fall back to a smaller model such as
    # ``valhalla/distilbart-mnli-12-1``.  Additional models can be added
    # to this list in order of preference.
    def _load_zero_shot(multi_label: bool) -> Any:
        """
        Load a zero‑shot classification pipeline with GPU preferred.  We
        attempt to load each candidate model on the currently selected
        device.  If loading fails (for example due to insufficient
        memory on the GPU), we automatically retry on the CPU.  Only
        if all models fail on both GPU and CPU will an exception be
        propagated.

        Args:
            multi_label: Whether to enable multi‑label predictions.
        Returns:
            A zero‑shot classification pipeline ready for inference.
        Raises:
            Exception: If no candidate model could be successfully loaded.
        """
        model_candidates = [
            # Smaller models are tried first to reduce memory usage.
            "valhalla/distilbart-mnli-12-1",
            "MoritzLaurer/DeBERTa-v3-base-mnli",
        ]
        last_err: Exception | None = None
        for mn in model_candidates:
            # Attempt to load on the preferred device (GPU if available)
            try:
                return pipeline(
                    "zero-shot-classification",
                    model=mn,
                    tokenizer=mn,
                    device=device,
                    multi_label=multi_label,
                )
            except Exception as exc_gpu:
                last_err = exc_gpu
                # If the failure occurred on GPU, retry on CPU before
                # moving to the next model.
                if device == 0:
                    try:
                        return pipeline(
                            "zero-shot-classification",
                            model=mn,
                            tokenizer=mn,
                            device=-1,
                            multi_label=multi_label,
                        )
                    except Exception as exc_cpu:
                        last_err = exc_cpu
                # Continue to next candidate model
                continue
        # If all attempts failed, propagate the last error
        raise last_err if last_err else RuntimeError("No zero-shot model could be loaded")

    # Load a single zero‑shot classification pipeline.  We use a
    # single pipeline for all classification tasks to avoid loading
    # multiple large models into memory.  The pipeline is configured
    # with ``multi_label=True``; however, we explicitly override the
    # ``multi_label`` flag when calling it for single‑label tasks.
    zero_shot_pipe = _load_zero_shot(multi_label=True)
    # Label sets for each classification step
    rel_labels = ["about ESG topics", "not about ESG"]
    esg_labels = list(ESG_KEYWORDS.keys())
    risk_labels = ["positive", "neutral", "negative", "high risk"]

    # Collect ESG-relevant chunks
    results = []
    print("🔍 Scanning chunks …")
    for chunk in tqdm(split_into_chunks(doc)):
        if not is_human_text(chunk):
            continue

        # First, determine if the chunk is ESG‑related.  Use
        # single‑label mode for relevance: we expect one of the two
        # labels to be the best match.
        rel_out = zero_shot_pipe(chunk,
                                 candidate_labels=rel_labels,
                                 multi_label=False)
        if rel_out["labels"][0] != rel_labels[0] or \
           rel_out["scores"][0] < RELEVANCE_THRESH:
            continue
        # Classify ESG categories using multi‑label mode
        esg_out = zero_shot_pipe(chunk,
                                 candidate_labels=esg_labels,
                                 multi_label=True)
        cats = [lbl for lbl, sc in zip(esg_out["labels"], esg_out["scores"]) if sc >= ESG_THRESH]
        if not cats:
            continue
        # Classify risk tone using multi‑label mode
        risk_out = zero_shot_pipe(chunk,
                                  candidate_labels=risk_labels,
                                  multi_label=True)
        risks = {lbl: round(sc, 3)
                 for lbl, sc in zip(risk_out["labels"], risk_out["scores"])
                 if sc >= RISK_THRESH}

        # Perform named entity recognition if a model with NER is available.
        if nlp:
            try:
                ent_doc = nlp(chunk)
                entities = [
                    {"text": ent.text, "label": ent.label_}
                    for ent in ent_doc.ents
                    if ent.label_ in {"PERSON", "ORG", "LAW"}
                ]
            except Exception:
                # If entity extraction fails, default to empty list
                entities = []
        else:
            # When using naive segmentation, skip entity extraction
            entities = []

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

    # -------------------------------------------------------------------
    # Aggregate metrics across all ESG-relevant chunks.  These summarise the
    # overall tone and ESG alignment of the report.  Even if topic modelling
    # is skipped, we still compute these metrics.
    num_chunks = len(results)
    risk_labels_list = ["positive", "neutral", "negative", "high risk"]
    risk_counts: dict[str, int] = {lbl: 0 for lbl in risk_labels_list}
    esg_counts: dict[str, int] = {cat: 0 for cat in ESG_KEYWORDS.keys()}
    for res in results:
        for lbl in risk_labels_list:
            if lbl in res["risk_scores"]:
                risk_counts[lbl] += 1
        for cat in res["esg_categories"]:
            esg_counts[cat] += 1
    risk_proportions = {k: (risk_counts[k] / num_chunks if num_chunks else 0.0) for k in risk_labels_list}
    esg_proportions = {k: (esg_counts[k] / num_chunks if num_chunks else 0.0) for k in ESG_KEYWORDS.keys()}

    # Write final JSON with aggregated metrics included
    out = {
        "company":     SAFE_COMPANY.replace("_", " "),
        "filing_date": FILING_DATE,
        "form_type":   FORM_TYPE,
        "esg_results": results,
        "topic_model": topic_info,
        "aggregated_metrics": {
            "num_chunks": num_chunks,
            "risk_counts": risk_counts,
            "risk_proportions": risk_proportions,
            "esg_counts": esg_counts,
            "esg_proportions": esg_proportions,
        },
    }
    output_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("💾 Saved →", output_path)


if __name__ == "__main__":
    import sys
    main(*sys.argv[1:])
