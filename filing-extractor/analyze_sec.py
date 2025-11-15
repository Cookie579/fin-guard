import re
import json
from pathlib import Path

import nltk
from nltk.corpus import wordnet as wn

import spacy
from typing import Any
from tqdm import tqdm
from transformers import pipeline
from bertopic import BERTopic
import torch
from hdbscan import HDBSCAN
from umap import UMAP

# ────────────────────────────────────────────────────────────────
# CONFIG
#
# Default configuration values for the SEC analysis. These values can be
# overridden by passing ``input_file`` or ``output_file`` to :func:`main`.
#
# ``INPUT_FILE`` and ``OUTPUT_FILE`` are placeholders; ``INPUT_FILE`` is
# updated to the actual file path passed in at runtime.  ``WINDOW_SIZE`` and
# ``STRIDE`` control the number of sentences per chunk and how far the
# window advances, respectively.  ``MIN_CHARS`` defines the minimum number
# of characters required for a chunk to be analysed.  ``ESG_THRESH`` and
# ``RISK_THRESH`` define confidence score thresholds for ESG and risk
# predictions.  ``RELEVANCE_THRESH`` filters out non-ESG text.  ``TAG_RATIO_MAX``
# helps filter out boilerplate HTML or markup based on the ratio of tag-like
# tokens.  ``TOPIC_MIN_CLUSTER`` and ``TOPIC_MIN_SAMPLES`` tune the HDBSCAN
# clustering used in BERTopic, while ``TOPIC_MODEL_MIN_CHUNKS`` sets the
# minimum number of ESG-relevant chunks required before running topic
# modelling.  See the accompanying architecture document for details.
INPUT_FILE             = "texts/Example/10-K/Example__2000-01-01__10-K.txt"
OUTPUT_FILE            = "esg-outputs/10-K/Example__2000-01-01__10-K.json"
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


# -----------------------------------------------------------------------------
# Note: Parsing of company name, filing date and form type is deferred until
# runtime in :func:`main`.  The values defined below are placeholders only.
SAFE_COMPANY: str | None = None
FILING_DATE: str | None = None
FORM_TYPE: str | None = None
COMPANY_NAME: str | None = None


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

# Allow passing custom input/output paths from batch script
def main(input_file=None, output_file=None):
    global INPUT_FILE, OUTPUT_FILE


    global INPUT_FILE, OUTPUT_FILE, SAFE_COMPANY, FILING_DATE, FORM_TYPE, COMPANY_NAME

    # Update input/output paths from parameters
    if input_file:
        INPUT_FILE = str(input_file)
    if output_file:
        OUTPUT_FILE = str(output_file)

    # Parse metadata from the input filename.  Expected format:
    #   <Company>__<YYYY-MM-DD>__<Form>.txt
    stem = Path(INPUT_FILE).stem
    try:
        parts = stem.split("__")
        if len(parts) != 3:
            raise ValueError(f"Invalid SEC filename format: {stem}")
        SAFE_COMPANY, FILING_DATE, FORM_TYPE = parts
    except Exception as exc:
        raise ValueError(f"❌ Could not parse metadata from filename: {stem}") from exc
    COMPANY_NAME = SAFE_COMPANY.replace("_", " ")
    # If no explicit output file was provided, construct one based on the
    # metadata.  The default location is under esg-outputs/<form>/...
    if not output_file:
        form_lower = FORM_TYPE.lower()
        OUTPUT_FILE = f"esg-outputs/{form_lower}/{SAFE_COMPANY}__{FILING_DATE}__{FORM_TYPE}.json"

    print("📖 Loading 10-K …")
    raw = load_text(INPUT_FILE)

    print("🧠 Loading spaCy & segmenting …")
    # Load the small English model and increase max_length to support very
    # large filings.  Without this, spaCy will raise a ValueError for
    # documents longer than 1 million characters.  We also add the
    # Sentencizer to avoid loading the parser and NER, reducing memory.
    # Dynamically choose between the full English model and a lightweight
    # blank model based on the size of the input.  For smaller filings
    # we prefer the full ``en_core_web_sm`` model to maximise accuracy.
    # If loading or processing with the full model fails (due to
    # memory), we fall back to a blank model with only a Sentencizer.
    # Dynamically choose between full and lightweight tokenizers.  If the
    # full model fails (e.g. due to memory constraints), we fall back to
    # a blank pipeline.  If that also fails we perform naive
    # segmentation.  ``nlp`` will be ``None`` in the naive case.
    nlp = None  # type: ignore[var‑annotated]
    doc = None  # type: ignore[var‑annotated]
    try:
        full_nlp = spacy.load("en_core_web_sm")
        full_nlp.max_length = max(full_nlp.max_length, len(raw) + 1)
        full_nlp.add_pipe("sentencizer")
        doc = full_nlp(raw)
        nlp = full_nlp
    except Exception as exc_full:
        print(f"⚠️  Falling back to lightweight sentence tokenizer: {exc_full}")
        try:
            lite_nlp = spacy.blank("en")
            lite_nlp.max_length = len(raw) + 1
            lite_nlp.add_pipe("sentencizer")
            doc = lite_nlp(raw)
            nlp = lite_nlp
        except Exception as exc_lite:
            print(f"⚠️  Could not tokenize even with lightweight pipeline; using naive segmentation: {exc_lite}")
            naive_sents = re.split(r"(?<=[\.!?])\s+", raw)
            class _DummySpan:
                def __init__(self, text: str):
                    self.text = text
            class _DummyDoc:
                def __init__(self, sents: list):
                    self.sents = sents
            doc = _DummyDoc([_DummySpan(s) for s in naive_sents])  # type: ignore[assignment]
            nlp = None
    # ``doc`` has already been created within the try/except above.  Do
    # not recreate it here to avoid reprocessing the text and
    # potentially triggering memory issues.  ``doc`` now refers to the
    # document produced by the selected pipeline.

    # If ``nlp`` is None, we resorted to naive segmentation because both
    # the full and lightweight spaCy models failed due to memory
    # constraints.  Running the zero‑shot classifier on this huge text
    # would be extremely resource‑intensive and likely to cause
    # additional failures.  Instead, skip SEC analysis for this
    # document, outputting an empty result with zero counts.  This
    # prevents the pipeline from stalling on oversized filings.
    if nlp is None:
        print("⚠️  Extremely large file; performing streaming SEC analysis due to memory constraints…")
        # Stream through larger text units.  Split the raw text by two or
        # more consecutive newlines to create paragraph‑sized segments.
        # This reduces the total number of segments analysed and thus
        # lowers memory requirements.  Only the first ``MAX_SEGMENTS``
        # segments are processed.
        MAX_SEGMENTS = 200
        zero_shot = _load_zero_shot(multi_label=True)
        rel_labels = ["about ESG topics", "not about ESG"]
        esg_labels = list(ESG_KEYWORDS.keys())
        risk_labels = ["positive", "neutral", "negative", "high risk"]
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
            rel_out = zero_shot(chunk, candidate_labels=rel_labels, multi_label=False)
            if rel_out["labels"][0] != rel_labels[0] or rel_out["scores"][0] < RELEVANCE_THRESH:
                continue
            esg_out = zero_shot(chunk, candidate_labels=esg_labels, multi_label=True)
            cats = [lbl for lbl, sc in zip(esg_out["labels"], esg_out["scores"]) if sc >= ESG_THRESH]
            if not cats:
                continue
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
            "company": COMPANY_NAME,
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
        Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(OUTPUT_FILE).write_text(json.dumps(out, indent=2), encoding="utf-8")
        print("💾 Saved →", OUTPUT_FILE)
        return

    # Prefer using the GPU if available.  Running zero‑shot models on
    # CUDA can accelerate inference significantly.  If the GPU has
    # insufficient memory, the `_load_zero_shot` helper will retry on
    # CPU automatically.
    device = 0 if torch.cuda.is_available() else -1

    print("⚙️  Setting up pipelines …")
    # Helper to load a zero-shot classification pipeline.  If the default
    # ``facebook/bart-large-mnli`` model cannot be loaded (for example
    # because the available memory or paging file is too small), fall
    # back to progressively smaller models.  Additional candidates can
    # be added to this list as needed.
    def _load_zero_shot(multi_label: bool) -> Any:
        """
        Load a zero‑shot classification pipeline, preferring GPU usage.
        Each candidate model is first attempted on the current device
        (`device`).  If loading fails (for example due to insufficient
        GPU memory), the same model is retried on the CPU.  Only if
        all attempts across all models fail will the last exception be
        raised.

        Args:
            multi_label: Whether to enable multi‑label classification.
        Returns:
            A ready‑to‑use transformers pipeline for zero‑shot
            classification.
        Raises:
            Exception: If no model could be loaded on either GPU or CPU.
        """
        model_candidates = [
            "valhalla/distilbart-mnli-12-1",
            "MoritzLaurer/DeBERTa-v3-base-mnli",
        ]
        last_err: Exception | None = None
        for mn in model_candidates:
            try:
                # Prefer GPU if available
                return pipeline(
                    "zero-shot-classification",
                    model=mn,
                    tokenizer=mn,
                    device=device,
                    multi_label=multi_label,
                )
            except Exception as exc_gpu:
                last_err = exc_gpu
                # If we attempted on GPU, retry on CPU
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
                continue
        raise last_err if last_err else RuntimeError("No zero-shot model could be loaded")

    # Load a single zero‑shot pipeline for all classification tasks.
    # Using a single pipeline reduces memory usage compared to
    # instantiating multiple pipelines for relevance, ESG categories and
    # risk.  We will override the ``multi_label`` argument when
    # calling the pipeline.
    zero_shot_pipe = _load_zero_shot(multi_label=True)
    rel_labels = ["about ESG topics", "not about ESG"]
    esg_labels = list(ESG_KEYWORDS.keys())
    risk_labels = ["positive", "neutral", "negative", "high risk"]

    results = []
    print("🔍 Scanning chunks …")
    for chunk in tqdm(split_into_chunks(doc)):
        if not is_human_text(chunk):
            continue

        # Determine if the chunk is ESG‑related.  Use single‑label mode
        # for relevance to select the best matching label.
        rel_out = zero_shot_pipe(chunk,
                                 candidate_labels=rel_labels,
                                 multi_label=False)
        if rel_out["labels"][0] != rel_labels[0] or rel_out["scores"][0] < RELEVANCE_THRESH:
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
        risks = {lbl: round(sc, 3) for lbl, sc in zip(risk_out["labels"], risk_out["scores"]) if sc >= RISK_THRESH}

        # Perform named entity recognition if a spaCy pipeline is available.
        if nlp:
            try:
                ent_doc = nlp(chunk)
                entities = [
                    {"text": ent.text, "label": ent.label_}
                    for ent in ent_doc.ents
                    if ent.label_ in {"PERSON", "ORG", "LAW"}
                ]
            except Exception:
                entities = []
        else:
            entities = []

        results.append({
            "text": highlight_all(chunk),
            "esg_categories": cats,
            "esg_confidences": {lbl: round(sc, 3) for lbl, sc in zip(esg_out["labels"], esg_out["scores"])},
            "risk_scores": risks,
            "entities": entities
        })

    print(f"✅ Found {len(results)} ESG-relevant chunks.")
    
    if len(results) < TOPIC_MODEL_MIN_CHUNKS:
        print(f"⚠️ Only {len(results)} ESG-relevant chunks found. Skipping topic modeling.")
        out = {
            "company": COMPANY_NAME,
            "filing_date": FILING_DATE,
            "form_type": FORM_TYPE,
            "esg_results": results,
            "topic_model": {}  # empty, but valid
        }
        Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(OUTPUT_FILE).write_text(json.dumps(out, indent=2), encoding="utf-8")
        print("💾 Saved →", OUTPUT_FILE)
        return

    # Perform topic modelling on the ESG-relevant chunks, if enough data exists.
    print("📊 Running topic modeling …")
    topic_info: dict[str, dict] = {}
    for cat in ESG_KEYWORDS.keys():
        # Collect texts belonging to this ESG category
        texts = [r["text"] for r in results if cat in r["esg_categories"]]
        # Require a minimum number of samples to avoid spurious clusters
        if len(texts) < TOPIC_MODEL_MIN_CHUNKS:
            print(f"⚠️ Skipping topic modeling for {cat} — only {len(texts)} relevant texts.")
            continue
        n_topics = len(texts)
        # Further skip if there are too few unique samples for clustering
        if n_topics <= 3:
            print(f"⚠️ Skipping topic modeling for {cat} — insufficient samples ({n_topics}).")
            continue
        # Determine embedding and clustering parameters dynamically
        n_components = min(5, max(2, n_topics - 1))
        n_neighbors = min(10, max(2, n_topics - 2))
        umap_model = UMAP(n_components=n_components, n_neighbors=n_neighbors, random_state=42)
        cluster_model = HDBSCAN(min_samples=TOPIC_MIN_SAMPLES, min_cluster_size=TOPIC_MIN_CLUSTER)
        tm = BERTopic(umap_model=umap_model, hdbscan_model=cluster_model)
        try:
            _topics, _probs = tm.fit_transform(texts)
        except Exception as e:
            print(f"❌ Failed topic modeling for {cat}: {e}")
            continue
        # Capture frequency counts and top terms per topic
        freq_df = tm.get_topic_freq().rename(columns={"Topic": "topic", "Count": "count"})
        freq_records = freq_df.to_dict(orient="records")
        terms = tm.get_topics()
        topic_info[cat] = {"frequencies": freq_records, "terms": terms}

    # -------------------------------------------------------------------
    # Aggregate metrics across all ESG-relevant chunks. These metrics
    # summarise the overall tone and ESG alignment of the filing.  They are
    # computed even if topic modelling is skipped.
    num_chunks = len(results)
    risk_labels_list = ["positive", "neutral", "negative", "high risk"]
    risk_counts: dict[str, int] = {lbl: 0 for lbl in risk_labels_list}
    esg_counts: dict[str, int] = {cat: 0 for cat in ESG_KEYWORDS.keys()}
    for res in results:
        # Count risk label occurrences (presence, not weighted by score)
        for lbl in risk_labels_list:
            if lbl in res["risk_scores"]:
                risk_counts[lbl] += 1
        # Count ESG categories
        for cat in res["esg_categories"]:
            esg_counts[cat] += 1
    # Normalised proportions
    risk_proportions = {k: (risk_counts[k] / num_chunks if num_chunks else 0.0) for k in risk_labels_list}
    esg_proportions = {k: (esg_counts[k] / num_chunks if num_chunks else 0.0) for k in ESG_KEYWORDS.keys()}

    out = {
        "company": COMPANY_NAME,
        "filing_date": FILING_DATE,
        "form_type": FORM_TYPE,
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
    # Ensure output directory exists
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_FILE).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("💾 Saved →", OUTPUT_FILE)


if __name__ == "__main__":
    main()
