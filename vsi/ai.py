"""AI-ready layer: complaint narratives as clean, scrubbed, metadata-rich documents, plus a similarity index.

gold.complaint_documents  one row per complaint (not per component): the consumer's narrative with personal
                          details replaced by placeholders, the vehicle, all components named, dates and severity flags.
                          This is what a search engine, an LLM (retrieval-augmented generation) or a classifier should
                          read, instead of the raw narrative in silver (which contains phone numbers and emails).
data/ai/                  a TF-IDF similarity index over recent documents, so `vsi search "..."` finds similar
                          complaints with no external API. The same table could feed an embedding model instead.

Narratives are at most 2,048 characters (~500 tokens), so one complaint = one document; no chunking needed.
"""
import json
import pickle
import time

import numpy as np
from pyspark.sql import functions as F

from .config import AI_DIR, OUT_DOCS, table
from .spark import get_spark

INDEX_DIR = AI_DIR
INDEX_FROM_YEAR = 2015          # index the last ~10 years (about 1M documents) to keep memory laptop-friendly

# Order matters: emails before phones (an email can contain digits), VIN before generic numbers.
PII_PATTERNS = [
    ("EMAIL", r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    ("URL", r"(?i)\b(?:https?://|www\.)\S+"),
    ("VIN", r"\b[A-HJ-NPR-Z0-9]{17}\b"),
    ("PHONE", r"(?<!\d)(?:\+?1[ .-]?)?\(?\d{3}\)?[ .-]?\d{3}[ .-]?\d{4}(?!\d)"),
]


def scrub(col):
    """Spark expression: replace each personal detail with a placeholder like [PHONE]."""
    for label, pattern in PII_PATTERNS:
        col = F.regexp_replace(col, pattern, f"[{label}]")
    return col


def build_documents():
    spark = get_spark()
    c = spark.read.format("delta").load(table("silver", "complaints")).where("NOT _is_deleted")
    docs = (c.groupBy("odino").agg(
                F.first("narrative", ignorenulls=True).alias("narrative"),
                F.first("make").alias("make"), F.first("model").alias("model"), F.first("model_year").alias("model_year"),
                F.sort_array(F.collect_set("component_system")).alias("component_systems"),
                F.min("received_date").alias("received_date"), F.min("fail_date").alias("fail_date"),
                F.max(F.coalesce("crash", F.lit(False))).alias("crash"), F.max(F.coalesce("fire", F.lit(False))).alias("fire"),
                F.max("injured").alias("injured"), F.max("deaths").alias("deaths"), F.first("state").alias("state"))
            .where(F.length("narrative") >= 20)          # empty / placeholder narratives are useless to a model
            .withColumn("text", scrub(F.col("narrative"))))
    for label, _ in PII_PATTERNS:
        docs = docs.withColumn(f"redacted_{label.lower()}", F.size(F.split("text", rf"\[{label}\]")) - 1)
    docs = (docs.withColumnRenamed("odino", "doc_id").drop("narrative")
                .withColumn("n_chars", F.length("text"))
                .withColumn("approx_tokens", F.ceil(F.length("text") / 4))
                .withColumn("content_hash", F.sha2("text", 256)))
    docs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table("gold", "complaint_documents"))
    d = spark.read.format("delta").load(table("gold", "complaint_documents"))
    stats = d.agg(F.count("*").alias("documents"), *[F.sum(f"redacted_{l.lower()}").alias(l.lower()) for l, _ in PII_PATTERNS],
                  *[F.sum((F.col(f"redacted_{l.lower()}") > 0).cast("int")).alias(f"docs_with_{l.lower()}") for l, _ in PII_PATTERNS],
                  F.expr("percentile(approx_tokens, 0.5)").alias("median_tokens")).collect()[0].asDict()
    print("gold.complaint_documents: " + ", ".join(f"{k}={v:,}" for k, v in stats.items()))
    return stats


def build_index():
    from deltalake import DeltaTable
    from sklearn.feature_extraction.text import TfidfVectorizer
    t0 = time.time()
    tbl = DeltaTable(table("gold", "complaint_documents")).to_pyarrow_table(
        columns=["doc_id", "text", "make", "model", "model_year", "component_systems", "received_date", "crash", "fire"])
    df = tbl.to_pandas()
    recent = df[df["received_date"].map(lambda d: d is not None and d.year >= INDEX_FROM_YEAR)]
    if len(recent) >= 1000:              # a small sample (VSI_SAMPLE_ROWS) may hold only old complaints: index all of it
        df = recent
    df = df.reset_index(drop=True)
    vec = TfidfVectorizer(stop_words="english", min_df=5 if len(df) > 50_000 else 1, max_df=0.5, max_features=150_000, ngram_range=(1, 2),
                          sublinear_tf=True, dtype=np.float32)
    X = vec.fit_transform(df["text"].str.lower())
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    meta = df.drop(columns=["text"]).copy()
    meta["received_date"] = meta["received_date"].astype(str)
    with (INDEX_DIR / "index.pkl").open("wb") as f:
        pickle.dump({"vectorizer": vec, "matrix": X, "meta": meta, "text": df["text"].tolist()}, f, protocol=5)
    info = {"documents": int(X.shape[0]), "terms": int(X.shape[1]), "nonzeros": int(X.nnz), "from_year": INDEX_FROM_YEAR,
            "build_seconds": round(time.time() - t0, 1)}
    (INDEX_DIR / "index.json").write_text(json.dumps(info))
    print(f"similarity index: {info}")
    return info


_INDEX = None


def search(query: str, k: int = 5, make: str | None = None) -> list[dict]:
    global _INDEX
    if _INDEX is None:
        with (INDEX_DIR / "index.pkl").open("rb") as f:
            _INDEX = pickle.load(f)
    q = _INDEX["vectorizer"].transform([query.lower()])
    scores = (_INDEX["matrix"] @ q.T).toarray().ravel()      # rows are L2-normalised, so this is cosine similarity
    if make:
        scores = np.where(_INDEX["meta"]["make"].values == make.upper(), scores, 0)
    top = np.argpartition(-scores, k)[:k]
    top = top[np.argsort(-scores[top])]
    out = []
    for i in top:
        m = _INDEX["meta"].iloc[i]
        out.append({"score": round(float(scores[i]), 3), "doc_id": m["doc_id"],
                    "vehicle": f"{'' if m['model_year'] != m['model_year'] else int(m['model_year'])} {m['make']} {m['model']}".strip(), "systems": list(m["component_systems"]),
                    "received": m["received_date"], "text": _INDEX["text"][i][:300]})
    return out


def build(load_date: str | None = None):
    stats = build_documents()
    info = build_index()
    OUT_DOCS.mkdir(parents=True, exist_ok=True)
    (OUT_DOCS / "ai-readiness.json").write_text(json.dumps({"documents": stats, "index": info}, default=str, indent=2))
