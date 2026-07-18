"""Download and prepare the Reuters-21578 corpus for LDA training.

Downloads the tarball (unless ``REUTERS_LOCAL_PATH`` points at an
existing copy), parses the SGML files with a small regex parser,
builds one shared vocabulary over the whole corpus and writes one
parquet per month. The shared vocabulary is what
makes topic-term vectors comparable across the per-(month, seed) LDA
models trained downstream.

Environment:
    RAPIDS_TOPICS_BASE   output base dir (default: $HOME/rapids_topics)
    REUTERS_URL          tarball URL (default: UCI KDD mirror)
    REUTERS_LOCAL_PATH   optional pre-downloaded reuters21578.tar.gz
    REUTERS_SHA256       expected tarball digest; "" disables the check
    MAX_FEATURES         vocabulary size cap (default: 20000)
"""

import hashlib
import os
import re
import tarfile
import urllib.request
from collections import Counter
from pathlib import Path

from dagster_pipes import PipesContext, open_dagster_pipes

DEFAULT_REUTERS_URL = (
    "https://kdd.ics.uci.edu/databases/reuters21578/reuters21578.tar.gz"
)

DEFAULT_REUTERS_SHA256 = (
    "3bae43c9b14e387f76a61b6d82bf98a4fb5d3ef99ef7e7075ff2ccbcf59f9d30"
)


def verify_sha256(path: Path, expected: str) -> None:
    """Raise if the file's sha256 does not match ``expected``."""
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != expected:
        raise RuntimeError(
            f"Checksum mismatch for {path}: expected {expected}, got {digest}. "
            "The download may be corrupt or the mirror changed; set "
            "REUTERS_SHA256 to override or '' to disable."
        )


_MONTHS = {
    "JAN": "01",
    "FEB": "02",
    "MAR": "03",
    "APR": "04",
    "MAY": "05",
    "JUN": "06",
    "JUL": "07",
    "AUG": "08",
    "SEP": "09",
    "OCT": "10",
    "NOV": "11",
    "DEC": "12",
}

_DOC_RE = re.compile(r"<REUTERS[^>]*>(.*?)</REUTERS>", re.DOTALL)
_DATE_RE = re.compile(r"<DATE>\s*(\d{1,2})-([A-Z]{3})-(\d{4})", re.DOTALL)
_TITLE_RE = re.compile(r"<TITLE>(.*?)</TITLE>", re.DOTALL)
_BODY_RE = re.compile(r"<BODY>(.*?)(?:&#3;)?</BODY>", re.DOTALL)


def _unescape(text: str) -> str:
    return (
        text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&#3;", "")
    )


def parse_sgml_docs(sgml: str) -> list[dict]:
    """Extract (month, title, body) records from one Reuters .sgm file."""
    docs = []
    for match in _DOC_RE.finditer(sgml):
        chunk = match.group(1)
        date_m = _DATE_RE.search(chunk)
        body_m = _BODY_RE.search(chunk)
        if not date_m or not body_m:
            continue
        month_abbr = date_m.group(2)
        if month_abbr not in _MONTHS:
            continue
        title_m = _TITLE_RE.search(chunk)
        docs.append(
            {
                "month": f"{date_m.group(3)}-{_MONTHS[month_abbr]}",
                "title": _unescape(title_m.group(1).strip()) if title_m else "",
                "body": _unescape(body_m.group(1).strip()),
            }
        )
    return docs


def _fetch_tarball(context: PipesContext, work_dir: Path) -> Path:
    expected_sha = os.environ.get("REUTERS_SHA256", DEFAULT_REUTERS_SHA256).strip()

    local = os.environ.get("REUTERS_LOCAL_PATH", "").strip()
    if local:
        context.log.info(f"Using local Reuters tarball: {local}")
        target = Path(local)
    else:
        url = os.environ.get("REUTERS_URL", DEFAULT_REUTERS_URL)
        target = work_dir / "reuters21578.tar.gz"
        if target.exists():
            context.log.info(f"Reusing cached tarball: {target}")
        else:
            context.log.info(f"Downloading {url} ...")
            urllib.request.urlretrieve(url, target)
            context.log.info(f"Downloaded {target.stat().st_size / 1e6:.1f} MB")

    if expected_sha:
        verify_sha256(target, expected_sha)
        context.log.info("Tarball sha256 verified")
    return target


def _load_docs(context: PipesContext, tarball: Path) -> list[dict]:
    docs: list[dict] = []
    with tarfile.open(tarball, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.name.endswith(".sgm"):
                continue
            fh = tar.extractfile(member)
            if fh is None:
                continue
            docs.extend(parse_sgml_docs(fh.read().decode("latin-1")))
    context.log.info(f"Parsed {len(docs)} documents with date + body")
    return docs


def main():
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq
    from sklearn.feature_extraction.text import CountVectorizer

    context = PipesContext.get()
    base = Path(
        os.path.expandvars(os.environ.get("RAPIDS_TOPICS_BASE", "$HOME/rapids_topics"))
    ).expanduser()
    corpus_dir = base / "corpus"
    corpus_dir.mkdir(parents=True, exist_ok=True)

    docs = _load_docs(context, _fetch_tarball(context, corpus_dir))

    for i, doc in enumerate(docs):
        doc["doc_id"] = i

    max_features = int(os.environ.get("MAX_FEATURES", "20000"))
    vectorizer = CountVectorizer(
        min_df=5,
        max_df=0.5,
        max_features=max_features,
    )
    vectorizer.fit(d["body"] for d in docs)
    vocabulary_path = corpus_dir / "vocabulary.json"
    vocabulary_path.write_text(
        json.dumps(vectorizer.vocabulary_, default=int, sort_keys=True),
        encoding="utf-8",
    )
    context.log.info(
        f"Vocabulary: {len(vectorizer.vocabulary_)} terms -> {vocabulary_path}"
    )

    month_counts: Counter[str] = Counter(d["month"] for d in docs)
    for month in sorted(month_counts):
        month_docs = [d for d in docs if d["month"] == month]
        table = pa.table(
            {
                "doc_id": [d["doc_id"] for d in month_docs],
                "month": [d["month"] for d in month_docs],
                "title": [d["title"] for d in month_docs],
                "body": [d["body"] for d in month_docs],
            }
        )
        out = corpus_dir / f"month={month}"
        out.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, out / "docs.parquet")
        context.log.info(f"  {month}: {len(month_docs)} docs")

    context.report_asset_materialization(
        metadata={
            "n_docs": len(docs),
            "n_months": len(month_counts),
            "vocab_size": len(vectorizer.vocabulary_),
            "months": ", ".join(f"{m}:{c}" for m, c in sorted(month_counts.items())),
            "output_dir": str(corpus_dir),
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
