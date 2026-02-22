"""Shared metaxy feature definitions for dagster-slurm examples.

These feature specs define the metadata schema for each processing stage.
They are imported both by the Dagster asset layer (for store operations)
and by HPC workloads (for MetaxyDatasource/MetaxyDatasink).

Three feature pairs are defined:
1. Simple example (DuckDB): raw_numbers -> processed_numbers (+ partitioned variant)
2. Ray example (DeltaLake): input_texts -> embeddings
3. Docling example (DeltaLake): source_documents -> converted_documents
"""

import metaxy as mx

# ---------------------------------------------------------------------------
# Example 1: Simple metaxy + dagster (DuckDB store)
# ---------------------------------------------------------------------------


class RawNumbers(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="example/raw_numbers",
        id_columns=["sample_uid"],
        fields=["value", "category"],
    ),
):
    sample_uid: str
    value: float
    category: str


class ProcessedNumbers(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="example/processed_numbers",
        id_columns=["sample_uid"],
        fields=["result", "value_bucket"],
        deps=[RawNumbers],
    ),
):
    sample_uid: str
    result: float
    value_bucket: str


class PartitionedProcessedNumbers(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="example/partitioned_processed_numbers",
        id_columns=["sample_uid"],
        fields=["result", "value_bucket"],
        deps=[RawNumbers],
    ),
):
    sample_uid: str
    result: float
    value_bucket: str


# ---------------------------------------------------------------------------
# Example 2: metaxy + dagster + Ray (DeltaLake store)
# ---------------------------------------------------------------------------


class InputTexts(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="ray_example/input_texts",
        id_columns=["sample_uid"],
        fields=["text"],
    ),
):
    sample_uid: str
    text: str


class Embeddings(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="ray_example/embeddings",
        id_columns=["sample_uid"],
        fields=["embedding"],
        deps=[InputTexts],
    ),
):
    sample_uid: str
    embedding: list[float]


# ---------------------------------------------------------------------------
# Example 3: metaxy-enabled docling pipeline (DeltaLake store)
# ---------------------------------------------------------------------------


class SourceDocuments(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="docling/source_documents",
        id_columns=["doc_uid"],
        fields=["source_path", "file_size_bytes"],
    ),
):
    doc_uid: str
    source_path: str
    file_size_bytes: int


class ConvertedDocuments(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="docling/converted_documents",
        id_columns=["doc_uid"],
        fields=["markdown_path", "num_pages", "conversion_status", "elapsed_s"],
        deps=[SourceDocuments],
    ),
):
    doc_uid: str
    markdown_path: str
    num_pages: int
    conversion_status: str
    elapsed_s: float
