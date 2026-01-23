"""Docling document processing with Ray for distributed PDF conversion.

This example processes PDF documents using docling and Ray Data for parallel processing.
Adapted for dagster-slurm integration to demonstrate HPC document processing workflows.

Based on: https://github.com/l-mds/duckpond/blob/main/projects/100_combined/ai_example/
"""

import glob
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import ray
import ray.data as rd
from dagster_pipes import DagsterPipesError, PipesContext, open_dagster_pipes
from dagster_slurm_example_shared import get_base_output_path
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import ConversionResult, ConversionStatus
from docling.document_converter import DocumentConverter
from docling_core.types.doc import ImageRefMode

# Optional: Advanced OCR with RapidOCR
# Uncomment these imports to enable RapidOCRDocumentConverter
# from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
# from docling.datamodel.pipeline_options import RapidOcrOptions, ThreadedPdfPipelineOptions


@dataclass
class ConvertSummary:
    """Summary of document conversion results."""

    ok: bool
    src_path: str
    pages: Optional[int]
    elapsed_s: float
    output_dir: Optional[str]
    error: Optional[str] = None


class BasicDocumentConverter:
    """Basic docling converter for PDF documents.

    This is the simple converter that works out-of-the-box with minimal configuration.
    For more advanced OCR capabilities, use RapidOCRDocumentConverter below.

    Based on: https://github.com/l-mds/duckpond/blob/main/projects/100_combined/ai_example/ai_example/defs/document_ai/document_plain_simple.py
    """

    def __init__(self, document_format: InputFormat = InputFormat.PDF):
        self.converter = DocumentConverter()
        self.converter.initialize_pipeline(document_format)

    def convert_one(self, uri: str) -> ConversionResult:
        """Convert a single document."""
        return self.converter.convert(uri)


# Uncomment this class to use advanced RapidOCR-based conversion
# You'll also need to uncomment the imports at the top of this file
#
# class RapidOCRDocumentConverter:
#     """Advanced docling converter with RapidOCR for better OCR quality.
#
#     This converter uses RapidOCR models for higher quality text extraction
#     from scanned documents and images within PDFs.
#
#     Based on: https://github.com/l-mds/duckpond/blob/main/projects/100_combined/ai_example/ai_example/defs/document_ai/document_plain_rapidocr.py
#
#     Requirements:
#     - RapidOCR model files (see rapidocr_models_root configuration)
#     - Additional dependencies: psutil
#     """
#
#     def __init__(self, models_root: Optional[str] = None):
#         self.format = InputFormat.PDF
#         self.models_root = models_root or os.getenv("RAPIDOCR_MODELS_ROOT", "/path/to/models")
#         self.converter = DocumentConverter(
#             format_options={
#                 self.format: PdfFormatOption(
#                     pipeline_options=self._configure_converter()
#                 )
#             }
#         )
#         self.converter.initialize_pipeline(self.format)
#
#     def _configure_converter(self):
#         """Configure the converter with RapidOCR options."""
#         pipeline_options = ThreadedPdfPipelineOptions()
#         pipeline_options.do_ocr = True
#         pipeline_options.images_scale = 2
#         pipeline_options.generate_page_images = False
#         pipeline_options.do_picture_classification = True
#         pipeline_options.generate_picture_images = True
#
#         # Configure RapidOCR model paths
#         det_model_path = os.path.join(
#             self.models_root, "PP-OCRv5/det/ch_PP-OCRv5_server_det.onnx"
#         )
#         rec_model_path = os.path.join(
#             self.models_root, "PP-OCRv5/rec/latin_PP-OCRv5_rec_mobile_infer.onnx"
#         )
#         cls_model_path = os.path.join(
#             self.models_root, "PP-OCRv4/cls/ch_ppocr_mobile_v2.0_cls_infer.onnx"
#         )
#         rec_font_path = os.path.join(self.models_root, "resources/fonts/FZYTK.TTF")
#
#         pipeline_options.ocr_options = RapidOcrOptions(
#             det_model_path=det_model_path,
#             rec_model_path=rec_model_path,
#             cls_model_path=cls_model_path,
#             rec_font_path=rec_font_path,
#         )
#         pipeline_options.ocr_options.lang = ["en"]
#
#         # Configure threading based on available CPU cores
#         physical_cores = psutil.cpu_count(logical=False)
#         usable_cores = max(2, physical_cores or 2)
#         pipeline_options.accelerator_options = AcceleratorOptions(
#             num_threads=usable_cores, device=AcceleratorDevice.AUTO
#         )
#
#         return pipeline_options
#
#     def convert_one(self, uri: str) -> ConversionResult:
#         """Convert a single document using RapidOCR."""
#         return self.converter.convert(uri)


class DoclingActor:
    """Ray actor for document processing with docling."""

    def __init__(self):
        self.converter = BasicDocumentConverter()

    def convert_one(
        self,
        *,
        src_path: str,
        base_out_dir: str,
        prefix: str = "",
    ) -> ConvertSummary:
        """Convert a single document to markdown using docling."""
        t0 = time.time()
        try:
            # Convert document using docling
            result: ConversionResult = self.converter.convert_one(src_path)

            if result.status != ConversionStatus.SUCCESS:
                error_msg = f"Conversion failed with status: {result.status}"
                if result.errors:
                    error_msg += f", errors: {result.errors}"
                return ConvertSummary(
                    False, src_path, None, time.time() - t0, None, error=error_msg
                )

            # Get document and metadata
            document = result.document
            pages = document.num_pages()
            doc_name = Path(src_path).stem
            out_dir = (
                Path(base_out_dir) / prefix / doc_name
                if prefix
                else Path(base_out_dir) / doc_name
            )
            out_dir.mkdir(parents=True, exist_ok=True)

            # Export to markdown with referenced images
            md_path = out_dir / f"{doc_name}.md"
            document.save_as_markdown(md_path, image_mode=ImageRefMode.REFERENCED)

            return ConvertSummary(True, src_path, pages, time.time() - t0, str(out_dir))
        except Exception as e:
            return ConvertSummary(
                False, src_path, None, time.time() - t0, None, error=str(e)
            )

    def ready(self) -> bool:
        """Check if actor is ready."""
        return True


def build_converter(
    *,
    base_out_dir: str,
    prefix: str,
):
    """Build a converter callable for Ray Data map_batches."""

    class _DocConverter:
        def __init__(self):
            self.worker = DoclingActor()
            self.base_out_dir = base_out_dir
            self.prefix = prefix

        def __call__(self, batch: Dict[str, List[str]]) -> Dict[str, List[Any]]:
            """Process a batch of documents."""
            rows: List[Dict[str, Any]] = []
            for p in batch["path"]:
                s = self.worker.convert_one(
                    src_path=p,
                    base_out_dir=self.base_out_dir,
                    prefix=self.prefix,
                )
                rows.append(
                    {
                        "src_path": s.src_path,
                        "ok": s.ok,
                        "pages": s.pages,
                        "elapsed_s": s.elapsed_s,
                        "output_dir": s.output_dir,
                        "error": s.error,
                    }
                )
            cols = {k: [r[k] for r in rows] for k in rows[0].keys()}
            return cols

    return _DocConverter


def warmup_model_cache(context: Optional[PipesContext] = None) -> bool:
    """Pre-download docling models to avoid race conditions with Ray workers.

    This function initializes a BasicDocumentConverter once before Ray processing
    starts, ensuring all models are cached. This prevents multiple Ray actors from
    downloading the same models simultaneously, which can cause file corruption.

    Args:
        context: Optional Dagster PipesContext for logging

    Returns:
        True if warmup succeeded or was skipped, False if it failed
    """
    log_func = context.log.info if context else print

    # Check if warmup is enabled (default: True for dev/staging, can be disabled for prod)
    warmup_enabled = os.getenv("DOCLING_WARMUP_MODELS", "true").lower() in (
        "true",
        "1",
        "yes",
    )

    if not warmup_enabled:
        log_func("Model warmup disabled via DOCLING_WARMUP_MODELS=false")
        return True

    try:
        log_func("Warming up model cache (pre-downloading docling models)...")
        # Initialize converter once to trigger model downloads
        _ = BasicDocumentConverter()
        log_func("✓ Model cache warmup complete - all models downloaded")
        return True
    except Exception as e:
        error_msg = f"Model cache warmup failed: {e}"
        log_func(error_msg)

        # Check if we should fail hard or continue (default: fail)
        fail_on_warmup_error = os.getenv(
            "DOCLING_FAIL_ON_WARMUP_ERROR", "true"
        ).lower() in ("true", "1", "yes")

        if fail_on_warmup_error:
            raise RuntimeError(
                f"Model warmup failed. {error_msg}. Set DOCLING_FAIL_ON_WARMUP_ERROR=false to continue anyway."
            ) from e

        log_func(
            "⚠️  Continuing despite warmup failure - Ray workers will download models individually"
        )
        return False


def run_processing(  # noqa: C901
    input_glob: str,
    output_dir: str,
    num_workers: int,
    batch_size: int,
    context: Optional[PipesContext] = None,
) -> Dict[str, Any]:
    """Run document processing pipeline.

    This function can be called in two modes:
    1. With a PipesContext (Dagster Pipes mode)
    2. Without a context (standalone Python mode)

    Args:
        input_glob: Glob pattern for input files (e.g., "data/**/*.pdf")
        output_dir: Directory for output files
        num_workers: Number of parallel workers
        batch_size: Documents per batch
        context: Optional Dagster PipesContext

    Returns:
        Dictionary with processing results
    """
    log_func = context.log.info if context else print

    log_func("Starting docling document processing with Ray...")
    log_func(
        f"Configuration: input_glob={input_glob}, output_dir={output_dir}, "
        f"num_workers={num_workers}, batch_size={batch_size}"
    )

    # Pre-download models before Ray processing to avoid race conditions
    warmup_model_cache(context=context)

    # Ray is already initialized by RayLauncher (or init manually in standalone mode)
    try:
        ray_address = ray.get_runtime_context().gcs_address
        log_func(f"Ray address: {ray_address}")
    except Exception:
        log_func("Ray not initialized or running in local mode")

    # Find input files
    files = [str(p) for p in glob.glob(input_glob, recursive=True)]
    if not files and not os.path.isabs(input_glob):
        project_root = os.getenv("PIXI_PROJECT_ROOT")
        if project_root:
            fallback_glob = os.path.join(project_root, input_glob)
            files = [str(p) for p in glob.glob(fallback_glob, recursive=True)]
    if not files:
        log_func(f"No files found for glob: {input_glob}")
        result = {
            "total_documents": 0,
            "successful": 0,
            "failed": 0,
            "duration_seconds": 0.0,
            "output_directory": output_dir,
            "num_workers": num_workers,
            "batch_size": batch_size,
            "warning": "No input files found",
        }
        if context:
            context.report_asset_materialization(metadata=result)
        log_func("No documents to process. Exiting.")
        return result

    log_func(f"Found {len(files)} documents to process")

    total_start_time = time.time()

    # Create Ray dataset
    ds = rd.from_items([{"path": p} for p in files])
    target_blocks = max(min(len(files), num_workers * 8), num_workers)
    if ds.num_blocks() < target_blocks:
        ds = ds.repartition(target_blocks)

    # Build converter
    mapper_document = build_converter(
        base_out_dir=output_dir,
        prefix="processed",
    )

    # Process documents with Ray Data
    log_func("Starting parallel document processing...")
    result = ds.map_batches(
        mapper_document,
        batch_size=batch_size,
        concurrency=(num_workers, num_workers),
        num_cpus=0,  # Managed by RayLauncher
        num_gpus=0,  # Set to >0 if using GPU-accelerated OCR
    )

    # Materialize results
    status_ds = result.materialize()

    # Compute statistics
    total = status_ds.count()
    ok = status_ds.filter(lambda r: r["ok"]).count()
    failed = total - ok

    total_duration = time.time() - total_start_time

    # Prepare results
    results = {
        "total_documents": total,
        "successful": ok,
        "failed": failed,
        "duration_seconds": round(total_duration, 2),
        "output_directory": output_dir,
        "num_workers": num_workers,
        "batch_size": batch_size,
    }

    # Report results
    log_func(
        f"Processing complete: total={total}, ok={ok}, failed={failed}, "
        f"duration={total_duration:.2f}s"
    )

    if context:
        context.report_asset_materialization(metadata=results)

    log_func("Docling document processing complete!")

    return results


def main():
    """Main entry point for Dagster Pipes mode."""
    context = PipesContext.get()

    # Get configuration from Pipes context extras
    def _get_extra(key: str) -> str | None:
        try:
            return context.get_extra(key)
        except DagsterPipesError:
            return None

    input_glob = _get_extra("INPUT_GLOB") or os.getenv("INPUT_GLOB") or "data/**/*.pdf"
    output_dir_raw = _get_extra("OUTPUT_DIR") or os.getenv("OUTPUT_DIR")

    # Resolve output directory using environment-aware paths
    if output_dir_raw == "$HOME/output/docling":
        # User used default - resolve based on deployment environment
        base_output = get_base_output_path()
        output_dir = f"{base_output}/docling"
    elif output_dir_raw:
        # User provided explicit path - expand variables
        output_dir = os.path.expandvars(output_dir_raw)
    else:
        # Fallback
        output_dir = "out/docling"

    num_workers = int(_get_extra("NUM_WORKERS") or os.getenv("NUM_WORKERS") or "2")
    batch_size = int(_get_extra("BATCH_SIZE") or os.getenv("BATCH_SIZE") or "4")

    # Run processing
    run_processing(
        input_glob=input_glob,
        output_dir=output_dir,
        num_workers=num_workers,
        batch_size=batch_size,
        context=context,
    )


if __name__ == "__main__":
    if os.getenv("DAGSTER_PIPES_CONTEXT") or os.getenv("DAGSTER_PIPES_MESSAGES"):
        # Dagster Pipes mode
        with open_dagster_pipes() as context:
            main()
    else:
        import argparse

        parser = argparse.ArgumentParser(
            description="Process PDF documents using docling and Ray"
        )
        parser.add_argument(
            "--input-glob",
            default="data/**/*.pdf",
            help="Glob pattern for input files (default: data/**/*.pdf)",
        )
        parser.add_argument(
            "--output-dir",
            default="out/docling",
            help="Output directory for processed files (default: out/docling)",
        )
        parser.add_argument(
            "--num-workers",
            type=int,
            default=2,
            help="Number of parallel workers (default: 2)",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=4,
            help="Documents per batch (default: 4)",
        )
        args = parser.parse_args()

        # Standalone mode
        # Initialize Ray if not already running
        if not ray.is_initialized():
            print("Initializing local Ray cluster...")
            ray.init()

        try:
            result = run_processing(
                input_glob=args.input_glob,
                output_dir=args.output_dir,
                num_workers=args.num_workers,
                batch_size=args.batch_size,
                context=None,
            )
            print("\nProcessing Results:")
            print(f"  Total: {result['total_documents']}")
            print(f"  Successful: {result['successful']}")
            print(f"  Failed: {result['failed']}")
            print(f"  Duration: {result['duration_seconds']}s")
            print(f"  Output: {result['output_directory']}")
        finally:
            if ray.is_initialized():
                ray.shutdown()
