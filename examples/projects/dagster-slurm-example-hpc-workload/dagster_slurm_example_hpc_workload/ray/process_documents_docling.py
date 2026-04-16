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


def _coerce_rapidocr_primitives(value: Any) -> Any:
    """Convert pathlib objects into primitive values for RapidOCR/OmegaConf."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: _coerce_rapidocr_primitives(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_coerce_rapidocr_primitives(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_coerce_rapidocr_primitives(item) for item in value)
    return value


def _update_rapidocr_config(parse_params, cfg, params: Dict[str, Any]):
    """Apply RapidOCR config overrides without OmegaConf's merge warning."""
    from enum import Enum

    from omegaconf import OmegaConf

    global_config = OmegaConf.to_container(cfg.Global)
    if not isinstance(global_config, dict):
        raise TypeError("RapidOCR Global config must resolve to a mapping.")

    global_keys = list(global_config)
    enum_params = {
        "engine_type",
        "model_type",
        "ocr_version",
        "lang_type",
        "task_type",
    }

    for key, value in params.items():
        key_parts = key.split(".")
        if key.startswith("Global") and key_parts[1] not in global_keys:
            raise ValueError(f"{key} is not a valid key.")

        if key_parts[1] in enum_params and not isinstance(value, Enum):
            raise TypeError(f"The value of {key} must be Enum Type.")

        parse_params.update(cfg, key, value, merge=False)

    return cfg


def _patched_rapidocr_load_config(
    default_cfg_path: Path,
    root_dir: Path,
    parse_params,
):
    def _load_config(
        self, config_path: Optional[str], params: Optional[Dict[str, Any]]
    ):
        if config_path is not None and Path(config_path).exists():
            cfg = parse_params.load(config_path)
        else:
            cfg = parse_params.load(default_cfg_path)

        if params:
            cfg = _update_rapidocr_config(
                parse_params,
                cfg,
                _coerce_rapidocr_primitives(params),
            )

        model_root_dir = cfg.Global.model_root_dir
        if model_root_dir is None:
            cfg.Global.model_root_dir = str(root_dir / "models")
        else:
            cfg.Global.model_root_dir = _coerce_rapidocr_primitives(model_root_dir)

        return cfg

    return _load_config


def _patch_pytorch_model_loader(
    model_loader_cls,
    file_info_cls,
    infer_session_cls,
    download_file_cls,
    download_file_input_cls,
    logger_obj,
) -> None:
    """Make RapidOCR's PyTorch backend accept string model_root_dir values."""
    original_init_model_path = model_loader_cls._init_model_path

    def _init_model_path(self, cfg):
        model_path = cfg.get("model_path", None)
        if model_path is None:
            model_info = infer_session_cls.get_model_url(
                file_info_cls(
                    engine_type=cfg.engine_type,
                    ocr_version=cfg.ocr_version,
                    task_type=cfg.task_type,
                    lang_type=cfg.lang_type,
                    model_type=cfg.model_type,
                )
            )
            default_model_url = model_info["model_dir"]
            model_root_dir = Path(str(cfg.model_root_dir))
            model_path = model_root_dir / Path(default_model_url).name
            download_file_cls.run(
                download_file_input_cls(
                    file_url=default_model_url,
                    sha256=model_info["SHA256"],
                    save_path=model_path,
                    logger=logger_obj,
                )
            )
            logger_obj.info(f"Using {model_path}")
            infer_session_cls._verify_model(model_path)
            return Path(model_path)

        return original_init_model_path(self, cfg)

    model_loader_cls._init_model_path = _init_model_path  # type: ignore[method-assign]


def _install_rapidocr_path_compat() -> None:
    """Patch RapidOCR to avoid passing pathlib.Path objects into OmegaConf."""
    try:
        from rapidocr.inference_engine.pytorch.networks.main import (
            DownloadFile,
            DownloadFileInput,
            FileInfo,
            InferSession,
            ModelLoader,
            logger,
        )
        from rapidocr.main import DEFAULT_CFG_PATH, ParseParams, RapidOCR, root_dir
    except ImportError:
        return

    marker_name = "_dagster_slurm_path_compat"
    if bool(getattr(RapidOCR, marker_name, False)):
        return

    _patch_pytorch_model_loader(
        ModelLoader,
        FileInfo,
        InferSession,
        DownloadFile,
        DownloadFileInput,
        logger,
    )
    RapidOCR._load_config = _patched_rapidocr_load_config(  # type: ignore[method-assign]
        DEFAULT_CFG_PATH,
        root_dir,
        ParseParams,
    )
    setattr(RapidOCR, marker_name, True)


_install_rapidocr_path_compat()


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
        # Ray serializes the mapper class into worker processes, so module-level
        # side effects from the driver are not sufficient to patch RapidOCR there.
        _install_rapidocr_path_compat()
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


def _effective_worker_count(num_workers: int, file_count: int) -> int:
    """Cap Ray workers to the number of files to avoid useless model duplication."""
    if file_count <= 0:
        return max(1, num_workers)
    return max(1, min(num_workers, file_count))


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
    effective_workers = _effective_worker_count(num_workers, len(files))
    if effective_workers != num_workers:
        log_func(
            f"Reducing worker count from {num_workers} to {effective_workers} "
            f"for {len(files)} input file(s)"
        )

    total_start_time = time.time()

    # Create Ray dataset
    ds = rd.from_items([{"path": p} for p in files])
    target_blocks = max(min(len(files), effective_workers * 8), effective_workers)
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
        concurrency=(effective_workers, effective_workers),
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
        "num_workers": effective_workers,
        "requested_num_workers": num_workers,
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
