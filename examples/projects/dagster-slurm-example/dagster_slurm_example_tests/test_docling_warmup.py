from pathlib import Path

from dagster_slurm_example_hpc_workload.ray.process_documents_docling import (
    _coerce_rapidocr_primitives,
    _install_rapidocr_path_compat,
)


def test_coerce_rapidocr_primitives_converts_paths():
    payload = {
        "Global.model_root_dir": Path("/tmp/models"),
        "Det.model_path": Path("/tmp/models/det.onnx"),
        "nested": [Path("/tmp/font.ttf"), {"key": Path("/tmp/rec.onnx")}],
    }

    coerced = _coerce_rapidocr_primitives(payload)

    assert coerced == {
        "Global.model_root_dir": "/tmp/models",
        "Det.model_path": "/tmp/models/det.onnx",
        "nested": ["/tmp/font.ttf", {"key": "/tmp/rec.onnx"}],
    }


def test_rapidocr_load_config_accepts_path_params():
    _install_rapidocr_path_compat()

    from rapidocr.main import RapidOCR

    rapidocr = RapidOCR.__new__(RapidOCR)
    cfg = rapidocr._load_config(  # type: ignore[attr-defined]
        None,
        {
            "Global.model_root_dir": Path("/tmp/models"),
            "Det.model_path": Path("/tmp/models/det.onnx"),
        },
    )

    assert cfg.Global.model_root_dir == "/tmp/models"
    assert cfg.Det.model_path == "/tmp/models/det.onnx"
