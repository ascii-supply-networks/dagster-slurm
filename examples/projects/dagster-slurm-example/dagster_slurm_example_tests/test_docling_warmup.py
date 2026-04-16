import warnings
from pathlib import Path
from types import SimpleNamespace

import dagster_slurm_example_hpc_workload.ray.process_documents_docling as process_documents_docling
from dagster_slurm_example_hpc_workload.ray.process_documents_docling import (
    BasicDocumentConverter,
    _coerce_rapidocr_primitives,
    _install_rapidocr_path_compat,
)
from docling.datamodel.base_models import InputFormat


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
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("error")
        cfg = rapidocr._load_config(  # type: ignore[attr-defined]
            None,
            {
                "Global.model_root_dir": Path("/tmp/models"),
                "Det.model_path": Path("/tmp/models/det.onnx"),
            },
        )

    assert cfg.Global.model_root_dir == "/tmp/models"
    assert cfg.Det.model_path == "/tmp/models/det.onnx"
    assert caught_warnings == []


def test_pytorch_model_loader_accepts_string_model_root_dir(monkeypatch, tmp_path):
    _install_rapidocr_path_compat()

    from rapidocr.inference_engine.pytorch.networks.main import ModelLoader

    model_root_dir = tmp_path / "models"
    model_root_dir.mkdir()
    downloaded_model = model_root_dir / "det-model.bin"

    monkeypatch.setattr(
        "rapidocr.inference_engine.pytorch.networks.main.InferSession.get_model_url",
        lambda _file_info: {
            "model_dir": "https://example.invalid/det-model.bin",
            "SHA256": "dummy",
        },
    )
    monkeypatch.setattr(
        "rapidocr.inference_engine.pytorch.networks.main.DownloadFile.run",
        lambda download_input: download_input.save_path.write_bytes(b"model"),
    )
    monkeypatch.setattr(
        "rapidocr.inference_engine.pytorch.networks.main.InferSession._verify_model",
        lambda model_path: None,
    )

    cfg = SimpleNamespace(
        engine_type="pytorch",
        ocr_version="v5",
        task_type="det",
        lang_type="en",
        model_type="server",
        model_root_dir=str(model_root_dir),
        get=lambda key, default=None: getattr(cfg, key, default),  # type: ignore[name-defined]
    )

    loader = ModelLoader.__new__(ModelLoader)
    model_path = loader._init_model_path(cfg)

    assert model_path == downloaded_model


def test_basic_document_converter_installs_path_compat_before_init(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        process_documents_docling,
        "_install_rapidocr_path_compat",
        lambda: calls.append("compat"),
    )

    class DummyDocumentConverter:
        def initialize_pipeline(self, document_format: InputFormat) -> None:
            calls.append(f"initialize:{document_format.value}")

    monkeypatch.setattr(
        process_documents_docling,
        "DocumentConverter",
        DummyDocumentConverter,
    )

    converter = BasicDocumentConverter()

    assert isinstance(converter.converter, DummyDocumentConverter)
    assert calls == ["compat", "initialize:pdf"]
