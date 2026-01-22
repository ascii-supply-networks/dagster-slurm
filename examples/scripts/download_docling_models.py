"""Download docling models to local cache for offline/restricted HPC environments.

Usage:
    # Set custom cache directory (optional)
    export MODEL_CACHE_DIR=/path/to/shared/filesystem/models
    export HF_HOME=/path/to/shared/filesystem/huggingface

    # Run the download script
    python download_docling_models.py

    # On compute nodes, ensure HF_HOME points to the downloaded models:
    export HF_HOME=/path/to/shared/filesystem/huggingface
"""

import os
from pathlib import Path

from docling.utils.model_downloader import download_models


# Configure cache directories
DEFAULT_CACHE_DIR = Path.home() / ".cache"
CACHE_DIR = Path(os.getenv("MODEL_CACHE_DIR", DEFAULT_CACHE_DIR))
HF_HOME = Path(os.getenv("HF_HOME", CACHE_DIR / "huggingface"))


def main():
    """Download all docling models to local cache."""
    print(f"Downloading docling models to: {HF_HOME}")
    HF_HOME.mkdir(parents=True, exist_ok=True)

    # Download docling models (excludes EasyOCR by default)
    print("--- Starting docling model download ---")
    download_models(progress=True, with_easyocr=False)
    print("✅ Docling model download completed.")
    print(f"Models are cached at: {HF_HOME}")
    print(f"\nOn compute nodes, set: export HF_HOME={HF_HOME}")


if __name__ == "__main__":
    main()
