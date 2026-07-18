"""Unit tests for the rapids_topics example (CPU-only, no Slurm/GPU)."""

import pytest
from dagster_slurm_example_hpc_workload.rapids_topics import (
    hdbscan_cluster,
    umap_reduce,
)
from dagster_slurm_example_hpc_workload.rapids_topics.prepare_corpus import (
    parse_sgml_docs,
)
from dagster_slurm_example_hpc_workload.rapids_topics.topic_map import cluster_labels
from pydantic import ValidationError

SGML_FIXTURE = """
<REUTERS TOPICS="YES" NEWID="1">
<DATE>26-FEB-1987 15:01:01.79</DATE>
<TITLE>BAHIA COCOA REVIEW</TITLE>
<BODY>Showers continued throughout the week in Bahia cocoa zone.
Reuter
&#3;</BODY>
</REUTERS>
<REUTERS TOPICS="NO" NEWID="2">
<DATE>3-MAR-1987 09:12:00.00</DATE>
<TITLE>NO BODY HERE</TITLE>
</REUTERS>
<REUTERS TOPICS="NO" NEWID="3">
<DATE>12-OCT-1987 11:00:00.00</DATE>
<BODY>Grain shipments &amp; exports rose sharply.</BODY>
</REUTERS>
"""


def test_parse_sgml_extracts_dated_docs_with_bodies():
    docs = parse_sgml_docs(SGML_FIXTURE)

    assert [d["month"] for d in docs] == ["1987-02", "1987-10"]
    assert docs[0]["title"] == "BAHIA COCOA REVIEW"
    assert "cocoa zone" in docs[0]["body"]
    # entity unescaping and untitled docs
    assert docs[1]["title"] == ""
    assert "&" in docs[1]["body"] and "&amp;" not in docs[1]["body"]


def test_sha256_verification_accepts_match_and_rejects_mismatch(tmp_path):
    import hashlib

    from dagster_slurm_example_hpc_workload.rapids_topics.prepare_corpus import (
        verify_sha256,
    )

    blob = tmp_path / "reuters.tar.gz"
    blob.write_bytes(b"not really a tarball")
    good = hashlib.sha256(b"not really a tarball").hexdigest()

    verify_sha256(blob, good)
    with pytest.raises(RuntimeError, match="Checksum mismatch"):
        verify_sha256(blob, "0" * 64)


def test_min_samples_clamped_only_above_cuml_cap():
    assert hdbscan_cluster.clamped_min_samples(5) == 5
    assert hdbscan_cluster.clamped_min_samples(1023) == 1023
    assert hdbscan_cluster.clamped_min_samples(5000) == 1023


def test_payload_modules_import_without_cuml():
    # The dev/CI env has no cuML; import must fall through cleanly so
    # the CPU fallback path is selected at runtime.
    assert umap_reduce._HAS_CUML is False
    assert hdbscan_cluster._HAS_CUML is False


def test_factories_fall_back_to_cpu_without_cuml():
    # umap-learn / hdbscan live in the packaged-cluster-rapids env, not
    # in dev (numba/numpy conflicts with the cluster stack); exercise
    # the factory bodies only where they are importable.
    hdbscan_lib = pytest.importorskip("hdbscan")
    umap_lib = pytest.importorskip("umap")

    reducer = umap_reduce.make_umap(
        n_components=2,
        n_neighbors=15,
        min_dist=0.1,
        metric="cosine",
        random_state=0,
        build_algo="auto",
    )
    assert isinstance(reducer, umap_lib.UMAP)

    clusterer = hdbscan_cluster.make_hdbscan(min_cluster_size=5, min_samples=5)
    assert isinstance(clusterer, hdbscan_lib.HDBSCAN)


def test_inline_plot_embeds_small_pngs_and_skips_large_ones():
    import base64

    from dagster_slurm_example_hpc_workload.rapids_topics.topic_map import (
        MAX_INLINE_PLOT_BYTES,
        inline_plot_metadata,
    )

    png_bytes = b"\x89PNG-fake-bytes"
    embedded = inline_plot_metadata(png_bytes)
    assert embedded is not None and embedded["type"] == "md"
    assert base64.b64encode(png_bytes).decode("ascii") in embedded["raw_value"]
    assert embedded["raw_value"].startswith("![topic map](data:image/png;base64,")

    assert inline_plot_metadata(b"x" * (MAX_INLINE_PLOT_BYTES + 1)) is None


def test_cluster_labels_use_most_frequent_terms_and_skip_noise():
    labels = cluster_labels(
        clusters=[0, 0, 0, -1, 1],
        top_terms=[
            ["oil", "crude", "barrel"],
            ["oil", "crude", "opec"],
            ["oil", "gas", "crude"],
            ["noise", "junk", "misc"],
            ["wheat", "grain", "corn"],
        ],
        n_label_terms=2,
    )

    assert labels[0] == "oil, crude"
    assert labels[1] == "wheat, grain"
    assert -1 not in labels


def test_lda_config_rejects_invalid_topic_counts():
    from dagster_slurm_example.defs.rapids_topics.topic_assets import LdaConfig

    with pytest.raises(ValidationError):
        LdaConfig(num_topics=0)
    with pytest.raises(ValidationError):
        LdaConfig(num_topics=501)
    assert LdaConfig().num_topics == 15


def test_launchpad_slurm_overrides_replace_only_set_fields():
    from dagster_slurm_example.defs.rapids_topics.topic_assets import (
        HdbscanConfig,
        _merged_slurm_opts,
    )

    defaults = {"nodes": 1, "cpus_per_task": 2, "mem": "4G", "gpus_per_node": 1}
    untouched = _merged_slurm_opts(defaults, HdbscanConfig())
    assert untouched == defaults

    merged = _merged_slurm_opts(defaults, HdbscanConfig(mem="64G", gpus_per_node=0))
    assert merged == {"nodes": 1, "cpus_per_task": 2, "mem": "64G", "gpus_per_node": 0}


def test_pre_deployed_env_override_skipped_in_local_mode():
    from types import SimpleNamespace
    from typing import cast

    from dagster_slurm import ComputeResource
    from dagster_slurm.config.environment import ExecutionMode
    from dagster_slurm_example.defs.rapids_topics.topic_assets import (
        HdbscanConfig,
        _run_overrides,
    )

    cfg = HdbscanConfig(pre_deployed_env_path="/home/user/env")
    slurm = cast(ComputeResource, SimpleNamespace(mode=ExecutionMode.SLURM))
    local = cast(ComputeResource, SimpleNamespace(mode=ExecutionMode.LOCAL))

    assert _run_overrides(cfg, slurm) == {
        "pre_deployed_env_path_override": "/home/user/env"
    }
    assert _run_overrides(cfg, local) == {}
    assert _run_overrides(HdbscanConfig(), slurm) == {}


def test_lda_partitions_cover_month_seed_grid():
    from dagster_slurm_example.defs.rapids_topics.topic_assets import (
        MONTHS,
        SEEDS,
        lda_partitions,
    )

    keys = lda_partitions.get_partition_keys()
    assert len(keys) == len(MONTHS) * len(SEEDS) == 15
