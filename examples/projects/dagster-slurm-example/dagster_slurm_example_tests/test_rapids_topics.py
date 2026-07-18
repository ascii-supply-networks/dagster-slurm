"""Unit tests for the rapids_topics example (CPU-only, no Slurm/GPU)."""

import base64
import hashlib
from types import SimpleNamespace
from typing import cast

import pytest
from dagster_slurm import ComputeResource
from dagster_slurm.config.environment import ExecutionMode
from dagster_slurm_example.defs.rapids_topics.topic_assets import (
    HdbscanConfig,
    _merged_slurm_opts,
    _run_overrides,
)
from dagster_slurm_example_hpc_workload.rapids_topics.prepare_corpus import (
    parse_sgml_docs,
    publish_env_for_reuse,
    verify_sha256,
)
from dagster_slurm_example_hpc_workload.rapids_topics.topic_map import (
    MAX_INLINE_PLOT_BYTES,
    cluster_labels,
    inline_plot_metadata,
)

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


def test_parser_keeps_only_dated_docs_with_bodies():
    docs = parse_sgml_docs(SGML_FIXTURE)

    # The bodyless story must be dropped; month abbreviations must map
    # to the zero-padded partition keys the assets use.
    assert [d["month"] for d in docs] == ["1987-02", "1987-10"]
    # SGML entities are unescaped and the end-of-text control is gone.
    assert "&" in docs[1]["body"]
    assert "&amp;" not in docs[1]["body"]
    assert "&#3;" not in docs[0]["body"]
    # Untitled stories survive with an empty title, not a crash.
    assert docs[1]["title"] == ""


def test_sha256_verification_accepts_match_and_rejects_mismatch(tmp_path):
    blob = tmp_path / "reuters.tar.gz"
    blob.write_bytes(b"not really a tarball")

    verify_sha256(blob, hashlib.sha256(b"not really a tarball").hexdigest())
    with pytest.raises(RuntimeError, match="Checksum mismatch"):
        verify_sha256(blob, "0" * 64)


def test_inline_plot_round_trips_and_respects_size_cap():
    png_bytes = b"\x89PNG-fake-bytes"
    embedded = inline_plot_metadata(png_bytes)

    assert embedded is not None and embedded["type"] == "md"
    # The markdown must carry the exact bytes: decode what the UI would.
    payload = embedded["raw_value"].split("base64,", 1)[1].rstrip(")")
    assert base64.b64decode(payload) == png_bytes

    # Oversized figures must fall back to path-only reporting instead
    # of pushing megabytes through the event log.
    assert inline_plot_metadata(b"x" * (MAX_INLINE_PLOT_BYTES + 1)) is None


def test_cluster_labels_rank_terms_by_frequency_and_skip_noise():
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


def test_launchpad_overrides_replace_only_set_fields():
    defaults = {"nodes": 1, "cpus_per_task": 2, "mem": "4G", "gpus_per_node": 1}

    assert _merged_slurm_opts(defaults, HdbscanConfig()) == defaults
    merged = _merged_slurm_opts(defaults, HdbscanConfig(mem="64G", gpus_per_node=0))
    assert merged == {"nodes": 1, "cpus_per_task": 2, "mem": "64G", "gpus_per_node": 0}


def test_env_publish_creates_and_retargets_symlink(tmp_path):
    # Packed-env layout: <base>/activate.sh + <base>/env (sys.prefix).
    def packed_env(name):
        base = tmp_path / name
        (base / "env").mkdir(parents=True)
        (base / "activate.sh").touch()
        return base / "env"

    link = tmp_path / "stable_link"
    assert publish_env_for_reuse(packed_env("cache-key-1"), link)
    assert link.resolve() == (tmp_path / "cache-key-1").resolve()

    # A newer pack must atomically retarget the existing link.
    assert publish_env_for_reuse(packed_env("cache-key-2"), link)
    assert link.resolve() == (tmp_path / "cache-key-2").resolve()

    # Outside a packed env (local dev interpreter): no-op, no link.
    plain = tmp_path / "venv" / "env"
    plain.mkdir(parents=True)
    other = tmp_path / "other_link"
    assert not publish_env_for_reuse(plain, other)
    assert not other.exists()


def test_no_asset_hardcodes_an_env_path_without_pins():
    # With no RAPIDS_TOPICS_*_ENV pins set, metadata carries only the
    # pack command; env reuse flows through the head's published path.
    from dagster_slurm_example.defs.rapids_topics import topic_assets as ta

    for asset in (
        ta.reuters_corpus,
        ta.lda_models,
        ta.topic_term_matrix,
        ta.umap_embedding,
        ta.hdbscan_meta_topics,
        ta.topic_map,
    ):
        metadata = asset.metadata_by_key[next(iter(asset.keys))]
        assert "slurm_pack_cmd" in metadata
        assert "slurm_pre_deployed_env_path" not in metadata


def test_run_override_precedence_launchpad_then_published():
    # LocalPipesClient.run() has no **kwargs: forwarding the override in
    # local mode raises TypeError, so it must be dropped there.
    cfg = HdbscanConfig(pre_deployed_env_path="/home/user/env")
    slurm = cast(ComputeResource, SimpleNamespace(mode=ExecutionMode.SLURM))
    local = cast(ComputeResource, SimpleNamespace(mode=ExecutionMode.LOCAL))

    # Launchpad value wins over the published path.
    assert _run_overrides(cfg, slurm, "/published/env") == {
        "pre_deployed_env_path_override": "/home/user/env"
    }
    # Published path applies when the launchpad is silent.
    assert _run_overrides(HdbscanConfig(), slurm, "/published/env") == {
        "pre_deployed_env_path_override": "/published/env"
    }
    # No path known: normal pack flow.
    assert _run_overrides(HdbscanConfig(), slurm) == {}
    # Local mode drops everything.
    assert _run_overrides(cfg, local, "/published/env") == {}
