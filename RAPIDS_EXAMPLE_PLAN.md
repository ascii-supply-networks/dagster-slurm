# Plan: RAPIDS topic-modeling example (LDA + cuML UMAP/HDBSCAN on Slurm)

Status: draft, 2026-07-15
Branch: `feat/rapids-topic-modeling-example`
Owner: Hernan

## Goal

Build a self-contained, public-data version of our LDA -> UMAP -> HDBSCAN
topic-modeling pipeline as a first-class dagster-slurm example, suitable for
contribution to the RAPIDS deployment examples gallery
(<https://docs.rapids.ai/deployment/stable/examples/>).

Two deliverables:

1. **In this repo:** a new example asset group + payloads that run the full
   chain on the docker Slurm cluster (CPU fallback) and on a real GPU cluster
   (cuML), with unit tests wired into the existing examples CI.
2. **Upstream:** a notebook-style guide (fraud-detection example format:
   narrative notebook + downloadable companion files) for a PR against
   `rapidsai/deployment`.

The pitch to the RAPIDS side: their gallery has 21 examples and every single
one targets cloud or Kubernetes (SageMaker, GKE, Databricks, Snowflake, Brev,
k8s). There is no HPC/Slurm example and no topic-modeling/NLP-clustering
example. This fills both gaps, and the "same asset code runs CPU locally and
cuML on a GPU node" story is something none of their current examples can
demo without provisioning cloud GPUs first.

## Non-goals

- Do **not** touch the frozen cc_index payloads (`lda_train_models.py`,
  `lda_aggregate_umap_hdbscan.py` and siblings carry a FREEZE NOTE). The
  example gets its own payload copies; no imports from the cc_index chain.
- No GPU execution in GitHub CI. The docker Slurm cluster is CPU-only; the
  CPU-fallback path is the CI story.
- No port of the fraud-detection content (Prefect/MLflow/Triton). We mirror
  its *presentation format* only.
- No serving component. Batch outputs (cluster assignments + topic map) are
  the terminal artifact; that is the HPC-idiomatic ending.

## Pipeline design

The deliverable is the orchestration; the dataset just has to make
partitions plentiful and clusters legible. Scale is a config knob on an
unchanged graph: 15 partitions against docker slurm, hundreds on a real
cluster.

```
reuters_corpus            (CPU sbatch, workload-topic-modeling env:
    |                      download + SGML parse + shared gensim dictionary)
lda_models                (partitioned month x seed = 15: one sbatch CPU
    |                      job each, gensim LDA per partition)
topic_term_matrix         (CPU: stack topic-term vectors from all models)
    |
umap_embedding            (GPU sbatch, gpus_per_node=1,
    |                      packaged-cluster-rapids env, cuML UMAP with
    |                      CPU umap-learn fallback)
hdbscan_meta_topics       (GPU, cuML HDBSCAN with CPU hdbscan fallback)
    |
topic_map                 (labeled meta-topic scatter + JSON summary)
```

Aggregation unit (2026-07-17 decision): doc-topic vectors from
independently trained monthly models are not comparable (topic i in
Feb is not topic i in Mar), so the chain clusters **topic-term
vectors** across (month, seed) models into meta-topics, exactly like
the production ``lda_aggregate_umap_hdbscan`` stage over bootstrap
models.

Environment split (implemented): ``workload-topic-modeling`` carries
only gensim on top of the cluster stack; umap-learn/hdbscan/matplotlib
live exclusively in the self-contained ``packaged-cluster-rapids`` env
(python 3.12, numpy<2.3). Putting umap-learn next to the cluster stack
makes uv backtrack through numba into unbuildable sdists; the GPU-stage
payloads run in the rapids env on every deployment, CPU fallback
included, so nothing is lost.

Design points:

- **Per-asset resource + environment targeting is the headline** (not Slurm
  hetjobs: each asset is an independent sbatch, not one heterogeneous job).
  LDA has no cuML equivalent, so the pipeline is honestly mixed: CPU fan-out
  for training, GPU jobs for reduction/clustering, and a *different packed
  environment per asset* via `slurm_pack_cmd` asset metadata (same mechanism
  the docling assets use). This is the thing dagster-slurm shows that the k8s
  examples handle with node-pool juggling.
- **Environments already exist.** `packaged-cluster` has gensim
  (`examples/pyproject.toml:107`); `packaged-cluster-rapids` has
  cuml-cu12/cudf-cu12 25.x plus the CPU-fallback deps (umap-learn, hdbscan)
  deliberately importable alongside cuML. The `pack-rapids` task packs it.
  Expect at most additive dependency tweaks, not a new solve-group.
- **GPU/CPU switching** uses the `_HAS_CUML` factory pattern from
  `bertopic_panel_model.py` (`_make_umap` / `_make_hdbscan`), copied into the
  example payloads, not imported: the payloads must be self-contained because
  they ship to `rapidsai/deployment` as companion files. Carry over the
  hard-won details: `cuml.set_global_output_type("numpy")`, the cuML HDBSCAN
  `min_samples <= 1023` clamp, nn-descent for UMAP neighbor search.
- **Partitioning** mirrors the real per-crawl chain at toy scale: a
  `StaticPartitionsDefinition` over Reuters date slices (the corpus is dated
  1987 newswire), so the backfill UI shows the fan-out that motivated
  dagster-slurm in the first place.

## File layout (new code)

| Piece | Path |
|---|---|
| Asset definitions | `examples/projects/dagster-slurm-example/dagster_slurm_example/defs/rapids_topics/` |
| Payloads | `examples/projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/rapids_topics/{lda_train.py, umap_hdbscan.py, topic_map.py}` |
| Unit tests | `examples/projects/dagster-slurm-example/dagster_slurm_example_tests/test_rapids_topics_*.py` |
| Repo docs page | `docs/docs/applications/topic-modeling-rapids.md` |
| Upstream notebook | separate `rapidsai/deployment` fork, `source/examples/<name>/notebook.ipynb` |

## Tasks

### Phase 0: alignment with the RAPIDS docs team (blocking for Phase 5 only)

- [ ] Confirm submission format (notebook + companion files, as in the
      fraud-detection example) and whether their docs CI executes notebooks.
      If it executes, the docker-compose CPU-fallback path must be the
      notebook's default execution mode from day one.
- [ ] Confirm gallery tags: they will need a new Platforms tag (HPC/Slurm);
      library tags cuML (+ cuDF if we use it in aggregation).
- [ ] Confirm dataset choice and licensing (see Open decisions).

### Phase 1: scaffolding + data

- [x] Create `rapids_topics` defs group and payload subpackage; register in
      `definitions.py` behind the same deployment-awareness as other groups.
- [x] `reuters_corpus` asset: download Reuters-21578, tokenize, write
      per-slice parquet. Cache the raw download so CI and repeated local runs
      don't hammer the source; pin a checksum.
- [x] Decide and implement the partition scheme (date slices, target ~4-8
      partitions so a docker-cluster backfill finishes in minutes).

### Phase 2: CPU chain (LDA fan-out + aggregation)

- [x] `lda_models` partitioned asset + `lda_train.py` payload (gensim,
      `BashLauncher`, `packaged-cluster` env, per-iteration logging on by
      default).
- [ ] `doc_topic_matrix` aggregation asset. Decide cuDF vs pyarrow here
      (cuDF earns a gallery tag but adds a GPU dependency to an otherwise
      CPU asset; pyarrow keeps the CPU/GPU boundary clean). Default: pyarrow,
      revisit if RAPIDS team wants the cuDF tag.
- [ ] Verify end-to-end on local mode (no Slurm) and against the docker
      cluster (`start-staging`).

### Phase 3: GPU chain (UMAP + HDBSCAN + report)

- [x] `umap_hdbscan.py` payload with the `_HAS_CUML` factories (copied from
      `bertopic_panel_model.py`, trimmed to what the example needs).
- [x] `umap_embedding` and `hdbscan_clusters` assets: `gres: gpu:1` when the
      deployment is a supercomputer, no gres in docker mode;
      `slurm_pack_cmd` metadata pointing at the rapids env pack.
- [x] `topic_map` asset: cluster scatter + top-terms-per-cluster table,
      emitted as Dagster asset metadata (plots inline in the UI) so the
      notebook gets its screenshot for free.
- [ ] Validate the CPU-fallback path of both GPU assets in the docker
      cluster (this is what CI will exercise).
- [ ] Validate the cuML path on datalab with 1 GPU (respect the 8-GPU
      association cap; this is a single short job, not a fan-out).

### Phase 4: tests + CI

- [x] Unit tests in `dagster_slurm_example_tests`: factory selection logic
      (cuml absent -> CPU classes), min_samples clamp, partition/config
      validation, payload arg plumbing. These run in `tutorial.yaml` via the
      examples `test` task automatically; no workflow changes needed.
- [ ] Decide with maintainers whether to add a `needs_slurm_docker`
      integration test materializing the chain CPU-fallback in
      `library.yaml`. Cost concern: the integration step currently packs only
      `packaged-cluster`; adding a rapids-env pack to CI may be too slow on a
      GitHub runner. Fallback: keep the docker-cluster run as a documented
      manual verification step in the example README.
- [x] Lint/format pass (`pixi run -e build --frozen fmt && lint`).

### Phase 5: documentation + upstream PR

- [x] Repo docs page under `docs/docs/applications/`, same register as the
      docling page: what it shows, how to run it in docker mode, how to point
      it at a real cluster. Includes the Jul 18 supercomputer-run screenshots
      (`docs/static/img/rapids-topics/`), the cuML-on-HPC practical notes, and
      a metaxy-refined incremental variant as a second example section.
- [ ] Notebook for `rapidsai/deployment`: narrative walkthrough
      (docker compose up -> pixi start -> materialize -> screenshots),
      companion files = the asset + payload modules.
- [ ] "Practical notes on cuML on HPC" section: min_samples cap, the 25.12
      HDBSCAN performance regression that made us pair cuML UMAP with sklearn
      HDBSCAN in one chain, why RAPIDS needs its own solve-group/packed env
      (numba -> numpy pin conflicts), pixi-pack for clusters without
      container runtimes.
- [ ] Open the upstream PR; iterate with their review.

## Open decisions

1. **Dataset.** Reuters-21578 is the working default: small (~21k docs, ~10k
   in the ModApte split), dated (preserves the temporal fan-out framing),
   long-established research corpus. Caveats to resolve in Phase 0: its
   distribution terms are informal ("free for research"), and it is too
   small to show a real GPU speedup. Scale-up options for an appendix:
   RCV1 raw text requires a NIST agreement (sklearn's `fetch_rcv1` ships
   TF-IDF vectors only, useless for LDA), so the realistic large option is a
   Wikipedia sample (CC BY-SA, and we already have wiki payload precedent).
   Recommendation: Reuters by default, Wikipedia scale-up section, and be
   explicit that the example demonstrates orchestration mechanics rather
   than benchmark speedups.
2. **cuDF in the aggregation asset**: see Phase 2; decide with the RAPIDS
   team since it is mostly about their tag taxonomy.
3. **LDA library**: gensim (already in the cluster env, matches the real
   pipeline) vs scikit-learn (fewer deps). Default gensim.
4. **Integration-test depth in CI**: see Phase 4.

## Risks

- **Rapids env pack time/size in CI** if we choose to integration-test the
  GPU assets' fallback path; mitigated by keeping that test optional.
- **cuML 25.x quirks** (HDBSCAN perf regressions, parameter caps) are known
  from the BERTopic chain; the example inherits the documented workarounds
  rather than discovering them fresh.
- **Upstream format drift**: rapidsai/deployment controls the final shape;
  Phase 0 exists so we don't build the notebook twice.
- **Dataset licensing pushback** on Reuters-21578; Wikipedia sample is the
  fallback for the whole example, not just the appendix, if NVIDIA legal
  objects.

## Acceptance criteria

- `pixi run start` (local mode): CPU stages (corpus, LDA partitions,
  aggregation) materialize on a laptop with no Slurm and no GPU. The
  GPU-stage payloads require a Slurm deployment with the rapids env:
  the dev env deliberately excludes umap-learn/hdbscan (numba/numpy
  conflict with the cluster stack), so full-chain local mode is out.
- Docker cluster (`start-staging` after `deploy-prod-docker` + rapids pack):
  full chain materializes; LDA partitions dispatch as separate sbatch jobs;
  UMAP/HDBSCAN assets run in the rapids env on the CPU fallback.
- Real cluster with 1 GPU: UMAP/HDBSCAN assets log the cuML (GPU) backend
  and produce the same artifact shapes as the fallback.
- Unit tests green in `tutorial.yaml`; lint clean.
- Notebook draft renders in the rapidsai/deployment build with correct tags.
