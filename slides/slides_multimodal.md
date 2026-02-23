---
# You can also start simply with 'default'
theme: default
# random image from a curated Unsplash collection by Anthony
# like them? see https://unsplash.com/collections/94734566/slidev
background: https://cover.sli.dev
# some information about your slides (markdown enabled)
title: Dagster + Slurm + Metaxy = Efficient Multimodal Processing
info: |
  ## From HPC orchestration to field-level incremental multimodal processing
  Keep Dagster assets portable across local and Slurm while Metaxy limits expensive recomputation.

  Learn more at [dagster-slurm](https://github.com/ascii-supply-networks/dagster-slurm/) and [Metaxy](https://docs.metaxy.io)
# apply unocss classes to the current slide
class: text-center
# https://sli.dev/features/drawing
drawings:
  persist: false
# slide transition: https://sli.dev/guide/animations.html#slide-transitions
transition: slide-left
# enable MDC Syntax: https://sli.dev/features/mdc
mdc: true
# open graph
seoMeta:
  # By default, Slidev will use ./og-image.png if it exists,
  # or generate one from the first slide if not found.
  ogImage: auto
  # ogImage: https://cover.sli.dev

# router mode for vue-router, can be "history" or "hash"
routerMode: hash
---

<div class="grid grid-cols-3 gap-8 max-w-4xl mx-auto mt-10 items-start">
  <div class="flex flex-col items-center">
    <div class="h-44 w-44 flex items-end justify-center">
      <img src="/img/dagster.svg" alt="Dagster" class="h-36 w-36 object-contain" />
    </div>
    <h2 class="mt-3 !text-3xl !font-700">Dagster</h2>
  </div>
  <div class="flex flex-col items-center">
    <div class="h-44 w-44 flex items-end justify-center">
      <img src="/img/featured.png" alt="dagster-slurm" class="h-44 w-44 object-contain" />
    </div>
    <h2 class="mt-3 !text-3xl !font-700">Slurm</h2>
  </div>
  <div class="flex flex-col items-center">
    <div class="h-44 w-44 flex items-end justify-center">
      <img src="/img/metaxy.svg" alt="Metaxy" class="h-36 w-36 object-contain" />
    </div>
    <h2 class="mt-3 !text-3xl !font-700">Metaxy</h2>
  </div>
</div>

## = Efficient Multimodal Processing

<div @click="$slidev.nav.next" class="mt-12 py-1" hover:bg="white op-10">
bridge orchestration, HPC scheduling, and field-level incremental metadata <carbon:arrow-right />
</div>

<div class="abs-br m-6 text-xl">
  <button @click="$slidev.nav.openInEditor()" title="Open in Editor" class="slidev-icon-btn">
    <carbon:edit />
  </button>
  <a href="https://github.com/ascii-supply-networks/dagster-slurm/" target="_blank" class="slidev-icon-btn">
    <carbon:logo-github />
  </a>
</div>

<!--
The last comment block of each slide will be treated as slide notes. It will be visible and editable in Presenter Mode along with the slide. [Read more in the docs](https://sli.dev/guide/syntax.html#notes)
-->

---
layout: statement
---

# EU sovereign GPU cloud does not come out of nowhere

maybe this project can support making HPC systems more accessible

<div class="abs-b mb-6 w-full flex justify-center">
  <img src="/img/featured.png" alt="dagster-slurm" class="h-45 object-contain" />
</div>

---
transition: slide-up
level: 2
class: bg-white text-black
---

![](/img/ascii_overview.svg)

---
transition: slide-left
layout: two-cols-header
class: px-6
---

# <span class="text-orange-500 !text-5xl">Challenges</span>

::left::

### HPC pain points

<v-clicks>

- Inconsistent Slurm scripts and limited obsevability create high operational overhead
- Python/ML environments break inconsistent between environments laptop and cluster/s.
- Moving one script from prototype to HPC often means rewriting runtime glue.

</v-clicks>

<v-clicks>
<p class="mt-6 text-orange-500"><strong>Bridge: dagster-slurm keeps Dagster UX while honoring HPC power.</strong></p>
</v-clicks>

::right::

### Why Metaxy

<v-clicks>

- Coarse invalidation causes expensive GPU/LLM recomputes.
- Multimodal fields change at different rates and need field-level tracking.
- Metaxy resolves `new` and `stale` rows only, so unchanged data is skipped.

</v-clicks>

<v-clicks>
<p class="mt-6 text-orange-500"><strong>Result: incremental processing fosters rapid experimentation.</strong></p>
</v-clicks>

---
transition: fade-out
class: bg-white text-black
disabled: true
---

<div class="-mt-22">
<img src="/img/pass-offering.svg" />
</div>
<!--
From the (public) cloud we expect so much more.
-->

---
#title: More than a single engine
layout: image-right
image: /img/engine-only.jpeg
backgroundSize: contain
transition: fade-out
---

# Why orchestration matters

<div class="text-[20px] leading-relaxed space-y-4">

- A raw engine is not enough once pipelines depend on ETL, sensors, or model training.
- Orchestrators provide dependency tracking, retries, metrics, and the control plane HPC teams lack.
- `dagster-slurm` maps that control plane directly to Slurm scheduling primitives.

</div>

---
transition: fade-out
# layout: two-cols
layout: image-right
image: /img/lineage-dark2.png
---

# Dagster asset graph

<img src="https://dagster-website.vercel.app/images/brand/logos/dagster-primary-mark.png"
     class="fixed top-4 right-4 w-12 h-12 object-contain z-50 pointer-events-none drop-shadow" />

- Like a calculator for crunching numbers
- Graph allows computer to reason about data dependencies
- Rapid iteration: Just edit code. No need to wait for XYZ SaaS service
- Break down tool and department silos
- Assets know when upstream data changed and re-materialise only when stale.

```python {3-5|7|all}
import dagster as dg

@dg.asset
def hello(context: dg.AssetExecutionContext):
    context.log.info("Hello!")

@dg.asset(deps=[hello])
def world(context: dg.AssetExecutionContext):
    context.log.info("World!")
```

---
layout: image
image: /img/asset-vs-task.png
backgroundSize: contain
transition: slide-up
---

---
transition: slide-left
layout: center
---

# Your code stays the same

- Same script runs locally or on Slurm; only configuration changes.
- Compute resource wraps payload execution with observability.

````md magic-move {lines: true}
```python
import dagster as dg
from dagster_slurm import BashLauncher, ComputeResource

@dg.asset
def train_pytorch(context: dg.AssetExecutionContext, compute: ComputeResource):
    script_path = dg.file_relative_path(
        __file__, "../workloads/pytorch/train_classifier.py"
    )

    completed = compute.run(
        context=context,
        payload_path=script_path,
        launcher=BashLauncher(),
        extra_env={"EPOCHS": "3", "BATCH_SIZE": "128"},
    )
    yield from completed.get_results()
```

```python
def main():
    context = PipesContext.get()
    num_epochs = context.extras["epochs"]
    context.log.debug(f"Number of epochs: {num_epochs}")
    input_path = os.environ.get("INPUT_DATA")
    context.log.info(f"Input: {input_path}")
    # Your processing logic here (pytorch, ...)
    result = {"rows_processed": 1000}
    context.report_asset_materialization(
        metadata={
            "rows": result["rows_processed"],
            "processing_time": "10s",
        }
    )

if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
```
````

---
layout: image
image: /img/process_data_run_view.png
backgroundSize: contain
---

---
layout: image
image: /img/process_data_asset_view.png
backgroundSize: contain
---

## transition: fadeclass: bg-slate-950 text-white

# <span class="!text-5xl">Architecture</span>

<div class="mt-8 w-full flex items-center justify-between gap-2 text-sm">
  <span class="rounded-lg border border-slate-500 px-3 py-2">Dagster asset</span>
  <span>-></span>
  <span class="rounded-lg border border-emerald-400 px-3 py-2">dagster-slurm</span>
  <span>-></span>
  <span class="rounded-lg border border-slate-500 px-3 py-2">Slurm job</span>
  <span>-></span>
  <span class="rounded-lg border border-emerald-400 px-3 py-2">Metaxy `resolve_update`</span>
  <span>-></span>
  <span class="rounded-lg border border-slate-500 px-3 py-2">Ray</span>
  <span>-></span>
  <span class="rounded-lg border border-slate-500 px-3 py-2">Docling</span>
</div>

<div class="mt-10 text-[18px] leading-relaxed space-y-3 pr-[68%]">

- Dagster decides **what** to run and captures lineage/materialization.
- dagster-slurm decides **where** to run by submitting the workload as a Slurm job.
- Metaxy decides **what actually changed**, so Ray/Docling process only `new` + `stale`.

</div>

<div class="abs-br mr-6 mb-5 w-[60%] flex justify-end">
  <img src="/img/arch-detail-dark.svg" alt="dagster-slurm backend architecture" class="w-full object-contain" />
</div>

---
transition: slide-left
layout: intro
class: bg-gradient-to-br from-purple-900 to-indigo-900 text-white
---

# Metaxy: No compute waste

<div class="abs-tr mt-5 mr-6">
  <img src="/img/metaxy.svg" alt="Metaxy" class="h-38 w-38 object-contain" />
</div>

- `dagster-slurm` solves runtime portability and observability.
- Metaxy solves incremental correctness inside each multimodal feature.
- Key objective: avoid unnecessary GPU/AI recomputation.
- Working principle: process only `new` and `stale`; do nothing when unchanged.

---
layout: center
class: bg-white text-black
---

# Where coarse invalidation wastes compute

<div class="grid place-items-center h-full">
  <img src="/img/anam-pipeline.svg" class="h-[420px] object-contain" />
</div>

---
layout: center
class: bg-white text-black
---

# Field-level provenance, not asset-level invalidation

<div class="grid place-items-center h-full">
  <img src="/img/anam-feature.svg" class="h-[420px] object-contain" />
</div>

---
layout: center
class: bg-white text-black
---

# Partial dependencies across fields

<div class="grid place-items-center h-full">
  <img src="/img/anam-anatomy.svg" class="h-[430px] object-contain" />
</div>

---

# Metaxy core loop (`resolve_update`)

````md magic-move {lines: true}
```python{1-5|7-10}
import metaxy as mx
import polars as pl

cfg = mx.init(config="metaxy.toml")
store = cfg.get_store("docling_dev")

source_samples = pl.DataFrame(...).with_columns(
  pl.struct("source_path",
  "file_size_bytes").alias("metaxy_provenance_by_field")
)
```

```python{1-9|10-22}
with store:
    src_inc = store.resolve_update("docling/source_documents",
samples=source_samples)

with store.open("w"):
    if len(src_inc.new) > 0:
        store.write("docling/source_documents", src_inc.new)
    if len(src_inc.stale) > 0:
        store.write("docling/source_documents", src_inc.stale)

with store:
    inc = store.resolve_update("docling/converted_documents")

parts = []
if len(inc.new) > 0:
    parts.append(inc.new.to_polars())
if len(inc.stale) > 0:
    parts.append(inc.stale.to_polars())

to_process = pl.concat(parts) if parts else pl.DataFrame()
if to_process.is_empty():
    print("Up to date: no work needed")
```
````

---
layout: two-cols-header
class: bg-white text-black
---

# Minimal ray data Docling mapper

<div class="abs-tr mt-4 mr-6">
  <img src="/img/docling-logo.svg" alt="Docling" class="h-16 object-contain" />
</div>

::left::

<div class="h-full flex items-center">
<div class="space-y-4 pl-6 pr-2 pb-4">

<ul class="space-y-2 text-[15px] leading-relaxed list-disc pl-5">
    <li><strong>Simply scale with Ray + Ray Data:</strong> table batches (`from_arrow -> map_batches`) parallelize work across cores/nodes.</li>
    <li><strong>Docling for parsing:</strong> robust PDF/document conversion with configurable model choices and output formats.</li>
    <li><strong>Keep compute bounded with Metaxy:</strong> only `new` + `stale` documents are sent to Ray; unchanged rows are skipped.</li>
  </ul>

<div class="flex items-center gap-2 pt-1 mb-3 text-xs text-slate-700">
    <span class="rounded-full border border-slate-300 px-2 py-1">Metaxy: what</span>
    <span class="rounded-full border border-slate-300 px-2 py-1">Ray: parallelism</span>
    <span class="rounded-full border border-slate-300 px-2 py-1">Docling: conversion</span>
  </div>
</div>
</div>

::right::

```python {1-3|5-15|17-22|all}
class DoclingMapper:
    def __call__(self, batch: pa.Table) -> pa.Table:
        out = []
        for row in batch.to_pylist():
            md = convert_pdf_with_docling(row["source_path"])
            out.append({
                "doc_uid": row["doc_uid"],
                "markdown_path": md.path,
                "num_pages": md.num_pages,
                "conversion_status": "ok",
                "elapsed_s": md.elapsed_s,
            })
        return pa.Table.from_pylist(out)

result_ds = rd.from_arrow(to_process.to_arrow()).map_batches(
DoclingMapper, batch_size=8
)
result_ds.write_datasink(
MetaxyDatasink(feature="docling/converted_documents", store=store, config=cfg)
)
```

---
layout: default
class: bg-slate-900 text-white
---

# <span class="!text-5xl">Demo: incremental updates</span>

````md magic-move {lines: true}
```text
========================================================================
ROUND 1: initial load
========================================================================

[root input]
┌────────────┬──────────────────┬───────┬──────────────────┐
│ sample_uid ┆ file_uri         ┆ value ┆ category         │
│ ---        ┆ ---              ┆ ---   ┆ ---              │
│ str        ┆ str              ┆ f64   ┆ str              │
╞════════════╪══════════════════╪═══════╪══════════════════╡
│ avatar_001 ┆ s3://anam/raw/av ┆ 10.0  ┆ video/full       │
│ avatar_002 ┆ s3://anam/raw/av ┆ 20.0  ┆ transcript/whisp │
│ avatar_003 ┆ s3://anam/raw/av ┆ 30.0  ┆ video/face_crop  │
└────────────┴──────────────────┴───────┴──────────────────┘

[processed latest rows]
┌────────────┬────────┬──────────────┐
│ sample_uid ┆ result ┆ value_bucket │
╞════════════╪════════╪══════════════╡
│ avatar_001 ┆ 100.0  ┆ lt_50        │
│ avatar_002 ┆ 400.0  ┆ lt_50        │
│ avatar_003 ┆ 900.0  ┆ lt_50        │
└────────────┴────────┴──────────────┘
[processed increment] new=3 stale=0 orphaned=0 processed=3
```

```text
========================================================================
ROUND 2: one changed + one added + one removed
========================================================================

[root input]
┌────────────┬──────────────────┬───────┬──────────────────┐
│ sample_uid ┆ file_uri         ┆ value ┆ category         │
╞════════════╪══════════════════╪═══════╪══════════════════╡
│ avatar_001 ┆ s3://anam/raw/av ┆ 10.0  ┆ video/full       │
│ avatar_002 ┆ s3://anam/raw/av ┆ 25.0  ┆ transcript/whisp │
│ avatar_004 ┆ s3://anam/raw/av ┆ 60.0  ┆ video/face_crop  │
└────────────┴──────────────────┴───────┴──────────────────┘

[processed latest rows]
┌────────────┬────────┬──────────────┐
│ sample_uid ┆ result ┆ value_bucket │
╞════════════╪════════╪══════════════╡
│ avatar_001 ┆ 100.0  ┆ lt_50        │
│ avatar_002 ┆ 625.0  ┆ lt_50        │
│ avatar_003 ┆ 900.0  ┆ lt_50        │
│ avatar_004 ┆ 3600.0 ┆ gte_50       │
└────────────┴────────┴──────────────┘
[processed increment] new=1 stale=1 orphaned=0 processed=2
```

```text
========================================================================
ROUND 3: no changes
========================================================================
[processed increment] new=0 stale=0 orphaned=0 processed=0
```
````

---
layout: center
class: bg-emerald-950 text-white
---

# <span class="!text-5xl">Takeaway</span>

Dagster + `dagster-slurm` gives portable orchestration from local dev to HPC.
Metaxy keeps recompute bounded by processing only `new` and `stale`.

<div class="mt-8 text-[20px] leading-relaxed">

- dagster-slurm: <a href="https://github.com/ascii-supply-networks/dagster-slurm/" target="_blank">github.com/ascii-supply-networks/dagster-slurm</a>
- Metaxy docs & Quickstart: <a href="https://docs.metaxy.io/latest/guide/quickstart/quickstart/" target="_blank">docs.metaxy.io/latest/guide/quickstart/quickstart/</a>

</div>

<div class="abs-b mb-6 w-full flex justify-center">
  <div class="flex items-center gap-8">
    <img src="/img/dagster.svg" alt="Dagster" class="h-32 w-32 object-contain" />
    <img src="/img/featured.png" alt="dagster-slurm" class="h-32 w-32 object-contain" />
    <img src="/img/metaxy.svg" alt="Metaxy" class="h-32 w-32 object-contain" />
    <img src="/img/docling-logo.svg" alt="Docling" class="h-32 w-32 object-contain" />
  </div>
</div>
