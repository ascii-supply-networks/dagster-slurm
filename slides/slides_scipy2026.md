---
theme: ./theme
title: "Dagster-Slurm: Productive Scientific Python on HPC"
info: |
  ## From laptop prototyping to sovereign supercomputers
  The same Dagster asset runs locally and on Slurm-backed HPC — with observability, lineage, and packaged environments preserved end-to-end.

  Learn more at [dagster-slurm](https://github.com/ascii-supply-networks/dagster-slurm)
class: text-center
drawings:
  persist: false
transition: slide-left
mdc: true
seoMeta:
  ogImage: auto
fonts:
  serif: 'EB Garamond'
  provider: google
routerMode: hash
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 1: TITLE  (dark navy)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-12 flex flex-col justify-between">
  <div class="eyebrow-light">SciPy 26</div>
  <div class="grid w-full gap-8 items-center" style="grid-template-columns:1.35fr 0.65fr">
    <div class="space-y-5">
      <h1 class="slide-title text-white" style="text-wrap:balance;font-size:4.75rem;line-height:1.02">
        dagster-slurm
      </h1>
      <p class="lead-dark" style="font-size:1.4rem;line-height:1.3;color:rgba(94,234,212,0.92);white-space:nowrap;letter-spacing:-0.005em">
        Modern data orchestration for Slurm-managed HPC.
      </p>
    </div>
    <div class="flex items-center justify-center">
      <a href="https://github.com/ascii-supply-networks/dagster-slurm">
        <img src="/img/dagster-slurm-logo.png" alt="dagster-slurm" class="w-auto object-contain rounded-2xl" style="max-height:52vh;filter:drop-shadow(0 8px 32px rgba(94,234,212,0.15))" />
      </a>
    </div>
  </div>
  <div class="flex items-center justify-between gap-6">
    <div class="flex items-center gap-8">
      <a href="https://docs.dagster.io/">
        <img src="/img/dagster-primary-mark.svg" alt="Dagster" class="h-12 w-auto object-contain" />
      </a>
      <a href="https://slurm.schedmd.com/">
        <img src="/img/slurm-logo.png" alt="Slurm" class="h-12 w-auto object-contain" style="filter:brightness(0) invert(1);opacity:0.94" />
      </a>
      <a href="https://pixi.sh/">
        <img src="/img/Paxton_Wand_FINAL-2.png" alt="pixi" class="h-14 w-auto object-contain" />
      </a>
    </div>
    <div class="mono-label text-right" style="color:rgba(94,234,212,0.78);font-size:1.05rem;line-height:1.45;letter-spacing:0.04em">
      <a href="https://georgheiler.com/" class="hover:text-teal-300">Georg Heiler</a>
      ·
      <a href="https://ascii.ac.at/person/hernan-picatto/" class="hover:text-teal-300">Hernan Picatto</a>
      <br/><span style="opacity:0.7">
        <a href="https://ascii.ac.at/" class="hover:text-teal-300">ASCII</a>
        ·
        <a href="https://csh.ac.at/" class="hover:text-teal-300">CSH Vienna</a>
      </span>
    </div>
  </div>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 2: ABOUT US  (warm light · 2-col speakers)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex items-center">
  <div class="w-full space-y-6">
    <div class="space-y-2">
      <div class="eyebrow">Who is presenting</div>
      <h1 class="slide-heading" style="text-wrap:balance;font-size:2.4rem">Practitioner perspective from ASCII</h1>
      <p class="lead" style="font-size:1rem;line-height:1.55;max-width:52rem">
        We build and operate the data and compute pipelines for firm-level supply chain research on large datasets.
      </p>
    </div>
    <div class="grid w-full gap-8" style="grid-template-columns:1fr 1fr;align-items:start">
      <div class="flex gap-5">
        <div class="w-40 flex-shrink-0 space-y-2">
          <div class="overflow-hidden rounded-lg aspect-[3/4] bg-neutral-200">
            <img
              src="/img/georg-heiler.jpg"
              class="h-full w-full object-cover object-[52%_28%] scale-[1.14]"
              alt="Georg Heiler"
            />
          </div>
        </div>
        <div class="flex-1 border-t border-neutral-300 pt-2">
          <div class="j-serif text-xl text-neutral-950">
            <a href="https://georgheiler.com/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Georg Heiler</a>
          </div>
          <div class="mt-2 text-xs leading-snug text-neutral-600">
            Co-founder of <a href="https://jubust.com/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Jubust</a><br/><br/>
            RSE @<a href="https://ascii.ac.at/" class="underline decoration-neutral-400/40 underline-offset-[0.14em]">ASCII</a>
            / <a href="https://csh.ac.at/" class="underline decoration-neutral-400/40 underline-offset-[0.14em]">CSH</a><br/><br/>
            Senior data expert @<a href="https://www.magenta.at/" class="underline decoration-neutral-400/40 underline-offset-[0.14em]">Magenta</a><br/><br/>
            Contributor: <span style="color:#CA8A04">Docling</span>, <span style="color:#9333EA">Metaxy</span><br/><br/>
            Maintainer of <a href="https://github.com/ascii-supply-networks/dagster-slurm" class="underline decoration-teal-700/40 underline-offset-[0.18em]">dagster-slurm</a>
          </div>
        </div>
      </div>
      <div class="flex gap-5">
        <div class="w-40 flex-shrink-0 space-y-2">
          <div class="overflow-hidden rounded-lg aspect-[3/4] bg-neutral-200">
            <img
              src="/img/hernan-picatto.jpg"
              class="h-full w-full object-cover object-center"
              alt="Hernan Picatto"
            />
          </div>
        </div>
        <div class="flex-1 border-t border-neutral-300 pt-2">
          <div class="j-serif text-xl text-neutral-950">
            <a href="https://ascii.ac.at/person/hernan-picatto/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Hernan Picatto</a>
          </div>
          <div class="mt-2 text-xs leading-snug text-neutral-600">
            Doctoral student @<a href="https://ascii.ac.at/" class="underline decoration-neutral-400/40 underline-offset-[0.14em]">ASCII</a><br/><br/>
            Firm-level supply chain interactions from web data<br/><br/>
            Graph reconstruction from public company websites; patterns over time<br/><br/>
            Heavy user of <a href="https://github.com/ascii-supply-networks/dagster-slurm" class="underline decoration-teal-700/40 underline-offset-[0.18em]">dagster-slurm</a>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 3: ASCII CONTEXT  (white · overview)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-center gap-4 relative">
  <a href="https://ascii.ac.at/" class="absolute top-6 right-16">
    <img src="/img/ascii-logo.svg" alt="ASCII" class="h-14 w-auto object-contain" />
  </a>
  <div class="max-w-5xl space-y-1 pr-40">
    <div class="eyebrow">Why this talk exists</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.2rem">Science groups need reproducible compute, not a second codebase for HPC</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      ASCII studies firm-level supply chain interactions at continental scale. The pipeline blends scraping, document AI, graph construction, and ML — and each of those steps has a different compute profile.
    </p>
  </div>
  <div class="grid w-full gap-3 items-stretch" style="grid-template-columns:1fr 1fr 1fr">
    <div class="border border-neutral-300 rounded-lg p-4 bg-white">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Scale</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Millions of firms</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">Continental, multi-country company coverage with recurring refreshes.</div>
    </div>
    <div class="border border-neutral-300 rounded-lg p-4 bg-white">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Data</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Heterogeneous web evidence</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">HTML, PDFs, registers, sanction lists, unstructured prose.</div>
    </div>
    <div class="border border-neutral-300 rounded-lg p-4 bg-white">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Compute</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">CPU · GPU · graph</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">Parsing, embeddings, GNN training, link prediction — each with different resource shapes.</div>
    </div>
  </div>
  <div class="teal-callout" style="padding:0.75rem 1rem">
    <div class="mono-label text-teal-700">Where we started — and why we moved on</div>
    <p class="mt-1 text-sm leading-relaxed text-neutral-700">
      We began on <strong>EMR Spark</strong>, already shaving ~50% off the Databricks surcharge
      (<a href="https://georgheiler.com/2024/06/21/cost-efficient-alternative-to-databricks-lock-in/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">blog</a>).
      Rising AI-compute demand outgrew our public-cloud budget. Today compute spans a <strong>local server</strong>, <strong>cloud</strong> partitions, and <strong>institutional HPC</strong> — three schedulers, one unified observability and control plane.
    </p>
  </div>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 4: THE GAP  (warm light · visual, less text)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-10 flex flex-col justify-center gap-10">
  <div class="space-y-3">
    <div class="eyebrow">The Gap</div>
    <h1 class="slide-heading" style="font-size:3.4rem;line-height:1.05;text-wrap:balance">
      Two tribes.<br/><span style="color:#0f766e">No shared tools.</span>
    </h1>
  </div>
  <div class="grid w-full items-center" style="grid-template-columns:1fr auto 1fr;gap:2.5rem">
    <div class="text-right space-y-1">
      <div style="font-size:4.5rem;line-height:1">🐍</div>
      <div class="j-serif text-2xl text-neutral-950">Python / SciPy</div>
      <div class="text-sm text-neutral-500">assets · notebooks · seconds</div>
    </div>
    <div class="flex flex-col items-center gap-2" style="min-width:10rem">
      <div class="j-serif text-6xl text-neutral-300" style="letter-spacing:0.1em">⇢ ? ⇠</div>
      <div class="mono-label text-neutral-400">no bridge</div>
    </div>
    <div class="text-left space-y-1">
      <div style="font-size:4.5rem;line-height:1">🏛️</div>
      <div class="j-serif text-2xl text-neutral-950">HPC</div>
      <div class="text-sm text-neutral-500">sbatch · modules · queue minutes</div>
    </div>
  </div>
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr 1fr">
    <div class="border-t border-neutral-300 pt-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Skill gap</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">CLI vs GUI, lockfile vs drift</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">Python / Dagster ships friendly GUIs and lockfiles in git. HPC stays CLI-only, with opaque module state on the compute node.</div>
    </div>
    <div class="border-t border-neutral-300 pt-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Human gap</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">Parallel worlds</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">Data / ML engineers and HPC operators rarely sit in the same room — different vocabulary, different priorities.</div>
    </div>
    <div class="border-t border-neutral-300 pt-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Feedback-loop gap</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">Agents wait in queue</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">Agentic engineering needs seconds. Fair-share queues stretch from minutes to days.</div>
    </div>
  </div>
</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 5: ARCHITECTURE  (dark navy · layered)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full py-4">
  <div class="max-w-6xl mx-auto px-16 flex flex-col justify-center gap-3 h-full">
    <div class="max-w-4xl space-y-1">
      <div class="eyebrow-light">Architecture</div>
      <h1 class="slide-heading text-white" style="text-wrap:balance;font-size:2.5rem">One graph — pluggable execution.</h1>
    </div>
    <div class="grid gap-3" style="grid-template-columns:1fr 1fr">
      <div class="stage-card">
        <div class="mono-label text-teal-300">Asset Layer</div>
        <div class="j-serif mt-1 text-xl text-white">Dagster</div>
        <p class="mt-2 text-sm leading-snug text-slate-300">Typed assets, lineage, schedules, retries, backfills. Unchanged when the target switches from laptop to Slurm.</p>
      </div>
      <div class="stage-card">
        <div class="mono-label text-teal-300">Compute Facade</div>
        <div class="j-serif mt-1 text-xl text-white">ComputeResource</div>
        <p class="mt-2 text-sm leading-snug text-slate-300">Pluggable abstraction over the execution target. Asset code never branches on <code class="text-xs">mode</code> — flip configuration instead.</p>
      </div>
      <div class="stage-card">
        <div class="mono-label text-teal-300">Launcher (what to run)</div>
        <div class="j-serif mt-1 text-xl text-white">Bash · Ray</div>
        <p class="mt-2 text-sm leading-snug text-slate-300">Plan generators for plain scripts or multi-node Ray clusters. New launchers (MPI, Dask, Spark, …) are additive.</p>
      </div>
      <div class="stage-card">
        <div class="mono-label text-teal-300">Execution + Packaging</div>
        <div class="j-serif mt-1 text-xl text-white">dagster-pipes · pixi-pack</div>
        <p class="mt-2 text-sm leading-snug text-slate-300">Structured metrics and events stream back over <a href="https://docs.dagster.io/guides/build/external-pipelines" class="text-teal-300 underline">dagster-pipes</a>; environments ship reproducibly via <a href="https://github.com/Quantco/pixi-pack" class="text-teal-300 underline">pixi-pack</a>.</p>
      </div>
    </div>
    <div class="dark-callout" style="padding:0.5rem 0.75rem">
      <div class="mono-label text-teal-200">Why four layers, not one</div>
      <div class="mt-1 text-sm leading-snug text-white">
        Targets can be swapped without touching asset code — not only <strong>laptop → HPC</strong>, but also <strong>one HPC site for another</strong> as quotas or GPU availability shift.
      </div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 6: CODE STAYS THE SAME  (white · magic-move code)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-start gap-3">
  <div class="max-w-4xl space-y-1">
    <div class="eyebrow">Portable Asset Code</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.15rem">Same asset. Same launcher. Different target.</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">Only configuration decides whether the payload runs locally or lands as a Slurm job. Dagster Pipes carries structured events back either way.</p>
  </div>

````md magic-move {lines: true}
```python
# asset definition — identical in dev and on HPC
import dagster as dg
from dagster_slurm import BashLauncher, ComputeResource

@dg.asset
def train_classifier(context: dg.AssetExecutionContext, compute: ComputeResource):
    script_path = dg.file_relative_path(__file__, "../workloads/train.py")
    completed = compute.run(
        context=context,
        payload_path=script_path,
        launcher=BashLauncher(),
        extra_env={"EPOCHS": "3", "BATCH_SIZE": "128"},
    )
    yield from completed.get_results()
```

```python
# the payload — a regular Python script, instrumented by Pipes
import os
from dagster_pipes import PipesContext, open_dagster_pipes

def main():
    context = PipesContext.get()
    epochs = int(os.environ["EPOCHS"])              # injected via extra_env
    context.log.info(f"Training for {epochs} epochs")
    # run PyTorch / sklearn / ray.train / whatever
    context.report_asset_materialization(
        metadata={"epochs": epochs, "rows": 1000, "accuracy": 0.91}
    )

if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
```
````

</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 7: SECTION HEADER — dagster-slurm AT ASCII  (dark)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-20 flex flex-col justify-center gap-6">
  <div class="eyebrow-light">Part II · In Depth</div>
  <h1 class="slide-title text-white" style="font-size:4rem;line-height:1.05;text-wrap:balance">
    dagster-slurm in production at ASCII
  </h1>
  <p class="lead-dark" style="max-width:56rem">
    Real workloads, real clusters, real blockers. What we learned applying dagster-slurm to supply chain intelligence research across CSH compute, VSC-5, and Musica.
  </p>
  <div class="flex items-center gap-6 mt-4">
    <img src="/img/ascii_overview.svg" class="h-20 w-auto object-contain bg-white/90 rounded p-2" alt="ASCII overview" />
    <div class="mono-label" style="color:rgba(94,234,212,0.8);font-size:0.95rem;line-height:1.5;letter-spacing:0.08em">
      ASCII · Austrian Supply Chain<br/>Intelligence Institute
    </div>
  </div>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 8: THE ASCII USE CASE  (light)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex items-center">
  <div class="grid w-full gap-10" style="grid-template-columns:1fr 1fr;align-items:stretch">
    <div class="space-y-4">
      <div class="eyebrow">The Workload</div>
      <h1 class="slide-heading" style="text-wrap:balance;font-size:2.2rem">Reconstructing continental supply chain graphs from the open web</h1>
      <p class="lead" style="font-size:1rem;line-height:1.55">
        Millions of firm websites contain evidence of commercial relationships: supplier pages, customer testimonials, case studies, patents, registry filings. Turning that evidence into a structured graph is a heavy, heterogeneous pipeline.
      </p>
      <div class="teal-callout" style="padding:0.75rem 1rem">
        <div class="mono-label text-teal-700">Economic stakes</div>
        <p class="mt-1 text-sm leading-relaxed text-neutral-700">
          Policymakers and firms need early warning for cascading shortages (semiconductors, rare earths, pharma intermediates). The graph is only as useful as it is fresh.
        </p>
      </div>
    </div>
    <div class="space-y-0">
      <div class="border-t border-neutral-300 py-3">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Step 1</div>
        <div class="j-serif mt-1 text-xl text-neutral-950">Ingest web snapshots</div>
        <div class="mt-1 text-sm leading-snug text-neutral-600">Filter Common Crawl / public web archives down to relevant firm pages; dedup and checkpoint per firm.</div>
      </div>
      <div class="border-t border-neutral-300 py-3">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Step 2</div>
        <div class="j-serif mt-1 text-xl text-neutral-950">Parse &amp; extract</div>
        <div class="mt-1 text-sm leading-snug text-neutral-600">HTML normalization, PDF parsing via Docling, NER, relation extraction. This is where GPUs pay off.</div>
      </div>
      <div class="border-t border-neutral-300 py-3">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Step 3</div>
        <div class="j-serif mt-1 text-xl text-neutral-950">Link &amp; score</div>
        <div class="mt-1 text-sm leading-snug text-neutral-600">Entity resolution across languages, graph construction, edge confidence via GNN / LLM-as-judge.</div>
      </div>
      <div class="border-t border-neutral-300 py-3">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Step 4</div>
        <div class="j-serif mt-1 text-xl text-neutral-950">Analyze &amp; serve</div>
        <div class="mt-1 text-sm leading-snug text-neutral-600">Cascade simulations, temporal patterns, interactive exploration for researchers.</div>
      </div>
    </div>
  </div>
</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 9: PIPELINE SHAPE  (dark · arch diagram)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-start gap-4">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow-light">How the pieces fit</div>
    <h1 class="slide-heading text-white" style="text-wrap:balance;font-size:2.2rem">One Dagster project spans laptop, CI, and HPC</h1>
    <p class="text-sm leading-snug text-slate-300 max-w-4xl">
      The asset graph is the contract. Each asset declares its launcher and resource shape; dagster-slurm handles sbatch translation, module/partition selection, and metrics collection. Operators see one timeline.
    </p>
  </div>
  <div class="flex items-center justify-center flex-1">
    <img src="/img/arch-detail-dark.svg" alt="dagster-slurm architecture" class="max-h-full max-w-full object-contain" />
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 10: WHAT ASCII RUNS WHERE  (white)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-center gap-4">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Site Mapping</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.2rem">Different clusters for different jobs — one asset graph decides</h1>
  </div>
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr 1fr">
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Local / CSH / CI</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Iterate &amp; test</div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">
        Laptop, shared CSH nodes, and slurm-in-docker CI for fast iteration and regression tests. Same <code class="text-xs">ComputeResource</code>, local launcher.
      </p>
    </div>
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Cloud</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Burst &amp; elastic jobs</div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">
        Managed Spark / GPU instances when queue wait or specific hardware doesn't match HPC availability. Same asset graph; a different <code class="text-xs">ComputeResource</code>.
      </p>
    </div>
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">HPC</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Heavy CPU &amp; GPU</div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">
        Institutional Slurm clusters for parallel parsing and multi-node GPU training. <code class="text-xs">BashLauncher</code> for arrays; <code class="text-xs">RayLauncher</code> for multi-node.
      </p>
    </div>
  </div>
  <div class="teal-callout" style="padding:0.75rem 1rem">
    <div class="mono-label text-teal-700">Operational payoff</div>
    <p class="mt-1 text-sm leading-relaxed text-neutral-700">
      One Dagster UI shows local unit tests, cloud burst jobs, and HPC training side by side. Reviewers click an asset, see its Slurm job ID, memory peak, CPU efficiency, and stdout — without SSH.
    </p>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 11: WHAT DAGSTER-SLURM HANDLES  (white · screenshot)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-4 flex flex-col justify-center gap-3">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Single Pane Of Glass</div>
    <h1 class="slide-heading" style="font-size:2.1rem">Slurm metrics and Pipes logs land in Dagster metadata</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      Memory peak, CPU efficiency, node-hours, and elapsed time show up on each materialization. Analysts inspect the same artifact view whether the job ran on a laptop or on Leonardo.
    </p>
  </div>
  <div class="grid grid-cols-3 gap-4 items-start">
    <div class="col-span-2 rounded-lg overflow-hidden border border-neutral-200">
      <img src="/img/process_data_run_view.png" class="w-full h-auto object-contain" alt="Dagster run view with Slurm metrics" />
    </div>
    <div class="space-y-0">
      <div class="border-t border-neutral-300 py-2">
        <h3 class="j-serif text-base text-teal-700">sbatch translation</h3>
        <p class="mt-0.5 text-xs leading-snug text-neutral-600">Asset resource hints become Slurm directives — partition, account, memory, GPU type.</p>
      </div>
      <div class="border-t border-neutral-300 py-2">
        <h3 class="j-serif text-base text-teal-700">Environment packaging</h3>
        <p class="mt-0.5 text-xs leading-snug text-neutral-600"><code class="text-xs">pixi-pack</code> ships a self-contained conda env to the cluster — no module system divergence.</p>
      </div>
      <div class="border-t border-neutral-300 py-2">
        <h3 class="j-serif text-base text-teal-700">Flexible SSH auth</h3>
        <p class="mt-0.5 text-xs leading-snug text-neutral-600">Password, SSH key, or short-lived SSH certificates via <a href="https://smallstep.com/docs/step-ca/" class="underline decoration-teal-700/40 underline-offset-[0.14em]">step-ca</a> — works with whatever the site mandates.</p>
      </div>
      <div class="border-t border-neutral-300 py-2">
        <h3 class="j-serif text-base text-teal-700">Pipes events + metrics</h3>
        <p class="mt-0.5 text-xs leading-snug text-neutral-600">Structured logs, progress, and materializations stream back <em>during</em> the job; Slurm <code class="text-xs">sacct</code> numbers (memory peak, CPU efficiency) land on the same materialization.</p>
      </div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 12: ASSET VIEW  (white · asset-level metadata)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-4 flex flex-col justify-start gap-3">
  <div class="flex items-baseline justify-between gap-6">
    <h1 class="slide-heading" style="font-size:1.9rem">Every asset — its own dashboard</h1>
    <div class="mono-label text-neutral-400">metadata · lineage · source · history</div>
  </div>
  <div class="flex-1 rounded-lg overflow-hidden border border-neutral-200 min-h-0">
    <img src="/img/process_data_asset_view.png" class="w-full h-full object-contain" alt="Dagster asset view with plots, lineage, and source code" />
  </div>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 13: METAXY AS REFERENCE  (light · brief mention)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex items-center">
  <div class="grid w-full gap-10" style="grid-template-columns:0.85fr 1.15fr;align-items:center">
    <div class="space-y-3">
      <img src="/img/metaxy.svg" alt="Metaxy" class="h-14 w-auto object-contain" />
      <div class="eyebrow">Companion Project</div>
      <h1 class="slide-heading" style="text-wrap:balance;font-size:2rem">Metaxy — perfecting the art of doing nothing</h1>
      <p class="lead" style="font-size:1rem;line-height:1.55">
        dagster-slurm makes HPC usable. <strong>Metaxy</strong> makes HPC spending <em>optional</em> — by skipping work whose inputs have not changed.
      </p>
    </div>
    <div class="space-y-0">
      <div class="border-t border-neutral-300 py-3">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Pairs with dagster-slurm</div>
        <div class="j-serif mt-1 text-lg text-neutral-950">Scope first, then schedule</div>
        <div class="mt-1 text-sm leading-snug text-neutral-600">Metaxy answers <em>what is stale</em>. dagster-slurm answers <em>where to run it</em>.</div>
      </div>
      <div class="border-t border-neutral-300 py-3">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Full deep-dive → virtual poster</div>
        <div class="j-serif mt-1 text-lg text-neutral-950">See you at the poster session</div>
        <div class="mt-1 text-sm leading-snug text-neutral-600">
          <a href="https://docs.metaxy.io/latest/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">docs.metaxy.io</a>
          ·
          <a href="https://georgheiler.com/2026/02/22/metaxy-dagster-slurm-multimodal/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Metaxy + dagster-slurm blog</a>
        </div>
      </div>
    </div>
  </div>
</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 14: EU SOVEREIGN GPU CLOUD  (statement · dark)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-16 flex flex-col justify-center items-center gap-8 text-center">
  <div class="eyebrow-light">A Call To The Room</div>
  <h1 class="slide-title text-white" style="text-wrap:balance;font-size:4rem;line-height:1.05">
    EU sovereign GPU cloud<br/>does not come out of nowhere.
  </h1>
  <p class="j-serif" style="font-size:1.6rem;line-height:1.35;color:rgba(94,234,212,0.88);text-wrap:balance;max-width:48rem">
    Maybe this project can help make HPC systems more accessible.
  </p>
  <a href="https://github.com/ascii-supply-networks/dagster-slurm" class="mt-4">
    <img src="/img/featured.png" alt="dagster-slurm" class="h-28 w-auto object-contain rounded-xl" style="filter:drop-shadow(0 8px 32px rgba(94,234,212,0.2))" />
  </a>
</div>

---
layout: dark-closing
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 16: CLOSING  (dark · 3 takeaways)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-16 flex flex-col justify-between">
  <div class="eyebrow-light">Three Takeaways</div>
  <div class="space-y-6 border-l pl-8" style="border-color:rgba(255,255,255,0.2)">
    <p class="j-serif text-white" style="font-size:2.8rem;line-height:1.15;text-wrap:balance">
      One asset graph. Laptop to HPC.
    </p>
    <p class="j-serif" style="font-size:2.8rem;line-height:1.15;text-wrap:balance;color:rgba(255,255,255,0.55)">
      Slurm stays Slurm. Python stays Python.
    </p>
    <p class="j-serif" style="font-size:2.8rem;line-height:1.15;text-wrap:balance;color:rgba(255,255,255,0.25)">
      Make sovereign compute actually usable.
    </p>
  </div>
  <div class="flex items-center justify-between">
    <div class="mono-label" style="color:rgba(94,234,212,0.45);line-height:1.8">
      <a href="https://github.com/ascii-supply-networks/dagster-slurm" class="hover:text-teal-300">DAGSTER-SLURM</a> · <a href="https://dagster-slurm.geoheil.com/" class="hover:text-teal-300">DOCS</a> · <a href="https://docs.metaxy.io/latest/" class="hover:text-teal-300">METAXY</a>
    </div>
    <div class="mono-label text-right" style="color:rgba(94,234,212,0.78);line-height:1.45;font-size:1.05rem;letter-spacing:0.04em">
      <a href="https://georgheiler.com/" class="hover:text-teal-300">Georg Heiler</a>
      · <a href="https://ascii.ac.at/person/hernan-picatto/" class="hover:text-teal-300">Hernan Picatto</a><br/>
      <span style="opacity:0.7">SciPy 2026</span>
    </div>
  </div>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 16: ACKNOWLEDGEMENTS  (light)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-10 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">With Gratitude</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.4rem">Acknowledgements</h1>
  </div>
  <p class="lead" style="font-size:1rem;line-height:1.65;max-width:60rem">
    We thank the operations teams at the <strong>Austrian Scientific Computing (ASC)</strong> — VSC-5 — and <strong>CINECA Leonardo</strong> for early feedback, and the <strong>Dagster community</strong> for discussions around orchestrating HPC workloads. Funding and in-kind support were provided by the <a href="https://csh.ac.at/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Complexity Science Hub Vienna</a> and the <a href="https://ascii.ac.at/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Austrian Supply Chain Intelligence Institute (ASCII)</a>.
  </p>
  <p class="text-sm leading-relaxed text-neutral-600 max-w-5xl">
    This work was completed in part at the <a href="https://www.openhackathons.org/s/siteevent/a0CUP000013Tp8f2AC/se000375" class="underline decoration-neutral-400/40 underline-offset-[0.14em]">EUROCC AI Hackathon 2025</a>, part of the Open Hackathons program. The authors would like to acknowledge <strong>OpenACC-Standard.org</strong> for their support.
  </p>
</div>

---
layout: white
---

<!-- ══════════════════════════════════════════════════════
     APPENDIX
══════════════════════════════════════════════════════ -->
<div class="h-full max-w-6xl mx-auto px-16 py-12 flex items-center justify-center">
  <h1 class="slide-heading text-center" style="text-wrap:balance">Appendix</h1>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     BACKUP: LINKS  (white · reference list)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-2">
    <div class="eyebrow">References</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.5rem">Links and resources</h1>
  </div>
  <div class="space-y-0">
    <div class="grid gap-5 border-t border-neutral-300 py-2.5" style="grid-template-columns:200px 1fr">
      <div class="text-sm font-semibold uppercase tracking-[0.22em] text-teal-700">dagster-slurm</div>
      <a href="https://github.com/ascii-supply-networks/dagster-slurm" class="text-base text-neutral-700 underline">github.com/ascii-supply-networks/dagster-slurm</a>
    </div>
    <div class="grid gap-5 border-t border-neutral-300 py-2.5" style="grid-template-columns:200px 1fr">
      <div class="text-sm font-semibold uppercase tracking-[0.22em] text-teal-700">Docs</div>
      <a href="https://dagster-slurm.geoheil.com/" class="text-base text-neutral-700 underline">dagster-slurm.geoheil.com</a>
    </div>
    <div class="grid gap-5 border-t border-neutral-300 py-2.5" style="grid-template-columns:200px 1fr">
      <div class="text-sm font-semibold uppercase tracking-[0.22em] text-teal-700">JOSS paper</div>
      <a href="https://doi.org/10.21105/joss.09795" class="text-base text-neutral-700 underline">doi.org/10.21105/joss.09795</a>
    </div>
    <div class="grid gap-5 border-t border-neutral-300 py-2.5" style="grid-template-columns:200px 1fr">
      <div class="text-sm font-semibold uppercase tracking-[0.22em] text-teal-700">Metaxy + Slurm</div>
      <a href="https://georgheiler.com/2026/02/22/metaxy-dagster-slurm-multimodal/" class="text-base text-neutral-700 underline">Scaling multimodal pipelines with Metaxy + dagster-slurm</a>
    </div>
    <div class="grid gap-5 border-t border-neutral-300 py-2.5" style="grid-template-columns:200px 1fr">
      <div class="text-sm font-semibold uppercase tracking-[0.22em] text-teal-700">Supercomputing essay</div>
      <a href="https://georgheiler.com/2025/10/24/rediscovering-the-super-in-supercomputing/" class="text-base text-neutral-700 underline">Rediscovering the Super in Supercomputing</a>
    </div>
  </div>
</div>
