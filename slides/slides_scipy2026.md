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
  ogTitle: "dagster-slurm at SciPy 2026"
  ogDescription: "From laptop to supercomputer without rewriting your pipeline — Dagster + Slurm for productive scientific Python on HPC."
  ogUrl: https://github.com/ascii-supply-networks/dagster-slurm/
favicon: ./img/dagster-slurm-logo.png
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
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 5: DIFFERENT STRENGTHS  (white · 3-col)
     Each side does what it is good at. The bridge is what's missing.
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Closing The Gap</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.2rem">Different strengths, better together</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      The two worlds are not redundant — they are complementary. The missing piece is the bridge that lets each keep its strengths.
    </p>
  </div>
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr 1fr;align-items:stretch">
    <div class="border border-neutral-300 rounded-lg p-4 bg-white flex flex-col">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Data Orchestrator</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Plans the work</div>
      <ul class="mt-3 space-y-1.5 text-sm leading-snug text-neutral-600 list-disc pl-4">
        <li>Models data products and refresh policies</li>
        <li>Captures lineage, metadata, and failures</li>
        <li>Keeps engineers productive with local runs and data quality tests</li>
      </ul>
    </div>
    <div class="border border-neutral-300 rounded-lg p-4 bg-white flex flex-col">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Slurm + HPC</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Owns the execution</div>
      <ul class="mt-3 space-y-1.5 text-sm leading-snug text-neutral-600 list-disc pl-4">
        <li>Maximises utilisation of scarce accelerators</li>
        <li>Enforces fair-share, placement, and resource binding</li>
        <li>Provides high-performance filesystems and interconnects</li>
      </ul>
    </div>
    <div class="border-2 border-teal-600 rounded-lg p-4 bg-teal-50/60 flex flex-col">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">dagster-slurm</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Is the bridge</div>
      <ul class="mt-3 space-y-1.5 text-sm leading-snug text-neutral-700 list-disc pl-4">
        <li>Dagster plans; Slurm owns physical execution</li>
        <li>One orchestrator spans HPC and non-HPC workloads</li>
        <li>Unified observability instead of two operational universes</li>
      </ul>
    </div>
  </div>
  <div class="teal-callout" style="padding:0.7rem 1rem">
    <p class="text-sm leading-relaxed text-neutral-700">
      The orchestrator: Fast feedback loop. <br> The cluster: massive scale. <br>The bridge: a thin Python library — not a new platform to operate.
    </p>
  </div>
</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 6: ARCHITECTURE  (dark navy · layered building-blocks)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full py-4">
  <div class="max-w-6xl mx-auto px-16 flex flex-col justify-center gap-3 h-full">
    <div class="max-w-4xl space-y-1">
      <div class="eyebrow-light">Architecture</div>
      <h1 class="slide-heading text-white" style="text-wrap:balance;font-size:2.4rem">One graph — pluggable execution.</h1>
    </div>
    <!-- Layered building-blocks stack -->
    <div class="flex flex-col gap-1.5">
      <div class="rounded-md border border-teal-400/40 bg-teal-900/20 px-4 py-2 flex items-center gap-4">
        <div class="mono-label text-teal-300 w-40">Your code</div>
        <div class="j-serif text-lg text-white">Python asset</div>
        <div class="text-xs text-slate-400">configurable IO paths · unchanged local dev ↔ HPC</div>
      </div>
      <div class="rounded-md border border-slate-500/50 bg-slate-800/50 px-4 py-2 flex items-center gap-4">
        <div class="mono-label text-teal-300 w-40">Asset layer</div>
        <div class="j-serif text-lg text-white">Dagster</div>
        <div class="text-xs text-slate-400">lineage · schedules · retries · backfills · UI</div>
      </div>
      <div class="rounded-md border border-slate-500/50 bg-slate-800/50 px-4 py-2 flex items-center gap-4">
        <div class="mono-label text-teal-300 w-40">Compute facade</div>
        <div class="j-serif text-lg text-white">ComputeResource</div>
        <div class="text-xs text-slate-400">mode = local · slurm</div>
      </div>
      <div class="rounded-md border border-slate-500/50 bg-slate-800/50 px-4 py-2 flex items-center gap-4">
        <div class="mono-label text-teal-300 w-40">Launcher</div>
        <div class="j-serif text-lg text-white">Bash · Ray</div>
        <div class="text-xs text-slate-400">what to run — Spark / MPI / Dask / … additive</div>
      </div>
      <div class="rounded-md border border-slate-500/50 bg-slate-800/50 px-4 py-2 flex items-center gap-4">
        <div class="mono-label text-teal-300 w-40">Transport + env</div>
        <div class="j-serif text-lg text-white">dagster-pipes · pixi-pack</div>
        <div class="text-xs text-slate-400">bidirectional events · reproducible conda envs</div>
      </div>
      <div class="rounded-md border border-teal-400/40 bg-teal-900/20 px-4 py-2 flex items-center gap-4">
        <div class="mono-label text-teal-300 w-40">Target</div>
        <div class="j-serif text-lg text-white">laptop · Slurm</div>
      </div>
    </div>
    <div class="dark-callout" style="padding:0.5rem 0.75rem">
      <div class="mono-label text-teal-200">Why layers, not a monolith</div>
      <div class="mt-1 text-sm leading-snug text-white">
        Swap targets without touching asset code — <strong>laptop → HPC</strong>, or <strong>one HPC site for another</strong> as quotas and GPU availability shift.
      </div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 7: CODE STAYS THE SAME  (white · magic-move code)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-start gap-3">
  <div class="max-w-4xl space-y-1">
    <div class="eyebrow">Portable Asset Code</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.15rem">Same asset. Same payload. Different target.</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">Only the <code class="text-xs">ComputeResource</code> config decides whether the payload runs locally or lands as a Slurm job. Asset and payload stay identical.</p>
  </div>

````md magic-move {lines: true}
```python
# the asset — identical in dev and on HPC
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

```python
# dev laptop (no SSH, no Slurm)
from dagster_slurm import ComputeResource, BashLauncher

compute = ComputeResource(
    mode="local",
    default_launcher=BashLauncher(),
)
```

```python
# HPC (same asset, same payload, different target)
from dagster_slurm import (
    ComputeResource, SlurmResource, SSHConnectionResource,
    SlurmQueueConfig, BashLauncher,
)

compute = ComputeResource(
    mode="slurm",
    slurm=SlurmResource(
        ssh=SSHConnectionResource(host="vsc5.vsc.ac.at", user="scipyuser"),
        queue=SlurmQueueConfig(
            partition="zen3_0512_a100x2",   # site-specific
            qos="zen3_0512_a100x2",         # quota class on that partition
            num_nodes=1,
            gpus_per_node=1,
            time_limit="04:00:00",
        ),
        remote_base="$HOME/dagster_runs",
    ),
    default_launcher=BashLauncher(),
    pack_platform="linux-64",               # pixi-pack target arch
)
```
````

</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 8: COMPUTE FLEX  (white · laptop↔cluster visual)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-start gap-3">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Compute Flexibility</div>
    <h1 class="slide-heading" style="font-size:2.15rem;line-height:1.05">One asset, three runtimes.</h1>
    <p class="text-sm leading-snug text-neutral-600 max-w-4xl">Scale without rewriting — <strong>Bash on your laptop</strong> for quick iteration, <strong>Ray on one box</strong> for realistic multi-process dev, <strong>Ray on Slurm</strong> when you need multi-node GPUs.</p>
  </div>
  <div class="flex-1 min-h-0 flex items-center justify-center">
    <img src="/img/compute-flex.svg" class="w-full h-full object-contain" alt="Three runtime tiers side by side: Script local (BashLauncher on laptop), Ray local (RayLauncher on one box), and Ray elastic (RayLauncher on k8s or Slurm HPC) — same asset graph, configuration picks the runtime." />
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 9: PIPES MENTAL MODEL  (white · bidirectional messaging)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-5 flex flex-col justify-start gap-3">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">How the remote job talks back</div>
    <h1 class="slide-heading" style="font-size:2.05rem;line-height:1.05">Dagster Pipes — one protocol, two directions</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      <a href="https://docs.dagster.io/guides/build/external-pipelines" class="underline decoration-teal-700/40 underline-offset-[0.14em]">Pipes</a> is the thin wire between the Dagster process and the remote payload. Context and parameters go out; structured logs, progress, and materialization metadata come back — during the run, not after.
    </p>
  </div>
  <div class="flex-1 min-h-0 flex items-center justify-center">
    <img src="/img/pipes-architecture.svg" class="max-h-full max-w-full object-contain" alt="Dagster Pipes architecture — asset process sends context to the external process; the external process streams logs, events, and metadata back." />
  </div>
  <div class="grid gap-3" style="grid-template-columns:1fr 1fr 1fr">
    <div class="border-t border-neutral-300 py-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">→ Outbound</div>
      <div class="j-serif mt-0.5 text-base text-neutral-950">Context + params</div>
      <div class="mt-0.5 text-xs leading-snug text-neutral-600">Asset key, run id, partition, <code class="text-xs">extras</code>, env vars — serialised into the job.</div>
    </div>
    <div class="border-t border-neutral-300 py-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">← Inbound</div>
      <div class="j-serif mt-0.5 text-base text-neutral-950">Events + metrics</div>
      <div class="mt-0.5 text-xs leading-snug text-neutral-600">Log lines, progress, asset checks, <code class="text-xs">report_asset_materialization()</code> with typed metadata.</div>
    </div>
    <div class="border-t border-neutral-300 py-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Transport</div>
      <div class="j-serif mt-0.5 text-base text-neutral-950">File or stream</div>
      <div class="mt-0.5 text-xs leading-snug text-neutral-600">Shared FS on HPC; local pipe in dev. No daemon, no broker to operate.</div>
    </div>
  </div>
</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 10: SECTION HEADER — dagster-slurm AT ASCII  (dark)
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
     SLIDE 11: THE ASCII USE CASE  (light)
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
     SLIDE 12: PIPELINE SHAPE  (dark · arch diagram)
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
     SLIDE 13: WHAT ASCII RUNS WHERE  (white)
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
     SLIDE 14: WHAT DAGSTER-SLURM HANDLES  (white · screenshot)
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
     SLIDE 15: ASSET VIEW  (white · asset-level metadata)
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
layout: white
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 16: QUICKSTART  (white · try it yourself)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-center gap-4">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Try It Yourself</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.1rem">Real Slurm on your laptop — no cluster account required</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      The example ships a Dockerised Slurm edge node. The same asset submits through real <code class="text-xs">sbatch</code> / <code class="text-xs">sacct</code> / <code class="text-xs">squeue</code> — locally.
    </p>
  </div>
  <div class="grid gap-4 items-start" style="grid-template-columns:1.1fr 0.9fr">
    <div class="rounded-lg bg-neutral-950 px-5 py-3.5">
      <pre class="font-mono text-[0.82rem] leading-[1.55] text-slate-100 m-0"><span class="text-slate-500"># clone</span>
git clone https://github.com/ascii-supply-networks/dagster-slurm
cd dagster-slurm/examples
<span>&nbsp;</span>
<span class="text-slate-500"># dev on the laptop — Dagster UI on :3000</span>
pixi run start
<span>&nbsp;</span>
<span class="text-slate-500"># flip config → submits to Slurm-in-Docker</span>
pixi run start-staging</pre>
    </div>
    <div class="space-y-0">
      <div class="border-t border-neutral-300 py-2.5">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">The magic moment</div>
        <div class="j-serif mt-1 text-base text-neutral-950">Same asset, real Slurm</div>
        <div class="mt-1 text-xs leading-snug text-neutral-600">Identical Python runs in dev → then submits through a real local Slurm stack. Same UI, sacct metrics, job IDs.</div>
      </div>
      <div class="border-t border-neutral-300 py-2.5">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Install surface</div>
        <div class="j-serif mt-1 text-base text-neutral-950"><code class="text-sm">pip install dagster-slurm</code></div>
        <div class="mt-1 text-xs leading-snug text-neutral-600">Or <code class="text-xs">pixi add --pypi dagster-slurm</code>.</div>
      </div>
    </div>
  </div>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 17: METAXY AS REFERENCE  (light · brief mention)
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
     SLIDE 18: A CALL TO THE ROOM  (statement · dark)
     Universal framing: Slurm lives everywhere — make it usable.
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-16 flex flex-col justify-center items-center gap-8 text-center">
  <div class="eyebrow-light">A Call To The Room</div>
  <h1 class="slide-title text-white" style="text-wrap:balance;font-size:4rem;line-height:1.05">
    Sovereign compute<br/>does not come out of nowhere.
  </h1>
  <p class="j-serif" style="font-size:1.6rem;line-height:1.35;color:rgba(94,234,212,0.88);text-wrap:balance;max-width:52rem">
    Most research GPUs sit behind Slurm. Make them usable from the same Python workflow you already run on your laptop — wherever your institution hosts them.
  </p>
  <a href="https://github.com/ascii-supply-networks/dagster-slurm" class="mt-4">
    <img src="/img/featured.png" alt="dagster-slurm" class="h-28 w-auto object-contain rounded-xl" style="filter:drop-shadow(0 8px 32px rgba(94,234,212,0.2))" />
  </a>
</div>

---
layout: dark-closing
---

<!-- ──────────────────────────────────────────────────────
     SLIDE 19: CLOSING  (dark · 3 takeaways)
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
     SLIDE 20: ACKNOWLEDGEMENTS  (light)
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
