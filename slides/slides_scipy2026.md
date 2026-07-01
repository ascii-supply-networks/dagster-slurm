---
theme: ./theme
title: "Dagster-Slurm: Productive Scientific Python on HPC"
info: |
  ## From laptop prototyping to Slurm-backed HPC
  The same Dagster asset runs locally and on Slurm-backed HPC. Observability, lineage, and packaged environments stay intact end to end.

  Learn more at [dagster-slurm](https://github.com/ascii-supply-networks/dagster-slurm)
class: text-center
drawings:
  persist: false
transition: slide-left
mdc: true
seoMeta:
  ogImage: auto
  ogTitle: "dagster-slurm at SciPy 2026"
  ogDescription: "From laptop to supercomputer without rewriting your pipeline. Dagster plus Slurm for productive scientific Python on HPC."
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
layout: dark
---

<!-- ══════════════════════════════════════════════════════
     SECTION 1 · THE PROBLEM  (divider · dark)
══════════════════════════════════════════════════════ -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-20 flex flex-col justify-center gap-6">
  <div class="eyebrow-light">Part 1 · The problem</div>
  <h1 class="slide-title text-white" style="font-size:3.8rem;line-height:1.05;text-wrap:balance">
    Why HPC and data orchestration<br/>are still separate
  </h1>
  <p class="lead-dark" style="max-width:56rem">
    A cost-effective pipeline puts each stage on the hardware that fits it. That placement moves the pipeline between a data team's orchestrator and a batch-scheduled cluster, and observability breaks at the most expensive steps.
  </p>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     THE GAP  (warm light · visual, less text)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-10 flex flex-col justify-center gap-10">
  <div class="space-y-3">
    <div class="eyebrow">The Gap</div>
    <h1 class="slide-heading" style="font-size:3.4rem;line-height:1.05;text-wrap:balance">
      Two separate worlds.<br/><span style="color:#0f766e">No shared tooling.</span>
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
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Tooling gap</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">GUI + lockfile vs CLI + drift</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">Dagster ships a UI, lineage, and lockfiles in git. HPC stays CLI-only, with opaque module state on the compute node.</div>
    </div>
    <div class="border-t border-neutral-300 pt-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Observability gap</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">Two toolchains, one pipeline</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">A second <code class="text-xs">sbatch</code> toolchain for the HPC stages fragments lineage where the pipeline consumes the most specialized resources.</div>
    </div>
    <div class="border-t border-neutral-300 pt-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Feedback-loop gap</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">Iteration waits in queue</div>
      <div class="mt-1 text-sm leading-snug text-neutral-600">Local dev needs seconds. Fair-share queues stretch from minutes to days, so people stop testing the HPC path.</div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     DIFFERENT STRENGTHS  (white · 3-col)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Closing The Gap</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.2rem">Each is good at a different job</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      The two are complementary, not redundant. They just lack a bridge between them.
    </p>
  </div>
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr 1fr;align-items:stretch">
    <div class="border border-neutral-300 rounded-lg p-4 bg-white flex flex-col">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Data Orchestrator</div>
      <div class="j-serif mt-1 text-xl text-neutral-950">Plans the work</div>
      <ul class="mt-3 space-y-1.5 text-sm leading-snug text-neutral-600 list-disc pl-4">
        <li>Models data products and refresh policies</li>
        <li>Captures lineage, metadata, and failures</li>
        <li>Keeps engineers productive with local runs and data-quality tests</li>
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
        <li>One view over both, instead of two separate systems</li>
      </ul>
    </div>
  </div>
  <div class="teal-callout" style="padding:0.7rem 1rem">
    <p class="text-sm leading-relaxed text-neutral-700">
      A complementary role to Parsl / executorlib / PSI-J: Slurm becomes <strong>one execution target</strong> for a Dagster asset graph that also touches object stores, databases, local dev, and CI, not a separate workflow boundary.
    </p>
  </div>
</div>

---
layout: dark
---

<!-- ══════════════════════════════════════════════════════
     SECTION 2 · ARCHITECTURE  (divider · dark)
══════════════════════════════════════════════════════ -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-20 flex flex-col justify-center gap-6">
  <div class="eyebrow-light">Part 2 · Architecture</div>
  <h1 class="slide-title text-white" style="font-size:3.8rem;line-height:1.05;text-wrap:balance">
    ComputeResource, Pipes over SSH,<br/>and pixi-pack
  </h1>
  <p class="lead-dark" style="max-width:56rem">
    A precise portability boundary: the same asset source runs on a laptop, in CI, against containerized Slurm, and on a production cluster over SSH. Queues, credentials, paths, and site defaults stay in configuration.
  </p>
</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     ARCHITECTURE  (dark navy · layered building-blocks)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full py-4">
  <div class="max-w-6xl mx-auto px-16 flex flex-col justify-center gap-3 h-full">
    <div class="max-w-4xl space-y-1">
      <div class="eyebrow-light">Architecture</div>
      <h1 class="slide-heading text-white" style="text-wrap:balance;font-size:2.4rem">One graph, pluggable execution.</h1>
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
        <div class="text-xs text-slate-400">what to run; Spark / MPI / Dask / … additive</div>
      </div>
      <div class="rounded-md border border-slate-500/50 bg-slate-800/50 px-4 py-2 flex items-center gap-4">
        <div class="mono-label text-teal-300 w-40">Transport + env</div>
        <div class="j-serif text-lg text-white">dagster-pipes · pixi-pack</div>
        <div class="text-xs text-slate-400">bidirectional events over SSH · relocatable pinned env</div>
      </div>
      <div class="rounded-md border border-teal-400/40 bg-teal-900/20 px-4 py-2 flex items-center gap-4">
        <div class="mono-label text-teal-300 w-40">Target</div>
        <div class="j-serif text-lg text-white">laptop · CI · containerized Slurm · production cluster</div>
      </div>
    </div>
    <div class="dark-callout" style="padding:0.5rem 0.75rem">
      <div class="mono-label text-teal-200">Why layers, not a monolith</div>
      <div class="mt-1 text-sm leading-snug text-white">
        Swap targets without touching asset code: <strong>laptop -> HPC</strong>, or <strong>one HPC site for another</strong> as quotas and GPU availability shift.
      </div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     CODE STAYS THE SAME  (white · magic-move code)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-start gap-3">
  <div class="max-w-4xl space-y-1">
    <div class="eyebrow">Portable Asset Code</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.15rem">Same asset. Same payload. Different target.</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">Only the <code class="text-xs">ComputeResource</code> config decides whether the payload runs locally or lands as a Slurm job. Asset and payload stay identical.</p>
  </div>

````md magic-move {lines: true}
```python
# the asset: identical in dev and on HPC (GPU extraction stage of the case study)
import dagster as dg
from dagster_slurm import BashLauncher, ComputeResource

@dg.asset(partitions_def=country_crawl_partitions)
def cc_family_nuextract_results(context: dg.AssetExecutionContext, compute: ComputeResource):
    completed = compute.run(
        context=context,
        payload_path=_payload("extract_nuextract.py"),
        extra_env={"NUEXTRACT_MODEL_PATH": model_path, "NUEXTRACT_GPU_MEM_UTIL": "0.85"},
        resource_requirements={"gpus": 1, "cpus": 8, "memory_gb": 64, "walltime": "23:00:00"},
    )
    yield from completed.get_results()
```

```python
# the payload: a plain Python script that Pipes instruments
import os
from dagster_pipes import PipesContext, open_dagster_pipes

def main():
    context = PipesContext.get()
    util = float(os.environ["NUEXTRACT_GPU_MEM_UTIL"])   # injected via extra_env
    context.log.info(f"serving NuExtract with vLLM (gpu_mem_util={util})")
    # ... run vLLM inference over the candidate pages ...
    context.report_asset_materialization(
        metadata={"pages": 18_432, "family_flag_rate": 0.036}
    )

if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
```

```python
# dev laptop (no SSH, no Slurm): DAGSTER_DEPLOYMENT=development
from dagster_slurm import ComputeResource, BashLauncher

compute = ComputeResource(
    mode="local",
    default_launcher=BashLauncher(),
)
```

```python
# production HPC: same asset, same payload, different target
from dagster_slurm import (
    ComputeResource, SlurmResource, SSHConnectionResource,
    SlurmQueueConfig, BashLauncher,
)

compute = ComputeResource(
    mode="slurm",
    slurm=SlurmResource(
        ssh=SSHConnectionResource(host="cluster.datalab.tuwien.ac.at", user="scipyuser"),
        queue=SlurmQueueConfig(
            partition="GPU-a100s",          # TU Wien DataLAB GPU partition
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
     PIPES MENTAL MODEL  (white · bidirectional messaging)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-5 flex flex-col justify-start gap-3">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">How the remote job talks back</div>
    <h1 class="slide-heading" style="font-size:2.05rem;line-height:1.05">Dagster Pipes: one protocol, two directions, over SSH</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      <a href="https://docs.dagster.io/guides/build/external-pipelines" class="underline decoration-teal-700/40 underline-offset-[0.14em]">Pipes</a> is the thin wire between the Dagster process and the remote payload. Context and parameters go out; structured logs, progress, and materialization metadata come back during the run, not after.
    </p>
  </div>
  <div class="flex-1 min-h-0 flex items-center justify-center">
    <img src="/img/pipes-architecture.svg" class="max-h-full max-w-full object-contain" alt="Dagster Pipes architecture: asset process sends context to the external process; the external process streams logs, events, and metadata back." />
  </div>
  <div class="grid gap-3" style="grid-template-columns:1fr 1fr 1fr">
    <div class="border-t border-neutral-300 py-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">→ Outbound</div>
      <div class="j-serif mt-0.5 text-base text-neutral-950">Context + params</div>
      <div class="mt-0.5 text-xs leading-snug text-neutral-600">Asset key, run id, partition, <code class="text-xs">extras</code>, and env vars go into the job.</div>
    </div>
    <div class="border-t border-neutral-300 py-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">← Inbound</div>
      <div class="j-serif mt-0.5 text-base text-neutral-950">Events + metrics</div>
      <div class="mt-0.5 text-xs leading-snug text-neutral-600">Log lines, progress, asset checks, <code class="text-xs">report_asset_materialization()</code> with typed metadata.</div>
    </div>
    <div class="border-t border-neutral-300 py-2">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Transport</div>
      <div class="j-serif mt-0.5 text-base text-neutral-950">File or stream</div>
      <div class="mt-0.5 text-xs leading-snug text-neutral-600">SSH pool with ControlMaster on HPC; local pipe in dev. No daemon, no broker to operate.</div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     COMPUTE FLEX  (white · laptop↔cluster visual)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-start gap-3">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Launchers</div>
    <h1 class="slide-heading" style="font-size:2.15rem;line-height:1.05">One asset, pluggable runtimes.</h1>
    <p class="text-sm leading-snug text-neutral-600 max-w-4xl">Scale without rewriting. <strong>Bash on your laptop</strong> for quick iteration, <strong>Ray on one box</strong> for realistic multi-process dev, <strong>Ray on Slurm</strong> when you need multi-node GPUs. New launchers extend a <code class="text-xs">ComputeLauncher</code> base class.</p>
  </div>
  <div class="flex-1 min-h-0 flex items-center justify-center">
    <img src="/img/compute-flex.svg" class="w-full h-full object-contain" alt="Three runtime tiers side by side: Script local (BashLauncher on laptop), Ray local (RayLauncher on one box), and Ray elastic (RayLauncher on Slurm HPC); same asset graph, configuration picks the runtime." />
  </div>
</div>

---
layout: dark
---

<!-- ══════════════════════════════════════════════════════
     SECTION 3 · LIVE DEMO  (divider · dark)
══════════════════════════════════════════════════════ -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-20 flex flex-col justify-center gap-6">
  <div class="eyebrow-light">Part 3 · Live demo</div>
  <h1 class="slide-title text-white" style="font-size:3.8rem;line-height:1.05;text-wrap:balance">
    One asset: laptop → Slurm-in-Docker,<br/>with live log streaming
  </h1>
  <p class="lead-dark" style="max-width:58rem">
    The demo stack is a self-contained Docker Compose cluster that runs on this laptop, with no external connectivity. We run a real case-study asset locally, then flip config to submit it through real <code style="color:rgba(94,234,212,0.9)">sbatch</code> / <code style="color:rgba(94,234,212,0.9)">sacct</code> / <code style="color:rgba(94,234,212,0.9)">squeue</code>.
  </p>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     THE EXAMPLE: FAMILY-OWNED FIRM DISCOVERY  (light · pipeline)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-4">
  <div class="grid w-full gap-10" style="grid-template-columns:1fr 1.05fr;align-items:center">
    <div class="space-y-3">
      <div class="eyebrow">The demo workload</div>
      <h1 class="slide-heading" style="text-wrap:balance;font-size:2rem">Which firms present themselves as family-owned?</h1>
      <p class="text-sm leading-relaxed text-neutral-600 max-w-xl">
        A production pipeline builds a structured corpus of European firms from public web text and matches them to a commercial company registry.
      </p>
      <div class="teal-callout" style="padding:0.7rem 1rem">
        <div class="mono-label text-teal-700">The engineering question this talk evaluates</div>
        <p class="mt-1 text-sm leading-relaxed text-neutral-700">
          How do you run a pipeline whose stages have <strong>very different hardware needs</strong> (a CPU index scan over ~170 M URLs, then GPU language-model extraction) <strong>without maintaining two toolchains</strong>?
        </p>
      </div>
      <p class="text-xs leading-snug text-neutral-500 max-w-xl">
        One <code class="text-xs">(region, crawl)</code> pair = one Slurm job. Full space: 23 country-TLDs × 19 languages, each crossed with 69 crawl snapshots. Partitions are independent and resumable.
      </p>
    </div>
    <div class="rounded-lg overflow-hidden border border-neutral-200 bg-white p-3">
      <img src="/img/pipeline.png" class="w-full h-auto object-contain" alt="Stages of one (region, crawl) run: Common Crawl index → classify → filter → fetch WARC → clean text → cross-crawl dedup → NuExtract-2.0-8B + per-region LoRA via vLLM → consolidate → registry match. Only the extraction stage uses a GPU." />
      <div class="mt-1 text-[0.7rem] leading-snug text-neutral-500 px-1">Only the extraction stage (NuExtract-2.0-8B + per-region LoRA, served with vLLM) uses a GPU. Everything else is CPU / network.</div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     WHY IT NEEDS MULTIPLE TIERS  (white · tiers table)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-4">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Why one allocation won't do</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.05rem">Four stages, four hardware profiles</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      A homogeneous allocation either underprovisions the GPU stages or wastes accelerators on CPU-bound scans. dagster-slurm lets each asset declare its own shape.
    </p>
  </div>
  <div class="overflow-hidden rounded-lg border border-neutral-300">
    <table class="w-full text-sm">
      <thead class="bg-neutral-100 text-neutral-700">
        <tr class="text-left">
          <th class="px-4 py-2 font-semibold">Stage</th>
          <th class="px-4 py-2 font-semibold">Hardware</th>
          <th class="px-4 py-2 font-semibold">Allocation</th>
          <th class="px-4 py-2 font-semibold">Runtime / throughput</th>
        </tr>
      </thead>
      <tbody class="text-neutral-600">
        <tr class="border-t border-neutral-200">
          <td class="px-4 py-2 j-serif text-neutral-950">classify / filter</td>
          <td class="px-4 py-2">CPU</td>
          <td class="px-4 py-2">8-32 cores, 32-128 GB</td>
          <td class="px-4 py-2">DuckDB; ~170 M URLs / crawl</td>
        </tr>
        <tr class="border-t border-neutral-200 bg-neutral-50/60">
          <td class="px-4 py-2 j-serif text-neutral-950">fetch + clean</td>
          <td class="px-4 py-2">CPU + network</td>
          <td class="px-4 py-2">8-96 worker threads</td>
          <td class="px-4 py-2">~1 ms/page clean; 50-95 MB/s S3</td>
        </tr>
        <tr class="border-t border-neutral-200">
          <td class="px-4 py-2 j-serif text-neutral-950">fine-tune</td>
          <td class="px-4 py-2 text-teal-700 font-medium">1 GPU</td>
          <td class="px-4 py-2">16 cores, 128 GB, A100 80 GB</td>
          <td class="px-4 py-2">seq 4096, bf16; 26-37 s/step</td>
        </tr>
        <tr class="border-t border-neutral-200 bg-neutral-50/60">
          <td class="px-4 py-2 j-serif text-neutral-950">LLM inference</td>
          <td class="px-4 py-2 text-teal-700 font-medium">1 GPU</td>
          <td class="px-4 py-2">8 cores, 64 GB, A100 40-80 GB</td>
          <td class="px-4 py-2">vLLM; 15,000-22,000 pages/hour</td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="teal-callout" style="padding:0.65rem 1rem">
    <p class="text-sm leading-relaxed text-neutral-700">
      Cost control lives on the right tier: the registry country filter runs <strong>at the index, on CPU, before any GPU time</strong>. For Spanish and French, that filter keeps a run that would otherwise exceed <strong>1,100 A100-hours</strong> inside a realistic allocation.
    </p>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     THE DEMO: RUN IT YOURSELF  (white · quickstart + what to watch)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-6 flex flex-col justify-center gap-4">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">The key moment</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.05rem">Real Slurm on a laptop, no cluster account</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      The example ships a Dockerised Slurm edge node. The <em>same</em> asset submits through real <code class="text-xs">sbatch</code> / <code class="text-xs">sacct</code> / <code class="text-xs">squeue</code>, locally.
    </p>
  </div>
  <div class="grid gap-4 items-start" style="grid-template-columns:1.05fr 0.95fr">
    <div class="rounded-lg bg-neutral-950 px-5 py-3.5">
      <pre class="font-mono text-[0.82rem] leading-[1.55] text-slate-100 m-0"><span class="text-slate-500"># clone</span>
git clone https://github.com/ascii-supply-networks/dagster-slurm
cd dagster-slurm/examples
<span>&nbsp;</span>
<span class="text-slate-500"># dev on the laptop: Dagster UI on :3000</span>
pixi run start
<span>&nbsp;</span>
<span class="text-slate-500"># flip config → submits to Slurm-in-Docker</span>
pixi run start-staging</pre>
    </div>
    <div class="space-y-0">
      <div class="border-t border-neutral-300 py-2">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Watch 1 · Local</div>
        <div class="j-serif mt-0.5 text-base text-neutral-950">Asset runs in-process</div>
        <div class="mt-0.5 text-xs leading-snug text-neutral-600">Same Python, no SSH, no queue. The developer inner loop.</div>
      </div>
      <div class="border-t border-neutral-300 py-2">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Watch 2 · Flip config</div>
        <div class="j-serif mt-0.5 text-base text-neutral-950">Same asset → real sbatch</div>
        <div class="mt-0.5 text-xs leading-snug text-neutral-600">Only <code class="text-xs">ComputeResource</code> changes; the asset and payload do not.</div>
      </div>
      <div class="border-t border-neutral-300 py-2">
        <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Watch 3 · Live logs</div>
        <div class="j-serif mt-0.5 text-base text-neutral-950">Streaming, during the job</div>
        <div class="mt-0.5 text-xs leading-snug text-neutral-600">Pipes logs and <code class="text-xs">sacct</code> metrics land on the materialization, not after it finishes.</div>
      </div>
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     DEMO FALLBACK 1: RUN VIEW  (white · screenshot)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-4 flex flex-col justify-center gap-3">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">If the live demo fails: run view</div>
    <h1 class="slide-heading" style="font-size:2rem">Slurm metrics + Pipes logs in one timeline</h1>
    <p class="text-sm leading-relaxed text-neutral-600 max-w-4xl">
      Memory peak, CPU efficiency, node-hours, and elapsed time show up on each materialization, whether the job ran on the laptop or on the cluster.
    </p>
  </div>
  <div class="flex-1 rounded-lg overflow-hidden border border-neutral-200 min-h-0">
    <img src="/img/process_data_run_view.png" class="w-full h-full object-contain" alt="Dagster run view with streamed logs and Slurm sacct metrics" />
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     DEMO FALLBACK 2: ASSET VIEW  (white · asset metadata)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-4 flex flex-col justify-start gap-3">
  <div class="flex items-baseline justify-between gap-6">
    <h1 class="slide-heading" style="font-size:1.9rem">Every asset gets its own dashboard</h1>
    <div class="mono-label text-neutral-400">metadata · lineage · source · history</div>
  </div>
  <div class="flex-1 rounded-lg overflow-hidden border border-neutral-200 min-h-0">
    <img src="/img/process_data_asset_view.png" class="w-full h-full object-contain" alt="Dagster asset view with plots, lineage, and source code" />
  </div>
</div>

---
layout: dark
---

<!-- ══════════════════════════════════════════════════════
     SECTION 4 · LESSONS FROM PRODUCTION  (divider · dark)
══════════════════════════════════════════════════════ -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-20 flex flex-col justify-center gap-6">
  <div class="eyebrow-light">Part 4 · Lessons from production</div>
  <h1 class="slide-title text-white" style="font-size:3.8rem;line-height:1.05;text-wrap:balance">
    Environment portability, air-gapped<br/>clusters, site-specific auth
  </h1>
  <p class="lead-dark" style="max-width:56rem">
    In production on the TU Wien DataLAB cluster, with VSC-5 and Leonardo site configs for portability checks. What broke, and what the design got right.
  </p>
</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     PRODUCTION SCALE  (dark · ops profile numbers)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow-light">Operated at scale</div>
    <h1 class="slide-heading text-white" style="text-wrap:balance;font-size:2.2rem">One asset graph, ~10 weeks on DataLAB</h1>
    <p class="text-sm leading-snug text-slate-300 max-w-4xl">
      Numbers read straight from Slurm accounting (<code class="text-xs" style="color:rgba(94,234,212,0.85)">sreport</code> / <code class="text-xs" style="color:rgba(94,234,212,0.85)">sacct</code>), independent of Dagster's own run log, which can drift when a connection drops mid-job.
    </p>
  </div>
  <div class="grid w-full gap-4" style="grid-template-columns:1fr 1fr 1fr 1fr">
    <div class="rounded-lg border border-teal-400/30 bg-teal-900/15 p-4">
      <div class="j-serif text-4xl text-white">9,626</div>
      <div class="mt-1 text-xs leading-snug text-slate-300">Slurm jobs submitted · 79.5% completed</div>
    </div>
    <div class="rounded-lg border border-slate-500/40 bg-slate-800/40 p-4">
      <div class="j-serif text-4xl text-white">2,509</div>
      <div class="mt-1 text-xs leading-snug text-slate-300">GPU-hours consumed</div>
    </div>
    <div class="rounded-lg border border-slate-500/40 bg-slate-800/40 p-4">
      <div class="j-serif text-4xl text-white">118,914</div>
      <div class="mt-1 text-xs leading-snug text-slate-300">CPU core-hours · 5,746 node-hours</div>
    </div>
    <div class="rounded-lg border border-slate-500/40 bg-slate-800/40 p-4">
      <div class="j-serif text-4xl text-white">~16 min</div>
      <div class="mt-1 text-xs leading-snug text-slate-300">CI: build + unit + Slurm-on-Docker, per commit</div>
    </div>
  </div>
  <div class="dark-callout" style="padding:0.6rem 0.9rem">
    <div class="mono-label text-teal-200">Partition space</div>
    <div class="mt-1 text-sm leading-snug text-white">
      1,587 (country, crawl) + 1,311 (language, crawl) Slurm-backed jobs. Cancellations here include <strong>deliberate reruns for stack consistency</strong>, not only errors. The scheduler stays the source of truth.
    </div>
  </div>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     THE THREE LESSONS  (white · 3-col)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">What production taught us</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.1rem">Portability, air-gaps, and auth take most of the effort</h1>
  </div>
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr 1fr;align-items:stretch">
    <div class="border border-neutral-300 rounded-lg p-4 bg-white flex flex-col">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Environment portability</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">pixi-pack or pre-deployed</div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">
        A Pixi lockfile pins every dependency; pixi-pack ships a relocatable bundle. Where a site pre-deploys a shared env, we transfer only the per-run payload script, with no module-system divergence.
      </p>
    </div>
    <div class="border border-neutral-300 rounded-lg p-4 bg-white flex flex-col">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Air-gapped data</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">Move code, not datasets</div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">
        The package transfers payloads and environments but leaves data to the site. A deployment-mode-aware path resolves to a local dir in dev and to the parallel filesystem or object store in production. Asset code stays unchanged.
      </p>
    </div>
    <div class="border border-neutral-300 rounded-lg p-4 bg-white flex flex-col">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Site-specific auth</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">Whatever the site mandates</div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">
        Password, SSH key, or short-lived certs via <a href="https://smallstep.com/docs/step-ca/" class="underline decoration-teal-700/40 underline-offset-[0.14em]">step-ca</a>; ControlMaster fallbacks and login-node hygiene; per-site QoS and reservation overrides. It delegates to SSH and does not bypass MFA.
      </p>
    </div>
  </div>
  <div class="teal-callout" style="padding:0.65rem 1rem">
    <div class="mono-label text-teal-700">Restart semantics</div>
    <p class="mt-1 text-sm leading-relaxed text-neutral-700">
      Idempotent payloads write outputs keyed by input URL and, on restart, skip what is already written. A relaunched partition continues instead of recomputing. A large region shards across GPUs by a hash of the URL, so each worker takes a disjoint slice.
    </p>
  </div>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     RESULTS PAYOFF  (light · family_by_region + eponymy, with caveat)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-4">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">What the orchestration produced</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2rem">One asset graph → a continental firm corpus</h1>
    <p class="text-sm leading-snug text-neutral-600 max-w-4xl">
      5.3 M firms; 51% match a registry company; the family-ownership flag is resolved for 3.3 M, of which <strong>3.6% describe themselves as family-owned</strong>.
    </p>
  </div>
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr;align-items:center">
    <div class="rounded-lg overflow-hidden border border-neutral-200 bg-white p-3">
      <img src="/img/family_by_region.png" class="w-full h-auto object-contain" alt="Share of firms describing themselves as family-owned, by regional grouping." />
      <div class="mt-1 text-[0.7rem] leading-snug text-neutral-500 px-1">Self-described family-owned share, by regional grouping.</div>
    </div>
    <div class="rounded-lg overflow-hidden border border-neutral-200 bg-white p-3">
      <img src="/img/eponymy.png" class="w-full h-auto object-contain" alt="Eponymy rate split by family-ownership self-description: 52% for family firms vs 32% for the rest." />
      <div class="mt-1 text-[0.7rem] leading-snug text-neutral-500 px-1">Eponymy: <strong>52%</strong> for self-described family firms vs <strong>32%</strong> for the rest. An internal consistency check.</div>
    </div>
  </div>
  <div class="border-l-2 border-neutral-400 pl-4">
    <p class="text-xs leading-snug text-neutral-500 max-w-5xl">
      <strong>These are descriptive artifacts, not population prevalence.</strong> A "firm" here is a web domain, not a legal entity; the numbers reflect registry coverage, page availability, extraction quality, and matching rules. The contribution the paper evaluates is the <em>orchestration that produced them with one asset graph rather than two</em>.
    </p>
  </div>
</div>

---
layout: dark
---

<!-- ══════════════════════════════════════════════════════
     SECTION 5 · ROADMAP + CONTRIBUTE  (divider · dark)
══════════════════════════════════════════════════════ -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-20 flex flex-col justify-center gap-6">
  <div class="eyebrow-light">Part 5 · Roadmap & how to contribute</div>
  <h1 class="slide-title text-white" style="font-size:3.8rem;line-height:1.05;text-wrap:balance">
    What's next,<br/>and where to help
  </h1>
  <p class="lead-dark" style="max-width:56rem">
    Two production execution modes and two stable launchers ship today. The interesting frontier is finer-grained scheduling inside an allocation, and more site recipes.
  </p>
</div>

---
layout: white
---

<!-- ──────────────────────────────────────────────────────
     ROADMAP  (white · shipped vs in-progress)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Roadmap</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.1rem">What ships today, and what's next</h1>
  </div>
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr;align-items:stretch">
    <div class="border border-neutral-300 rounded-lg p-5 bg-white">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Shipped</div>
      <ul class="mt-3 space-y-2 text-sm leading-snug text-neutral-700 list-disc pl-4">
        <li><strong>Execution modes:</strong> local, and one Slurm job per asset partition</li>
        <li><strong>Launchers:</strong> Bash and Ray, both production-stable</li>
        <li><strong>Observability:</strong> Pipes logs + <code class="text-xs">sacct</code> metadata on materializations</li>
        <li><strong>CI:</strong> local + Slurm-on-Docker on every commit</li>
      </ul>
    </div>
    <div class="border-2 border-teal-600 rounded-lg p-5 bg-teal-50/50">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">In progress</div>
      <ul class="mt-3 space-y-2 text-sm leading-snug text-neutral-700 list-disc pl-4">
        <li><strong>Spark launcher</strong></li>
        <li><strong>Session-based allocation reuse and heterogeneous jobs:</strong> finer scheduling inside one allocation</li>
        <li><strong>Non-interactive integration</strong> with strict multi-factor environments</li>
        <li>More <strong>site recipes</strong> (queues, QoS, reservations)</li>
      </ul>
    </div>
  </div>
</div>

---
layout: light
---

<!-- ──────────────────────────────────────────────────────
     HOW TO CONTRIBUTE  (light · install + contribute + metaxy)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">How to contribute</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.1rem">Apache-2.0: recipes and launchers welcome</h1>
  </div>
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr 1fr;align-items:stretch">
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Install</div>
      <div class="j-serif mt-1 text-lg text-neutral-950"><code class="text-sm">pip install dagster-slurm</code></div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">or <code class="text-xs">pixi add --pypi dagster-slurm</code>. Try the Slurm-in-Docker example first, no cluster needed.</p>
    </div>
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Contribute</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">Cluster recipes + launchers</div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">Site configs for your cluster, new <code class="text-xs">ComputeLauncher</code>s, docs. Issues and PRs at the repo.</p>
    </div>
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Pairs with</div>
      <div class="j-serif mt-1 text-lg text-neutral-950">Metaxy: skip stale work</div>
      <p class="mt-2 text-sm leading-snug text-neutral-600">dagster-slurm answers <em>where to run</em>; <a href="https://docs.metaxy.io/latest/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Metaxy</a> answers <em>what is stale</em>. Full deep-dive at the poster session.</p>
    </div>
  </div>
  <div class="teal-callout" style="padding:0.75rem 1rem">
    <div class="mono-label text-teal-700">One place to start</div>
    <p class="mt-1 text-sm leading-relaxed text-neutral-700">
      <a href="https://github.com/ascii-supply-networks/dagster-slurm" class="underline decoration-teal-700/40 underline-offset-[0.18em]">github.com/ascii-supply-networks/dagster-slurm</a> · docs at <a href="https://dagster-slurm.geoheil.com/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">dagster-slurm.geoheil.com</a>
    </p>
  </div>
</div>

---
layout: dark
---

<!-- ──────────────────────────────────────────────────────
     WHY THIS MATTERS  (statement · dark)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-16 flex flex-col justify-center items-center gap-8 text-center">
  <div class="eyebrow-light">Why this matters</div>
  <h1 class="slide-title text-white" style="text-wrap:balance;font-size:4rem;line-height:1.05">
    Public HPC already exists.<br/>The hard part is using it.
  </h1>
  <p class="j-serif" style="font-size:1.6rem;line-height:1.35;color:rgba(94,234,212,0.88);text-wrap:balance;max-width:52rem">
    Most research GPUs sit behind Slurm. Make them usable from the same Python workflow you already run on your laptop, wherever your institution hosts them.
  </p>
  <a href="https://github.com/ascii-supply-networks/dagster-slurm" class="mt-4">
    <img src="/img/featured.png" alt="dagster-slurm" class="h-28 w-auto object-contain rounded-xl" style="filter:drop-shadow(0 8px 32px rgba(94,234,212,0.2))" />
  </a>
</div>

---
layout: dark-closing
---

<!-- ──────────────────────────────────────────────────────
     CLOSING  (dark · 3 takeaways)
────────────────────────────────────────────────────── -->
<div class="relative z-10 h-full max-w-6xl mx-auto px-16 py-16 flex flex-col justify-between">
  <div class="eyebrow-light">Three Takeaways</div>
  <div class="space-y-6 border-l pl-8" style="border-color:rgba(255,255,255,0.2)">
    <p class="j-serif text-white" style="font-size:2.8rem;line-height:1.15;text-wrap:balance">
      One asset graph. Laptop to HPC.
    </p>
    <p class="j-serif" style="font-size:2.8rem;line-height:1.15;text-wrap:balance;color:rgba(255,255,255,0.55)">
      It does not replace Slurm or Python.
    </p>
    <p class="j-serif" style="font-size:2.8rem;line-height:1.15;text-wrap:balance;color:rgba(255,255,255,0.25)">
      The HPC you already have (sovereign), usable from Python.
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
     ACKNOWLEDGEMENTS  (light)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-10 flex flex-col justify-center gap-5">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Thanks</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2.4rem">Acknowledgements</h1>
  </div>
  <p class="lead" style="font-size:1rem;line-height:1.65;max-width:60rem">
    We thank the operations team at the <strong>TU Wien DataLAB</strong>, where the case-study pipeline runs in production, and the operations teams at <strong>Austrian Scientific Computing (VSC-5)</strong> and <strong>CINECA's Leonardo</strong> for early feedback, and the <strong>Dagster community</strong> for discussions on orchestrating HPC workloads. Funding and in-kind support were provided by the <a href="https://csh.ac.at/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Complexity Science Hub Vienna</a> and the <a href="https://ascii.ac.at/" class="underline decoration-teal-700/40 underline-offset-[0.18em]">Austrian Supply Chain Intelligence Institute (ASCII)</a>.
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
layout: light
---

<!-- ──────────────────────────────────────────────────────
     APPENDIX: ASSET GRAPH BY TIER  (light · hand-built, not a generated image)
────────────────────────────────────────────────────── -->
<div class="h-full max-w-6xl mx-auto px-16 py-8 flex flex-col justify-center gap-6">
  <div class="max-w-5xl space-y-1">
    <div class="eyebrow">Appendix · the full picture</div>
    <h1 class="slide-heading" style="text-wrap:balance;font-size:2rem">One asset graph, colour-coded by compute tier</h1>
    <p class="text-sm leading-snug text-neutral-600 max-w-4xl">
      Each stage of the family-owned pipeline is a Dagster asset; dagster-slurm maps its declared shape onto the right hardware. Only the extraction stage touches a GPU.
    </p>
  </div>

  <!-- the pipeline flow, tier-coloured -->
  <div class="flex items-stretch gap-1.5 w-full">
    <div class="flex-1 rounded-lg border-2 px-3 py-3" style="border-color:#0369a1;background:#f0f9ff">
      <div class="mono-label" style="color:#0369a1;font-size:0.6rem;letter-spacing:0.1em">CPU</div>
      <div class="j-serif text-neutral-950 text-[0.92rem] leading-tight mt-0.5">classify → page-type</div>
    </div>
    <div class="flex items-center text-neutral-300 text-2xl px-0.5">→</div>
    <div class="flex-1 rounded-lg border-2 px-3 py-3" style="border-color:#0369a1;background:#f0f9ff">
      <div class="mono-label" style="color:#0369a1;font-size:0.6rem;letter-spacing:0.1em">CPU</div>
      <div class="j-serif text-neutral-950 text-[0.92rem] leading-tight mt-0.5">filter · registry domains</div>
    </div>
    <div class="flex items-center text-neutral-300 text-2xl px-0.5">→</div>
    <div class="flex-1 rounded-lg border-2 px-3 py-3" style="border-color:#15803d;background:#f0fdf4">
      <div class="mono-label" style="color:#15803d;font-size:0.6rem;letter-spacing:0.1em">CPU + NET</div>
      <div class="j-serif text-neutral-950 text-[0.92rem] leading-tight mt-0.5">fetch → clean → dedup</div>
    </div>
    <div class="flex items-center text-neutral-300 text-2xl px-0.5">→</div>
    <div class="flex-1 rounded-lg border-2 px-3 py-3" style="border-color:#7e22ce;background:#faf5ff">
      <div class="mono-label" style="color:#7e22ce;font-size:0.6rem;letter-spacing:0.1em">GPU</div>
      <div class="j-serif text-neutral-950 text-[0.92rem] leading-tight mt-0.5">NuExtract-8B + LoRA · vLLM</div>
    </div>
    <div class="flex items-center text-neutral-300 text-2xl px-0.5">→</div>
    <div class="flex-1 rounded-lg border-2 px-3 py-3" style="border-color:#0369a1;background:#f0f9ff">
      <div class="mono-label" style="color:#0369a1;font-size:0.6rem;letter-spacing:0.1em">CPU</div>
      <div class="j-serif text-neutral-950 text-[0.92rem] leading-tight mt-0.5">consolidate → registry match</div>
    </div>
  </div>

  <!-- targets + observability + totals -->
  <div class="grid w-full gap-6" style="grid-template-columns:1fr 1fr 1fr">
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Execution targets</div>
      <div class="j-serif mt-1 text-base text-neutral-950">laptop · CI · DataLAB</div>
      <div class="mt-1 text-xs leading-snug text-neutral-600">Same asset source; <code class="text-xs">DAGSTER_DEPLOYMENT</code> picks the target.</div>
    </div>
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Observability</div>
      <div class="j-serif mt-1 text-base text-neutral-950">Pipes → <code class="text-sm">sacct</code> metadata</div>
      <div class="mt-1 text-xs leading-snug text-neutral-600">Job id, CPU efficiency, memory peak, node-hours land on each materialization.</div>
    </div>
    <div class="border-t border-neutral-300 py-3">
      <div class="text-xs font-semibold uppercase tracking-[0.22em] text-teal-700">Measured on DataLAB</div>
      <div class="j-serif mt-1 text-base text-neutral-950">9,626 jobs · 2,509 GPU-h</div>
      <div class="mt-1 text-xs leading-snug text-neutral-600">~10-week accounting window, straight from <code class="text-xs">sreport</code>.</div>
    </div>
  </div>
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
  