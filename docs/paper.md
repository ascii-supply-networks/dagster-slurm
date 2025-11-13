---
title: 'Discovering the SUPER in computing - dagster-slurm for reproducible research on HPC'
tags:
  - Python
  - dagster
  - slurm
  - reproducible-research
  - RSE
  - secops
  - sops
  - age
  - pixi
  - pixi-pack
  - conda
authors:
  - name: Hernan Picatto
    orcid: 0000-0001-6662-464X
    affiliation: "2"
  - name: Maximilian Heß
    orcid: 0009-0007-8093-7117
    affiliation: "2"
  - name: Georg Heiler
    orcid: 0000-0002-8684-1163
    affiliation: "1, 2"
affiliations:
 - name: Complexity Science Hub Vienna (CSH)
   index: 1
 - name: Austrian Supply Chain Intelligence Institute (ASCII)
   index: 2

date: 1st November 2025
bibliography: paper.bib

# Optional fields if submitting to a AAS journal too, see this blog post:
# <https://blog.joss.theoj.org/2018/12/a-new-collaboration-with-aas-publishing
aas-doi: 10.3847/xxxxx
aas-journal: Journal of Open Source Software
---

# Summary

Dagster is a modern data orchestrator that emphasises reproducibility, observability, and a strong developer experience @dagster.
In parallel, most high-performance computing (HPC) centres continue to rely on Slurm for batch scheduling and resource governance @yoo2003slurm.
The two ecosystems rarely meet in practice: Dagster projects often target cloud or single-node deployments, while Slurm users maintain bespoke submission scripts with limited reuse or visibility.
This paper introduces **dagster-slurm**, an open-source integration that allows the same Dagster assets to run unchanged across laptops, CI pipelines, containerised Slurm clusters, and Tier-0 supercomputers.
The project packages dependencies with Pixi @pixi, submits workloads through Slurm using Dagster Pipes @dagsterpipes, and streams logs plus scheduler metrics back to the Dagster UI.

The key contribution is a unified compute resource (`ComputeResource`) that hides SSH transport (including password-only jump hosts and OTP prompts), dependency packaging, and queue configuration while still respecting Slurm’s scheduling semantics.
Today the project ships with two production-ready execution modes—`local` for laptop/CI development and `slurm` for one-job-per-asset submissions.
Experimental support for session-based reuse and heterogeneous jobs is work in progress. Launchers for Bash, Ray, and Spark (work in progress) ship out of the box and can be extended.

# Statement of Need

Research software engineers (RSE) and data scientists increasingly face cross-environment workflows.
The 22nd edition of the Research Software Engineering (RSE) International Survey reports that every participating country now marks HPC as an important RSE skill @hettrick_2022_7015772.
Reproducibility challenges in HPC environments are well documented, and Antunes et al. @ANTUNES2024100655 provide comprehensive coverage of issues unique to HPC in their survey.
Courtes et al. @Courtes9882991 further examine the tension between reproducibility and performance, demonstrating that these goals need not be mutually exclusive.

The 2024 Community Workshop on Practical Reproducibility in HPC produced a comprehensive report highlighting cost-effective reproducibility challenges and the need for tools that bridge development and production environments @keahey_2025_15306610.
Existing solutions either ignore the orchestrator (hand-written Slurm scripts) or bypass the scheduler entirely (running ad-hoc services).
This leads to duplicated logic, fragile deployments, and a loss of telemetry. **dagster-slurm** was created to:

- Preserve Dagster’s asset-based design, lineage tracking, and alerting for workloads that ultimately run on Slurm-managed hardware.
- Remove the need to rewrite orchestration glue when moving from development to production supercomputers.
- Provide a batteries-included path for packaging Python environments reproducibly (Pixi https://pixi.sh/latest/ + pixi-pack, https://github.com/Quantco/pixi-pack) and deploying them in air-gapped environments.
- Offer a clear path to advanced HPC patterns (session reuse for long-lived clusters, heterogeneous Slurm jobs) while keeping the current stable surface area intentionally small; these features are being iterated on in the open.
- Encourage research software engineering best practices as outlined by Eisty et al. @eisty2025, covering planning, testing, documentation, and maintenance across the development lifecycle.

# System Overview

The integration is composed of three layers:

1. **Resource definitions** – `ComputeResource`, `SlurmResource`, `SlurmSessionResource`, and `SSHConnectionResource` are Dagster `ConfigurableResource` objects. They encapsulate queue defaults, SSH authentication (including ControlMaster fallback, password-based jump hosts, and interactive OTP prompts), and execution modes.
2. **Launchers and Pipes clients** – Launchers (Bash, Ray) translate payloads into execution plans. 
The Slurm Pipes client handles environment packaging (on demand or via pre-deployed bundles), transfers scripts, triggers `sbatch` or session jobs, and streams logs/metrics back through Dagster Pipes @dagsterpipes. Spark and custom launchers are planned for near future release
3. **Operational helpers** – Environment deployment scripts, heterogeneous job managers, metrics collectors, and SSH pooling utilities target HPC constraints such as login-node sandboxes, session allocations, and queue observability.

This layered approach keeps Dagster’s user code agnostic to the underlying transport while retaining the full control plane visibility of the orchestrator.
As illustrated in Figure \ref{fig:architecture}, the same scalable job can follow multiple paths:
direct local execution for development, automated testing through CI/CD chains, or production deployment to HPC clusters via SSH-accessible edge nodes.

![Architecture overview showing the progression from local development to production HPC deployment. The workflow enables researchers to code locally, test through CI/CD pipelines, and deploy to Slurm-enabled clusters with minimal changes.\label{fig:architecture}](../slides/public/img/arch-overview.png)

# Minimal usage example

```python
import dagster as dg
from dagster_slurm import (
    ComputeResource,
    ExecutionMode,
    RayLauncher,
    SlurmQueueConfig,
    SlurmResource,
    SSHConnectionResource,
)

ssh = SSHConnectionResource.from_env(prefix="SLURM_EDGE_NODE")
slurm = SlurmResource(
    ssh=ssh,
    queue=SlurmQueueConfig(partition="batch",
      time_limit="02:00:00", cpus=8, mem="32G"),
    remote_base=f"/home/{ssh.user}/dagster_runs",
)

compute = ComputeResource(
    mode=ExecutionMode.SLURM,
    slurm=slurm,
    default_launcher=RayLauncher(num_gpus_per_node=1),
)

@dg.asset(required_resource_keys={"compute"})
def train_model(context: dg.AssetExecutionContext):
    payload = dg.file_relative_path(__file__, "../workloads/train.py")
    completed = context.resources.compute.run(
        context=context,
        payload_path=payload,
        resource_requirements={"framework": "ray", 
          "cpus": 32, "gpus": 1, "memory_gb": 120},
        extra_env={"EXPERIMENT": context.run.run_id},
    )
    yield from completed.get_results()
```

Local development simply swaps `ExecutionMode.SLURM` for `ExecutionMode.LOCAL`.
The example project bundled with the repository demonstrates this workflow, complete with Dockerised Slurm nodes for integration testing.
Session reuse and heterogeneous jobs remain under active development; early adopters can track progress in the repository milestones.

# Evaluation

We validate the approach along three dimensions:

- **Reproducibility** – Integration tests run inside GitHub Actions using a containerised Slurm cluster.
The pipeline provisions the environment with Pixi, deploys it once via `pixi run deploy-prod-docker`, and then runs Dagster assets through all four execution modes.
This continuous integration approach aligns with emerging best practices for reproducible HPC workflows @hayotsasson2025address.
- **HPC readiness** – The project has been exercised on academic clusters such as VSC-5 (Austria) and Leonardo (Italy).
SSH ControlMaster fallbacks, password-based jump hosts, `.bashrc` hygiene, queue/QoS/reservation overrides, and verification snippets (`squeue`, `scontrol`) are documented for both sites.
- **Observability** – Slurm job IDs, CPU efficiency, memory, and node-hours are exposed as Dagster metadata entries, while Ray clusters stream their stdout/stderr back through Pipes.
This enables conventional Dagster asset checks and alerting to operate unchanged.

# Impact and Future Work

dagster-slurm lowers the barrier for research teams to adopt modern data orchestration on top of established HPC schedulers.
By eliminating duplicated scripts and surfacing rich observability, the integration reduces operational toil and shortens iteration loops.
Future work focuses on:

- Deepening heterogeneous job support (automatic fusion of dependent assets, richer allocation policies).
- Exploring pilot-job back ends (e.g., RADICAL-Pilot, QCG) for even finer-grained scheduling inside allocations, and non-interactive OTP integrations for environments with strict MFA policies.
- Extending language/runtime coverage via additional launchers (MPI, GPU-accelerated frameworks) and multi-language Pipes integrations.

Community contributions—issue reports, cluster-specific recipes, and new launchers—are actively encouraged at <https://github.com/ascii-supply-networks/dagster-slurm>.

# Acknowledgements

We thank the operations teams at the Austrian Scientific Computing (ASC) (VSC-5) and CINECA’s Leonardo supercomputer for early feedback, and the Dagster community for discussions around orchestrating HPC workloads.
Funding and in-kind support were provided by the Complexity Science Hub Vienna and the Austrian Supply Chain Intelligence Institute.
This work was completed in part at the EUROCC AI HACKATHON 2025, part of the Open Hackathons program.
The authors would like to acknowledge OpenACC-Standard.org for their support.

# References
