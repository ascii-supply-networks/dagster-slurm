---
sidebar_position: 99
title: HPC - VSC-5
---

## Sample configuration: Austrian Scientific Computing (ASC) (VSC-5)

```dotenv title=".env.vsc5"
# SSH / edge node access
SLURM_EDGE_NODE_HOST=vsc5.vsc.ac.at
SLURM_EDGE_NODE_PORT=22
SLURM_EDGE_NODE_USER=your_vsc_username
SLURM_EDGE_NODE_KEY_PATH=/Users/you/.ssh/id_ed25519_vsc5
SLURM_EDGE_NODE_JUMP_HOST=vmos.vsc.ac.at        # optional, but recommended
SLURM_EDGE_NODE_JUMP_USER=your_vsc_username
SLURM_EDGE_NODE_JUMP_PASSWORD=...              # required: VMOS bastion only accepts password/OTP auth

# Deployment settings
SLURM_DEPLOYMENT_BASE_PATH=/home/your_vsc_username/dagster-slurm
SLURM_PARTITION=zen3_0512                      # pick a queue you can access
SLURM_QOS=zen3_0512_devel                      # optional QoS/account string
SLURM_RESERVATION=dagster-slurm_21             # optional reservation (if active)
SLURM_SUPERCOMPUTER_SITE=vsc5

# Dagster deployment selector
DAGSTER_DEPLOYMENT=production_supercomputer
```

VSC-5 prefers key-based authentication; ensure your SSH config allows agent forwarding or provide the key path above. Replace the queue/QoS/reservation values with the combinations granted to your project (for example `batch`, `short`, or a project-specific reservation).  
VMOS currently rejects key-only authentication, so always provide `SLURM_EDGE_NODE_JUMP_PASSWORD` (and be ready to type the time-based OTP on the first prompt). The final hop to `vsc5.vsc.ac.at` can still use your SSH key. If your policies require password-only access end-to-end, set `SLURM_EDGE_NODE_PASSWORD`; Dagster prints `Enter … for vsc5.vsc.ac.at:` on your terminal—type the OTP there to continue. Password-based sessions automatically request a pseudo-TTY, so you only need `SLURM_EDGE_NODE_FORCE_TTY=true` if your site mandates it even for key-based authentication.

> **Cleanup behaviour:** Completed runs trigger an asynchronous `rm -rf` of the run directory on the edge node. This keeps quotas tidy without delaying Dagster shutdown. Set `debug_mode=True` on the relevant `ComputeResource` while debugging to keep run folders around for manual inspection.


## SSH on VSC-5
To keep the VSC-5 login chain responsive, configure SSH keepalives for the `vmos` bastion and `vsc5` login node:

```sshconfig title="~/.ssh/config"
Host vmos
    HostName vmos.vsc.ac.at
    User dagster01
    ServerAliveInterval 120
    ServerAliveCountMax 2

Host vsc5
    HostName vsc5.vsc.ac.at
    User dagster01
    ProxyJump vmos
    ServerAliveInterval 120
    ServerAliveCountMax 2
```

Replace the usernames with your account. These settings dramatically reduce dropped ControlMaster tunnels and `ssh_exchange_identification` failures when submitting many jobs through `dagster-slurm`.
