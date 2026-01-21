---
sidebar_position: 99
title: HPC - VSC-5
---

## Sample configuration: Austrian Scientific Computing (ASC) VSC-5

```dotenv title=".env.vsc5"
# SSH / cluster login node access
SLURM_EDGE_NODE_HOST=vsc5.vsc.ac.at
SLURM_EDGE_NODE_PORT=27                        # use port 22 for password-based auth and port 27 for key-based auth on VSC5
SLURM_EDGE_NODE_USER=your_vsc_username
# SLURM_EDGE_NODE_PASSWORD=password            # specify either password or ssh key path
SLURM_EDGE_NODE_KEY_PATH=/Users/you/.ssh/id_ed25519_vsc5
SLURM_EDGE_NODE_JUMP_HOST=vmos.vsc.ac.at       # optional, may be necessary if outside of university network / VPN
SLURM_EDGE_NODE_JUMP_USER=your_vsc_username
SLURM_EDGE_NODE_JUMP_PASSWORD=...              # required: VMOS bastion only accepts password auth

# Deployment settings
SLURM_DEPLOYMENT_BASE_PATH=/home/your_vsc_username/dagster-slurm
SLURM_PARTITION=zen3_0512                      # default partition (without GPUs)
SLURM_QOS=zen3_0512                            # matching QOS for default partition
# SLURM_QOS=zen3_0512_devel                    # alternative: Development queue with higher priority (max. 5 nodes, max. 10 minutes)
# SLURM_PARTITION=zen3_0512_a100x2             # GPU partition with 2xA100
# SLURM_QOS=zen3_0512_a100x2                   # matching QOS for A100 partition
# SLURM_QOS=zen3_0512_a100x2_devel             # alternative: Development queue with higher priority (max. 2 nodes, max. 10 minutes)
# SLURM_RESERVATION=reservation_name           # optional reservation (if active)
SLURM_SUPERCOMPUTER_SITE=vsc5

# Dagster deployment selector
DAGSTER_DEPLOYMENT=production_supercomputer
```

VSC-5 prefers key-based authentication; ensure your SSH config allows agent forwarding or provide the key path above. Replace the queue/QoS/reservation values with the combinations granted to your project (for example `batch`, `short`, or a project-specific reservation).\
VMOS currently rejects key-only authentication, so always provide `SLURM_EDGE_NODE_JUMP_PASSWORD` (and be ready to type the time-based OTP on the first prompt). The final hop to `vsc5.vsc.ac.at` can still use your SSH key. If your policies require password-only access end-to-end, set `SLURM_EDGE_NODE_PASSWORD`; Dagster prints `Enter … for vsc5.vsc.ac.at:` on your terminal—type the OTP there to continue. Password-based sessions automatically request a pseudo-TTY, so you only need `SLURM_EDGE_NODE_FORCE_TTY=true` if your site mandates it even for key-based authentication.

> **Cleanup behaviour:** Completed runs trigger an asynchronous `rm -rf` of the run directory on the edge node. This keeps quotas tidy without delaying Dagster shutdown. Set `debug_mode=True` on the relevant `ComputeResource` while debugging to keep run folders around for manual inspection.

## SSH on VSC-5

To keep the VSC-5 login chain responsive, configure SSH keepalives for the `vmos.vsc.ac.at` bastion and `vsc5.vsc.ac.at` login node:

```sshconfig title="~/.ssh/config"
Host vmos.vsc.ac.at vsc5.vsc.ac.at
    ServerAliveInterval 120
    ServerAliveCountMax 2
```

Replace the usernames with your account. These settings dramatically reduce dropped tunnels and `ssh_exchange_identification` failures when submitting many jobs through `dagster-slurm`.

For additional information about VSC-5, see the [ASC documentation](https://docs.asc.ac.at/).
