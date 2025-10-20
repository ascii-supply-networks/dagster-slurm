---
sidebar_position: 3
title: Troubleshooting connectivity & logs
---

The `dagster-slurm` presets hide most SSH and Slurm plumbing, but a few deployment-specific quirks are worth calling out. The tips below keep log streaming responsive, explain how to retain run artefacts for inspection, and silence noisy shells in the Docker sandbox.

## Log streaming without ControlMaster

Some HPC centres (e.g. VSC-5) disable SSH ControlMaster sockets. When that happens the Dagster code location automatically falls back to password-aware connections and polls the remote log files. You still get live output, but a few tweaks make the experience smoother:

- Set `DAGSTER_SLURM_SSH_CONTROL_DIR` if your site restricts socket paths. The default is `~/.ssh/dagster-slurm`; point it to a writable directory on shared machines.
- Leave `SLURM_EDGE_NODE_FORCE_TTY=false` unless your cluster explicitly requires a TTY. Password-based sessions request one automatically.
- If you rely on jump hosts or OTP prompts, prefer a local `~/.ssh/config` entry. The same configuration is reused by the Pipes log tailer.

When the connection falls back to polling, Dagster prints a debug line such as:

```
Streaming ... via polling fallback (ControlMaster unavailable)
```

This is expected and no changes are required unless you notice missing output.

## Keep or delete remote run directories

Every successful run now schedules an asynchronous cleanup on the edge node:

```
nohup rm -rf /path/to/dagster_runs/runs/<run_id> >/dev/null 2>&1 &
```

That keeps storage quotas tidy without delaying Dagster shutdown. You can still opt out temporarily:

- Set `debug_mode=True` on the relevant `ComputeResource` to keep run folders around for inspection.
- Manually remove old folders with a periodic cron job if you prefer a deterministic retention policy:

```bash
find /home/$USER/dagster_runs/runs -mindepth 1 -maxdepth 1 -mtime +3 -exec rm -rf {} +
```

The `cleanup_on_failure` flag continues to apply—failed runs respect `debug_mode` and skip removal.
