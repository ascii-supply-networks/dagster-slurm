---
sidebar_position: 99
title: HPC - MUSICA
---

MUSICA requires SSH certificate authentication via the Step client. Standard password or key-based authentication is not supported.

## Authentication

Generate a time-limited SSH certificate:

```bash
step ssh certificate 'your.email@example.com' ~/.ssh/id_musica --no-password --insecure --force
```

After running this command, authenticate in your browser. The certificate is valid for a limited time.

### Automatic Certificate Refresh (Recommended)

Enable automatic OIDC-based certificate refresh by setting `SLURM_SUPERCOMPUTER_SITE=musica` and adding these variables:

```dotenv
# OIDC credentials (app password valid for 30 days)
ASC_OIDC_CLIENT_ID=...
ASC_OIDC_USERNAME=xy12345
ASC_OIDC_APP_PASSWORD=...
ASC_OIDC_TOKEN_URL=https://auth.asc.ac.at/application/o/token/

# Step CA configuration
ASC_STEP_CA_URL=https://auth.asc.ac.at:9000
ASC_STEP_FINGERPRINT=44b048473242281db1da57124c2b843741d6c92a8fb5d0482dec032e957f2919
```

The refresh hook automatically fetches new certificates before SSH connections.

**Verify the flow:**

```bash
pixi run -e dev python scripts/refresh_step_auth.py
```

**Rotate app password** (requires API token with `goauthentik.io/api` scope):

```bash
pixi run -e dev python scripts/refresh_step_auth.py --rotate-app-password
```

Required for rotation:

```dotenv
ASC_AUTHENTIK_API_BASE=https://auth.asc.ac.at
ASC_AUTHENTIK_API_TOKEN=...
ASC_AUTHENTIK_TOKEN_IDENTIFIER=dagster-slurm
```

## Configuration

```dotenv title=".env.musica"
# Cluster access
SLURM_EDGE_NODE_HOST=musica.vie.asc.ac.at
SLURM_EDGE_NODE_USER=xy12345
SLURM_EDGE_NODE_KEY_PATH=~/.ssh/id_musica

# SLURM settings
SLURM_DEPLOYMENT_BASE_PATH=/home/xy12345/dagster-slurm
SLURM_PARTITION=zen4_0768_h100x4
SLURM_QOS=zen4_0768_h100x4
SLURM_SUPERCOMPUTER_SITE=musica

# Deployment mode
DAGSTER_DEPLOYMENT=production_supercomputer
```

**Validation:** Run `sinfo` on the login node to verify the partition name before launching jobs.

## Ray Launcher Configuration

MUSICA requires increased file descriptor limits for Ray. The site override automatically applies `ulimit -n 65536` before starting Ray.

**Implementation:**

```python title="resources/__init__.py" {5}
SUPERCOMPUTER_SITE_OVERRIDES = {
    "musica": {
        "launchers": {
            "ray": {
                "pre_start_commands": ["ulimit -n 65536"],
            }
        },
    }
}
```

[View full configuration →](https://github.com/asc-repos/dagster-slurm/blob/main/examples/projects/dagster-slurm-example/dagster_slurm_example/resources/__init__.py#L165-L184)

If the Ray dashboard fails to start, add `ray_start_args: ["--include-dashboard=false"]` to the launcher config.

## Resources

- [MUSICA Documentation](https://docs.asc.ac.at/musica_test/)
- [Step CLI Reference](https://smallstep.com/docs/step-cli/)
