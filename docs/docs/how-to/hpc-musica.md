---
sidebar_position: 99
title: HPC - MUSICA
---

Login on MUSICA is either via a link / qr-code on each login (not feasible for dagster-slurm) or with the help of the step client.
To obtain a time-limited ssh certificate that can be used with dagster-slurm, use the step client. Execute the following command and log in in your web browser:

```bash
step ssh certificate 'your.email.address@example.com' ~/.ssh/id_musica --no-password --insecure --force
```

## Optional: automated Step/OIDC refresh

The example repo supports an automated refresh flow that fetches an OIDC token and runs
`step ssh login` before each SSH connection when `SLURM_SUPERCOMPUTER_SITE=musica`.
Set the following variables (in addition to the SSH settings below):

```dotenv
# OIDC client credentials (app password is valid for 30 days)
ASC_OIDC_CLIENT_ID=...
ASC_OIDC_USERNAME=xy12345
ASC_OIDC_APP_PASSWORD=...
ASC_OIDC_TOKEN_URL=https://auth.asc.ac.at/application/o/token/
ASC_OIDC_SCOPE=profile

# Step settings
ASC_STEP_CONTEXT=asc
ASC_STEP_CA_URL=https://auth.asc.ac.at:9000
ASC_STEP_FINGERPRINT=44b048473242281db1da57124c2b843741d6c92a8fb5d0482dec032e957f2919
ASC_STEP_CERT_PATH=~/.step/ssh/certs/asc.crt
ASC_STEP_REFRESH_SKEW=30
```

You can verify the refresh flow locally with:

```bash
cd examples
pixi exec -e dev -- python scripts/refresh_step_auth.py
```

To rotate the app password via the authentik API (requires a separate API token with `goauthentik.io/api` scope):

```bash
cd examples
pixi exec -e dev -- python scripts/refresh_step_auth.py --rotate-app-password
```

Required API env vars for rotation:

```dotenv
ASC_AUTHENTIK_API_BASE=https://auth.asc.ac.at
ASC_AUTHENTIK_API_TOKEN=...  # API token with goauthentik.io/api scope
ASC_AUTHENTIK_TOKEN_IDENTIFIER=dagster-slurm
ASC_AUTHENTIK_TOKEN_INTENT=app_password
ASC_AUTHENTIK_TOKEN_EXPIRES_DAYS=30
ASC_AUTHENTIK_APP_PASSWORD_OUTPUT=~/.secrets/asc_app_password.txt
```

## Sample configuration: MUSICA

```dotenv title=".env.musica"
# SSH / cluster login node access
SLURM_EDGE_NODE_HOST=musica.vie.asc.ac.at
SLURM_EDGE_NODE_PORT=22
SLURM_EDGE_NODE_USER=xy12345                  # replace with your MUSICA username
SLURM_EDGE_NODE_KEY_PATH=/Users/you/.ssh/id_musica  # Log in with the step client first to get a time-limited certificate

# Deployment settings
SLURM_DEPLOYMENT_BASE_PATH=/home/your_user/dagster-slurm
SLURM_PARTITION=zen4_0768_h100x4
SLURM_QOS=zen4_0768_h100x4
# SLURM_RESERVATION=...                       # optional reservation if provided by admins
SLURM_SUPERCOMPUTER_SITE=musica

# Dagster deployment selector
DAGSTER_DEPLOYMENT=production_supercomputer # or staging_supercomputer
```

With the variables defined, restart your Dagster code location. For a quick validation, run `sinfo` on the login node to confirm the partition name and `srun` with the parameters above to verify access before launching Dagster jobs.

## Ray launcher note

Ray requires a higher file-descriptor limit on MUSICA. Configure the Ray launcher with `pre_start_commands` so it runs `ulimit -n 65536` before `ray start`. If the Ray dashboard fails to start on MUSICA, add `ray_start_args: ["--include-dashboard=false"]`. In the example repo, this lives under `SUPERCOMPUTER_SITE_OVERRIDES["musica"]` in `examples/projects/dagster-slurm-example/dagster_slurm_example/resources/__init__.py`.

For additional information about MUSICA, see the [MUSICA](https://docs.asc.ac.at/musica_test/).
