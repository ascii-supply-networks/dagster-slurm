---
sidebar_position: 99
title: HPC - MUSICA
---

MUSICA requires SSH certificate authentication via the Step client. Standard password or key-based authentication is not supported.

:::info Official Documentation
For comprehensive MUSICA cluster documentation, see the [official MUSICA documentation](https://docs.asc.ac.at/musica_test/index.html).
:::

## Step client authentication

Generate a time-limited SSH certificate:

```bash
step ssh certificate 'your.email@example.com' ~/.ssh/id_musica --no-password --insecure --force
```

After running this command, authenticate in your browser. The certificate is valid for a limited time.

### Bootstrap Requirements

The MUSICA integration needs at least one working bootstrap path before the
first SSH connection. A run can start if any one of these is true:

- you already have a valid local Step SSH certificate
- your `ASC_AUTHENTIK_CREDENTIALS_FILE` already exists and contains current credentials
- you provide a fresh bootstrap `ASC_AUTHENTIK_API_TOKEN` so the credentials file can be created or recovered
- you provide `ASC_OIDC_APP_PASSWORD` directly

Important:

- `ASC_AUTHENTIK_CREDENTIALS_FILE=/some/path.env` by itself is not enough; the file must already be populated
- if neither a valid certificate nor one of the refresh/bootstrap paths above is available, `dagster-slurm` now fails fast with a clear MUSICA bootstrap error before attempting SSH

### Automatic Certificate Refresh (Recommended)

Enable automatic OIDC-based certificate refresh by setting `SLURM_SUPERCOMPUTER_SITE=musica` and adding these variables:

```dotenv
# OIDC identity
ASC_OIDC_CLIENT_ID=...
ASC_OIDC_USERNAME=xy12345
ASC_OIDC_TOKEN_URL=https://auth.asc.ac.at/application/o/token/

# Runtime auto-rotation
ASC_AUTHENTIK_API_BASE=https://auth.asc.ac.at
ASC_AUTHENTIK_CREDENTIALS_FILE=~/.config/dagster-slurm/musica-auth.env
ASC_AUTHENTIK_API_TOKEN=...  # bootstrap / recovery only

# Step CA configuration
ASC_STEP_CA_URL=https://auth.asc.ac.at:9000
ASC_STEP_FINGERPRINT=44b048473242281db1da57124c2b843741d6c92a8fb5d0482dec032e957f2919
```

The refresh hook automatically fetches new certificates before SSH connections.
The runtime reads the current Authentik credentials from
`ASC_AUTHENTIK_CREDENTIALS_FILE`. If the file is missing, incomplete, or within
the refresh window, it creates fresh replacement credentials, writes them
atomically to that file, and continues automatically.

`ASC_AUTHENTIK_API_TOKEN` is only needed to bootstrap the credentials file for
the first time or to recover after all stored credentials have expired.

**Bootstrap or rotate the credentials file only:**

```bash
rm -f "$ASC_AUTHENTIK_CREDENTIALS_FILE"
pixi run -e dev python scripts/refresh_step_auth.py --rotate-credentials
```

Expected result:

- the credentials file is created with mode `0600`
- it now contains the current rotated credentials for future runs
- the script stops after rotating credentials; it does not talk to Step yet

**Rotate credentials and refresh the Step SSH certificate in one run:**

```bash
pixi run -e dev python scripts/refresh_step_auth.py --rotate-credentials --refresh-step-after-rotate
```

**Verify runtime refresh using the stored credentials only:**

```bash
unset ASC_AUTHENTIK_API_TOKEN
pixi run -e dev python scripts/refresh_step_auth.py
```

As long as Dagster or the refresh script runs before the stored credentials
expire, both the app password and the API token can keep rolling forward from
that file without another manual bootstrap token.

**Force a manual rotation of both stored credentials:**

```bash
pixi run -e dev python scripts/refresh_step_auth.py --rotate-credentials
```

Required for rotation:

```dotenv
ASC_AUTHENTIK_API_BASE=https://auth.asc.ac.at
ASC_AUTHENTIK_CREDENTIALS_FILE=~/.config/dagster-slurm/musica-auth.env
ASC_AUTHENTIK_API_TOKEN=...
ASC_AUTHENTIK_TOKEN_IDENTIFIER=dagster-slurm
```

:::warning Token vs App Password
`ASC_AUTHENTIK_API_TOKEN` must be an authentik **Token**, not an App Password.
In the [authentik user settings](https://auth.asc.ac.at/if/user/#/settings;%7B%22page%22%3A%22page-tokens%22%7D),
use **"New Token"** — not "New App Password".

- A **Token** grants API access and can create new tokens or app passwords.
- An **App Password** is only usable with applications (like the step client) and cannot call the authentik API.

Both expire after roughly one month. The file-backed flow above keeps rotating
both secrets forward, but if the stored API token is already expired and no
fresh bootstrap token is available, recovery still requires creating a new
**Token** manually in authentik.
:::

For short MUSICA test jobs, you can also use the devel QoS:

```dotenv
SLURM_PARTITION=zen4_0768
SLURM_QOS=dev_zen4_0768
```

or for GPU testing:

```dotenv
SLURM_PARTITION=zen4_0768_h100x4
SLURM_QOS=dev_zen4_0768_h100x4
```

These `dev_*` QoS values are limited to 10 minutes and intended for fast
feedback, not production runs.

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

## Background

MUSICA's authentication flow combines OAuth2 machine-to-machine credentials with SSH certificate issuance:

- **OAuth2 Machine-to-Machine Flow**: The automatic refresh uses app passwords (OAuth2 client credentials) to obtain access tokens. See [Authentik M2M Provider Documentation](https://next.goauthentik.io/add-secure-apps/providers/oauth2/machine_to_machine/) for the authentication flow details.
- **SSH Certificate Issuance**: Step CA issues time-limited SSH certificates using the OAuth2 access token. See [Step CA SSH Certificate Operations](https://smallstep.com/docs/step-ca/basic-certificate-authority-operations/#2-issue-an-ssh-user-certificate-and-test-your-connection) for how SSH certificate authentication works.
