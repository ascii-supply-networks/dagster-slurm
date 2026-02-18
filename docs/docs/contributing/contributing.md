---
sidebar_position: 2
title: Contributing
---

## Contributing to dagster-slurm

We welcome contributions of all kinds—bug fixes, documentation improvements, new launchers, and cluster-specific recipes.

**How to get started:**

1. Browse open issues, especially those labelled [`good first issue`](https://github.com/ascii-supply-networks/dagster-slurm/issues?q=label%3A%22good+first+issue%22).
2. Check the [project board](https://github.com/orgs/ascii-supply-networks/projects/4) for planned work.
3. Fork the repository and create a feature branch.
4. Follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `chore:`).
5. Run formatting and linting before pushing:
   ```bash
   pixi run -e build --frozen fmt
   pixi run -e build --frozen lint
   ```
6. Open a pull request against `main`.

**Coding conventions:** Python 3.12, 4-space indentation, type hints on public APIs.

## Reporting Issues

Found a bug or have a feature request? [Open an issue](https://github.com/ascii-supply-networks/dagster-slurm/issues/new).

To help us resolve issues quickly, please include:

- **dagster-slurm version**
- **Python version and OS**
- **Slurm version** (if applicable, run `sinfo --version`)
- A **minimal reproducible example** (ideally as a full git repository including the pixi-based dependencies with a lockfile and ideally reproduciblle with the docker-based local slurm cluster)
- Full **traceback or error output**
- **Expected vs. actual behaviour**

## Seeking Support

- **GitHub Discussions** — For questions, how-to requests, and general feedback, use [GitHub Discussions](https://github.com/ascii-supply-networks/dagster-slurm/discussions). This is the preferred channel for conversations that are not confirmed bugs.
- **GitHub Issues** — For confirmed bugs and feature requests.
- **Dagster Community Slack** — For questions that span the broader Dagster ecosystem, join the [Dagster Slack](https://dagster.io/community).

## Development Setup

You have to have uv installed for the library development - but pixi for executing the examples.

### Prerequisites

- **pixi installation**: https://pixi.sh/latest/installation/
  ```bash
  curl -fsSL https://pixi.sh/install.sh | sh
  ```
- install global tools
  ```bash
  pixi global install git make uv pre-commit
  ```

### Development setup

```bash
git clone https://github.com/ascii-supply-networks/dagster-slurm.git
cd dagster-slurm
pixi run pre-commit-install
pixi run pre-commit-run
```

### Useful commands during development

```bash
# fmt
pixi run -e build --frozen fmt

# lint
pixi run -e build --frozen lint

# test
pixi run -e build --frozen test

# documentation
pixi run -e docs --frozen docs-serve
pixi run -e docs --frozen docs-build
```

When committing - ensure semver compatible standard commit patterns are followed.

### Dependency upgrade

```bash
pixi update
pixi run -e build --frozen sync-lib-with-upgrade
cd examples pixi update
```

## Local slurm cluster

Start a local slurm cluster via

```bash
docker compose up
ssh submitter@localhost -p 2223
# password: submitter
sinfo
```
