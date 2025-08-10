---
sidebar_position: 2
title: Contributing
---

## Call for contribution

As part of the hackathon (https://www.openhackathons.org/s/siteevent/a0CUP000013Tp8f2AC/se000375) we intend to work on this integration.
We are looking for more hands to join in - or review the task list so that we can make sure we are not missing anything.

- Tasks for implementation: https://github.com/orgs/ascii-supply-networks/projects/4
- Project lives here https://github.com/ascii-supply-networks/dagster-slurm

See the (draft) [documentation](https://ascii-supply-networks.github.io/dagster-slurm/)

> We are actively looking for contributions to bring this package to life together


## Contributing

You have to have uv installed for the library development - but pixi for executing the examples.

### Prerequisites

- **pixi installation**: https://pixi.sh/latest/installation/
  ```bash
  curl -fsSL https://pixi.sh/install.sh | sh
  ````
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
