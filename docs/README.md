# dagster-slurm

## contributing

you have to have uv installed for the library development - but pixi for executing the examples.

preparation:

- installation of pixi: https://pixi.sh/latest/installation/ `curl -fsSL https://pixi.sh/install.sh | sh`
- `pixi global install git`
- `pixi global install make`
- `pixi global install uv`

```bash
git clone https://github.com/ascii-supply-networks/dagster-slurm.git
cd dagster-slurm
pixi run pre-commit-install
pixi run pre-commit-run

# to build the library
pixi run -e build --frozen build-lib

# to publish
pixi run -e build --frozen publish-lib

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

## release

Verify release

```bash
uv run semantic-release -v --noop version
```
