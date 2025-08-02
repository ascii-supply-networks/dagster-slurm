# dagster-slurm

## contributing

you have to have uv installed for the library development - but pixi for executing the examples.

preparation:

```bash
# https://pixi.sh/latest/installation/#__tabbed_1_1
curl -fsSL https://pixi.sh/install.sh | sh
pixi global install uv
```

```bash
uv clean
uv sync --all-packages --upgrade
uv build --all-packages

uv publish --token pypi-xxx
```
