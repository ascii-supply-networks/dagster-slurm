# dagster slurm example

## using the example

### prerequisites

- installation of pixi: https://pixi.sh/latest/installation/ `curl -fsSL https://pixi.sh/install.sh | sh`
- `pixi global install git`
- `pixi global install make`


### usage

```
git clone https://github.com/ascii-supply-networks/dagster-slurm.git
cd dagster-slurm/examples

pixi shell -e dev
ray start --head --port=6379
make start
```

go to http://localhost:3000 and you should see the dagster webserver running.