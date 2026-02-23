# Repository-local Agent Skills

This repository vendors Dagster skills as a git submodule at:

- `.agent-skills/dagster-skills`

## For collaborators

Initialize the submodule after cloning:

```bash
git submodule update --init --recursive
```

Update to the latest upstream version when needed:

```bash
git submodule update --remote .agent-skills/dagster-skills
```

## Skill locations

Dagster skills are under:

- `.agent-skills/dagster-skills/skills/dagster-expert`
- `.agent-skills/dagster-skills/skills/dagster-integrations`
- `.agent-skills/dagster-skills/skills/dignified-python`

This keeps a single shared copy inside the repo; no global installation is required by this repository setup itself.
