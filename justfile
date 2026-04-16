# Default recipe to display available commands
default:
    @just --list

# Bootstrap all environments and install hooks
setup:
    pixi run -e build --frozen sync
    pixi run -e docs --frozen docs-install
    pixi run -e docs --frozen slides-install
    cd examples && pixi install -e dev --frozen
    cd examples && pixi install -e workload-document-processing --frozen
    prek install

# Install prek git hooks
prek-install:
    prek install

# Update lockfiles in root/examples and refresh uv dependencies in the root project
update:
    pixi update
    cd examples && pixi update
    uv sync --all-packages --upgrade

# Delete local branches merged into main, with remote-gone branches, and prune remotes.
# Safe mode: `just clean-branches`
# Force mode: `just clean-branches --force`
clean-branches force="false":
    #!/usr/bin/env bash
    set -euo pipefail
    git fetch --prune
    default=$(git symbolic-ref refs/remotes/origin/HEAD | sed 's@^refs/remotes/origin/@@')
    original_current=$(git branch --show-current)
    current="${original_current}"
    force_value="{{force}}"

    if [[ "${force_value}" == "--force" || "${force_value}" == "force=true" ]]; then
        force_value="true"
    fi

    if [ "${current}" != "${default}" ]; then
        git checkout "${default}"
    fi

    if [ "${force_value}" = "true" ]; then
        echo "Force: deleting ALL local branches except ${default} and the originally checked out branch ${original_current}"
        branches=$(
            git for-each-ref --format='%(refname:short)' refs/heads \
                | grep -vFx "${default}" \
                | grep -vFx "${original_current}" \
                || true
        )
        if [ -n "${branches}" ]; then
            echo "${branches}" | xargs git branch -D
        fi
    else
        echo "Safe: deleting branches merged into ${default} and branches whose remote is gone"
        # branches fully merged into main
        merged=$(git branch --merged "${default}" | grep -vE "^\*|^\+| ${default}$" || true)
        if [ -n "${merged}" ]; then
            echo "${merged}" | xargs git branch -d
        fi
        # branches whose remote tracking ref has been pruned
        gone=$(git branch -vv | awk '/: gone]/{print $1}' | grep -vE "^\*|^\+|^${default}$" || true)
        if [ -n "${gone}" ]; then
            echo "${gone}" | xargs git branch -D
        fi
        echo "Tip: run 'just clean-branches --force' to nuke ALL branches except ${default}"
    fi

# Run prek on all files
prek-run:
    prek run --all-files

# Format all code in root project
fmt:
    pixi run -e build --frozen fmt

# Lint all code in root project
lint:
    pixi run -e build --frozen lint

# Format code in examples project
fmt-examples:
    cd examples && pixi run -e ci-basics --frozen fmt

# Lint code in examples project (formatting only, no type checking)
lint-examples:
    cd examples && pixi run -e ci-basics --frozen ruff check ./projects && pixi run -e ci-basics --frozen dprint check

# Lint examples with full type checking (may show import errors in ci-basics env)
lint-examples-full:
    cd examples && pixi run -e ci-basics --frozen lint

# Format both root and examples
fmt-all: fmt fmt-examples

# Lint both root and examples (formatting only)
lint-all: lint lint-examples

# Run all checks (format + lint) on root
check: fmt lint

# Run all checks (format + lint) on examples
check-examples: fmt-examples lint-examples

# Run all checks on both root and examples
check-all: fmt-all lint-all

# Run integration tests against SLURM cluster (requires docker slurm)
test-integration:
    pixi run -e build --frozen testpixi-integration

# Format documentation markdown
fmt-docs:
    pixi run -e build --frozen fmt-docs

# Check documentation markdown formatting
lint-docs:
    pixi run -e build --frozen lint-docs

# Serve documentation locally (includes API docs and slides)
docs-serve:
    pixi run -e docs --frozen docs-serve

# Build documentation for production
docs-build:
    pixi run -e docs --frozen docs-build

# Serve slides in development mode
slides:
    pixi run -e docs --frozen slides

# Build slides as static files
slides-build:
    pixi run -e docs --frozen slides-build

# Export slides to PDF
slides-export-pdf:
    pixi run -e docs --frozen slides-export-pdf

slides-export-pdf-multimodal:
    pixi run -e docs --frozen slides-export-pdf-multimodal
