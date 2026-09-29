#!/usr/bin/env bash
# Push a release made by `semantic-release version --no-push`, then create its
# GitHub release.
#
# semantic-release pushes the version commit and its tag separately. When the
# tag push failed, main kept a version commit without a tag, and a re-run then
# refused to release because main had advanced. One atomic push updates both
# refs or neither, so a failed push leaves main as tested and the job can be
# re-run.
set -euo pipefail

version="$(grep "version =" pyproject.toml | head -1 | cut -d'"' -f2)"
tag="v${version}"
if ! git rev-parse --verify --quiet "refs/tags/${tag}" >/dev/null; then
    echo "::error::semantic-release did not create tag ${tag}" >&2
    exit 1
fi

for attempt in 1 2 3; do
    if git push --atomic origin main "refs/tags/${tag}"; then
        break
    fi
    if ((attempt == 3)); then
        echo "::error::Could not push main and ${tag}" >&2
        exit 1
    fi
    sleep $((attempt * 15))
done

# `--no-push` also skipped the GitHub release, which `publish` uploads to.
pixi run -e build --frozen uv run semantic-release changelog --post-to-release-tag "${tag}"
