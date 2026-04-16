#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || {
    echo "Not inside a git repository." >&2
    exit 1
}

cd "${repo_root}"

force_value="${1:-false}"
if [[ "${force_value}" == "--force" || "${force_value}" == "force=true" ]]; then
    force_value="true"
fi

git fetch --prune

default="$(git symbolic-ref refs/remotes/origin/HEAD | sed 's@^refs/remotes/origin/@@')"
original_current="$(git branch --show-current)"
current="${original_current}"

if [[ "${current}" != "${default}" ]]; then
    git checkout "${default}"
fi

if [[ "${force_value}" == "true" ]]; then
    echo "Force: deleting ALL local branches except ${default} and the originally checked out branch ${original_current}"
    branches="$(
        git for-each-ref --format='%(refname:short)' refs/heads \
            | grep -vFx "${default}" \
            | grep -vFx "${original_current}" \
            || true
    )"
    if [[ -n "${branches}" ]]; then
        echo "${branches}" | xargs git branch -D
    fi
else
    echo "Safe: deleting branches merged into ${default} and branches whose remote is gone"
    merged="$(git branch --merged "${default}" | grep -vE "^\*|^\+| ${default}$" || true)"
    if [[ -n "${merged}" ]]; then
        echo "${merged}" | xargs git branch -d
    fi

    gone="$(git branch -vv | awk '/: gone]/{print $1}' | grep -vE "^\*|^\+|^${default}$" || true)"
    if [[ -n "${gone}" ]]; then
        echo "${gone}" | xargs git branch -D
    fi

    echo "Tip: rerun this cleanup with --force to nuke ALL branches except ${default}"
fi
