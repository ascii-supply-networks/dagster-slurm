---
sidebar_position: 3
title: Release process
---
This project uses [semantic-release](https://semantic-release.gitbook.io/) for automated versioning and publishing.


When committing - ensure semver compatible standard commit patterns are followed.

The CI/CD fully automates releases based on semantic release patterns.
Each merge to main results in a new pre-release being pushed to pypi.
A full release is triggered by manually triggering the release workflow in the CI/CD pipeline.

Possibly intersting, but less useful ones as CI automates them:

```bash
# to build the library
pixi run -e build --frozen build-lib

# to publish (albeit CI is doing this automatically)
pixi run -e build --frozen publish-lib

```

Check what version would be released:

```bash
uv run semantic-release -v --noop version
```