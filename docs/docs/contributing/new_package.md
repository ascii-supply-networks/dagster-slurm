---
sidebar_position: 5
title: New package added
---

When a new package was added:

1. manually add it to pypi.org and test.pypi.org
2. update the trusted publish settings in pyi and test.pypi to reflect the change

## manual upload

```bash
uv build --all-packages
UV_PUBLISH_TOKEN=pypi-<<token>> uv publish --index testpypi

UV_PUBLISH_TOKEN=pypi-<<token>> uv publish
```
