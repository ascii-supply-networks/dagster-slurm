---
title: 'LLM-in-a-Box: A Templated, Self-Hostable Framework for Generative AI in Research'
tags:
  - Python
  - dagster
  - slurm
  - reproducible research
  - RSE
  - secops
  - sops
  - age
authors:
  - name: Georg Heiler
    orcid: 0000-0002-8684-1163
    affiliation: "1, 2"
affiliations:
 - name: Complexity Science Hub Vienna (CSH)
   index: 1
 - name: Austrian Supply Chain Intelligence Institute (ASCII)
   index: 2

date: 1st October 2025
bibliography: paper.bib

# Optional fields if submitting to a AAS journal too, see this blog post:
# <https://blog.joss.theoj.org/2018/12/a-new-collaboration-with-aas-publishing
aas-doi: 10.3847/xxxxx <- update this with the DOI from AAS once you know it.
aas-journal: Journal of Open Source Software
---

# Summary

TODO write the text

Some citatione [@graham_mcps_2025].

# Statement of Need 


# Example usage

some code
Then you can start the template project with:
```bash
# cpu
docker compose \
  --profile llminabox \
  --profile ollama-cpu \
  --profile docling-cpu \
  --profile vectordb-cpu \
  up -d
```

Please follow along with the [README.md](https://github.com/complexity-science-hub/llm-in-a-box-template/blob/main/README.md) file from here.

# Impact and future work


# Acknowledgements

# References