---
title: 'Discovering the SUPER in computing - dagster-slurm for reproducible research on HPC'
tags:
  - Python
  - dagster
  - slurm
  - reproducible-research
  - RSE
  - secops
  - sops
  - age
  - pixi
  - pixi-pack
  - conda
authors:
  - name: Hernan Picatto
    orcid: 0000-0002-8684-1163
    affiliation: "2"
  - name: Georg Heiler
    orcid: 0000-0002-8684-1163
    affiliation: "1, 2"
affiliations:
 - name: Complexity Science Hub Vienna (CSH)
   index: 1
 - name: Austrian Supply Chain Intelligence Institute (ASCII)
   index: 2

date: 1st November 2025
bibliography: paper.bib

# Optional fields if submitting to a AAS journal too, see this blog post:
# <https://blog.joss.theoj.org/2018/12/a-new-collaboration-with-aas-publishing
aas-doi: 10.3847/xxxxx <- update this with the DOI from AAS once you know it.
aas-journal: Journal of Open Source Software
---

# Summary

TODO write the text

Some citation [@graham_mcps_2025].

# Statement of Need 


# Example usage

Then you can start the template project with:
```bash
git clone https://github.com/ascii-supply-networks/dagster-slurm.git
cd dagster-slurm/examples

# local
pixi run start

# slurm docker
docker compose up -d
pixi run start-staging
```

Then go to http://localhost:3000 and explore for yourself
```

Check out for much more detail:
- [the docs](https://ascii-supply-networks.github.io/dagster-slurm/)
- [the example](https://github.com/ascii-supply-networks/dagster-slurm/tree/main/examples)

# Impact and future work

Please use and help us improve this integration.
https://github.com/ascii-supply-networks/dagster-slurm

# Acknowledgements

# References