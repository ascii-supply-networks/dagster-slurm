"""Payloads for the rapids_topics example.

LDA -> UMAP -> HDBSCAN topic-modeling chain over the Reuters-21578
corpus, mirroring a production Common Crawl pipeline at toy scale:

1. ``prepare_corpus.py``  - download + parse Reuters, shared vocabulary
2. ``lda_train.py``       - one scikit-learn LDA per (month, seed) partition
3. ``aggregate_topics.py``- stack topic-term vectors from all models
4. ``umap_reduce.py``     - cuML UMAP (CPU umap-learn fallback)
5. ``hdbscan_cluster.py`` - cuML HDBSCAN (CPU hdbscan fallback)
6. ``topic_map.py``       - labeled meta-topic scatter + summary

The CPU stages (1-3) run in the ``workload-topic-modeling`` env; the
GPU stages (4-6) run in ``packaged-cluster-rapids`` and fall back to
the CPU implementations when cuML is not importable, so the same
payloads run unchanged on the docker slurm cluster and on a GPU node.
"""
