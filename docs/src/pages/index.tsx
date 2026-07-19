import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import HomepageFeatures, {
  HomepagePipeline,
} from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <p className={styles.eyebrow}>dagster-slurm</p>
        <Heading as="h1" className="hero__title">
          Develop locally. Run on Slurm. Orchestrate the whole pipeline.
        </Heading>
        <p className={clsx('hero__subtitle', styles.heroSubtitle)}>
          Configure a cluster once, then materialize Dagster assets with the
          payload from your current checkout and a Pixi-locked environment. Keep
          local feedback, Slurm telemetry, and data dependencies in one graph.
        </p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro">
            Get started
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            href="https://github.com/ascii-supply-networks/dagster-slurm">
            View on GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="Run Dagster data pipelines on Slurm HPC"
      description="Develop Dagster assets locally, run selected steps on Slurm, and keep environments, telemetry, and end-to-end data dependencies in one pipeline.">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <HomepagePipeline />
      </main>
    </Layout>
  );
}
