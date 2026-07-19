import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<'svg'>>;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Configure once, then click Materialize',
    Svg: require('@site/static/img/undraw_server-cluster_7ugi.svg').default,
    description: (
      <>
        dagster-slurm transfers the payload from your current checkout, provides
        its Pixi-locked environment, submits it with <code>sbatch</code>, and
        returns the job status and logs to Dagster.
      </>
    ),
  },
  {
    title: 'Fast feedback before HPC',
    Svg: require('@site/static/img/undraw_scooter_izdb.svg').default,
    description: (
      <>
        Run the same asset locally without SSH or Slurm. Developers and coding
        agents can iterate quickly, then switch the compute configuration when
        the workload is ready for the cluster.
      </>
    ),
  },
  {
    title: 'Advanced HPC controls',
    Svg: require('@site/static/img/undraw_monitor_ypga.svg').default,
    description: (
      <>
        Dagster reports Slurm state, logs, CPU efficiency, memory, elapsed time,
        and node-hours. Choose environments per asset and define a separate{' '}
        <code>ComputeResource</code> per cluster. Run-scoped Ray can reuse one
        allocation; shared-allocation sessions and HET jobs are experimental.
      </>
    ),
  },
];

function Feature({title, Svg, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}

export function HomepagePipeline(): ReactNode {
  return (
    <section className={styles.pipeline}>
      <div className="container text--center">
        <Heading as="h2">Before, during, and after HPC</Heading>
        <p className={styles.pipelineLead}>
          <strong>Before:</strong> ingest and validate data.{' '}
          <strong>During:</strong> send the heavy asset to Slurm.{' '}
          <strong>After:</strong> aggregate, publish, or serve the result. Dagster
          dependencies and lineage connect every stage, even when they run in
          different places.
        </p>
      </div>
    </section>
  );
}
