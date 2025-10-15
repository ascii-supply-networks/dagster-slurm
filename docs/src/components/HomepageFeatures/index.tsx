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
    title: 'Unified Compute Interface',
    // Suggestion: An SVG representing a developer or a clean interface.
    Svg: require('@site/static/img/undraw_docusaurus_mountain.svg').default,
    description: (
      <>
        Develop on your laptop, deploy to an HPC cluster. The <strong>ComputeResource</strong> provides a single, consistent API for local and remote execution, so your asset code never has to change. Dependencies are consistently managed and distributed automatically via <strong>pixi</strong>
      </>
    ),
  },
  {
    title: 'HPC-Optimized Execution',
    // Suggestion: An SVG representing a server cluster or high performance.
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        Go beyond simple job submission. Use <strong>multi-assets</strong> to fuse multiple assets into a single Slurm allocation, minimizing queue times.
      </>
    ),
  },
  {
    title: 'Extensible & Environment-Aware',
    // Suggestion: An SVG representing building blocks or packaging.
    Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Built-in <strong>Launchers</strong> for Ray and Spark (WIP) enable distributed workloads out of the box. Automatic environment packaging with <code>pixi</code> ensures that your remote execution environment is a perfect replica of your local one.
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