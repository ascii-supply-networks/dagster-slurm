# CHANGELOG

<!-- version list -->

## v1.18.0-rc.2 (2026-08-04)

### Chores

- Update uv lockfile version [skip ci]
  ([`e167b1d`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e167b1d07047ae2ccb069fa8e627e18e374bc705))

### Features

- Summarize SSH stdio forwarding
  ([`e726a1c`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e726a1c0405532bc3b0d49146c50574e1274b0a1))

### Refactoring

- Centralize SSH message forwarding
  ([`cf691a3`](https://github.com/ascii-supply-networks/dagster-slurm/commit/cf691a3efdb9c07e666e9ed2061e895351193dd6))


## v1.18.0-rc.1 (2026-08-04)

### Bug Fixes

- Avoid duplicating SSH stdio events
  ([`4919fb4`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4919fb48e7aa117e3b9ff8821b4642b0f24d5bcf))

### Chores

- Update uv lockfile version [skip ci]
  ([`8572b68`](https://github.com/ascii-supply-networks/dagster-slurm/commit/8572b68e83e1e92a0927565a2f218c828644e09c))


## v1.18.0 (2026-08-02)

### Chores

- Update uv lockfile version [skip ci]
  ([`4d6e718`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4d6e71893dab955acd52168431a86f54c2ea56fd))


## v1.17.0-rc.2 (2026-08-02)

### Bug Fixes

- Assign Ray client port on worker nodes
  ([`0ae351c`](https://github.com/ascii-supply-networks/dagster-slurm/commit/0ae351cc50a493acbc9342aff0bac8efbc387b42))

- Preserve workload exit after pre-timeout signal
  ([`0df210b`](https://github.com/ascii-supply-networks/dagster-slurm/commit/0df210bf8cab392a7857f36dcd6053ca073d1905))

- Tolerate delayed Ray worker registration
  ([`5f26423`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5f2642339f5e577518d7ce03e570e9d82cef7a0e))

### Chores

- Update uv lockfile version [skip ci]
  ([`fcbe298`](https://github.com/ascii-supply-networks/dagster-slurm/commit/fcbe298eb431383a8a33b44f19a6a38620e29371))


## v1.17.0-rc.1 (2026-08-02)

### Chores

- Update uv lockfile version [skip ci]
  ([`42ff14f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/42ff14fba65d88eef740a046cc99f9f6d8f07399))

### Features

- Add extensible Slurm GPU metrics
  ([`96e72fe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/96e72fe96dc58dc38efe180f96de5a9339dbfd40))


## v1.17.0 (2026-08-01)

### Chores

- Update uv lockfile version [skip ci]
  ([`dbc11ec`](https://github.com/ascii-supply-networks/dagster-slurm/commit/dbc11ecf95db33e37203fe426432666aa543440a))


## v1.16.0-rc.1 (2026-08-01)

### Bug Fixes

- Bound pipes message reader reconnect storms
  ([`9cf0725`](https://github.com/ascii-supply-networks/dagster-slurm/commit/9cf072588a3c835aad0cdb6d695324da594e94a1))

- Keep status polling from overshooting its own deadline
  ([`d7b60b8`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d7b60b89054535182c04e87ee45facdc48a57a7d))

- Merge nested launcher config instead of replacing it
  ([`2e1e3f7`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2e1e3f7ca89e70cf644472cfb8a9bb9445a49798))

- Never disable ssh multiplexing silently on long socket paths
  ([`1fbb23d`](https://github.com/ascii-supply-networks/dagster-slurm/commit/1fbb23d98e8b66361a61e78c2dad882c3207c47e))

- Report password auth as unsupported, and report from the pool
  ([`fc22809`](https://github.com/ascii-supply-networks/dagster-slurm/commit/fc22809632239066bbafafe2d296ad5ccd8cffe9))

### Chores

- Update uv lockfile version [skip ci]
  ([`46bde79`](https://github.com/ascii-supply-networks/dagster-slurm/commit/46bde79faf891f0b7c1bb006a039c60e927b2c15))

### Features

- Back off slurm status polling and report lost multiplexing
  ([`d26eee2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d26eee28d9d55dc3e103470ca67772e12142ef68))

- Emit loud warning in case of ssh controlmaster failure
  ([`4373082`](https://github.com/ascii-supply-networks/dagster-slurm/commit/43730825aaf751dcd5c67ee5984d88e75b5ce7af))

- Let ssh settings defer to the operator's ssh config
  ([`5068a8a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5068a8a57a112030f89fe5e6b6f13474b442c2d3))

- Notice a finished job as soon as pipes reports it closed
  ([`60d547b`](https://github.com/ascii-supply-networks/dagster-slurm/commit/60d547b2a4074fe29bebe3b17e860d22d282b544))

- Parallel ray clusters and advanced networking on slurm
  ([`4b8cc6f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4b8cc6faacaa6e9bf52beea4b9c08c1518f151a5))

- Share and self-heal a single ssh controlmaster
  ([`9f5cd05`](https://github.com/ascii-supply-networks/dagster-slurm/commit/9f5cd05a304e50fd52257f2b967f60acf1a3a58b))

- Signal before timeout
  ([`1b47398`](https://github.com/ascii-supply-networks/dagster-slurm/commit/1b4739868d15be3d8a647ab34a6699e19a3fa12d))

- Ssh strict mode
  ([`5d10054`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5d10054a89fe4717a9c4bad3f59fd6b149bc3d30))

- Update devtooling
  ([`83afe98`](https://github.com/ascii-supply-networks/dagster-slurm/commit/83afe98c84177b6c6aedb0570d143afaea7bdc1f))

### Performance Improvements

- Collapse the post-completion ssh round trips
  ([`83eb5d4`](https://github.com/ascii-supply-networks/dagster-slurm/commit/83eb5d473c80d083e89c3d7c2dee64a78d3127eb))

- Drain instead of sleeping on the job-completion path
  ([`df8b392`](https://github.com/ascii-supply-networks/dagster-slurm/commit/df8b39268d3fbc491e34e9c34c6844050c089501))

### Testing

- Assert the new pre-timeout contract in the integration test
  ([`4b60e35`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4b60e352ba1d5c8b4bc99264e9697817dc679519))

- Teach the ssh pool test double about multiplexing state
  ([`2258468`](https://github.com/ascii-supply-networks/dagster-slurm/commit/225846882f6db01fb4e991d1feb90d7d6883c2e3))


## v1.16.0 (2026-07-30)

### Chores

- Update uv lockfile version [skip ci]
  ([`2c52350`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2c52350e892c24227be2bc49f1ffbcca9d877af0))


## v1.15.2-rc.1 (2026-07-30)

### Bug Fixes

- Make stable release notes cumulative
  ([`15cfa1e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/15cfa1e796dfcbc23b468c97fd98f7d5590ec800))

- Retry tutorial environment packing
  ([`5a9cb17`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5a9cb17868a1744107c860ca6de04b51799a0693))

- Revalidate stale release candidates
  ([`ff8e7c9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/ff8e7c96d2ebd608cd550c7f733dad002b1f658b))

### Chores

- Update uv lockfile version [skip ci]
  ([`7f8ab7b`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7f8ab7bd73d2caa734dc2481566ea67063bb1379))

### Features

- Add project setup hooks
  ([`aada2fa`](https://github.com/ascii-supply-networks/dagster-slurm/commit/aada2fad14d35d97164610b5753e8d71054c948b))


## v1.15.2 (2026-07-29)

### Chores

- Update uv lockfile version [skip ci]
  ([`a8d87d6`](https://github.com/ascii-supply-networks/dagster-slurm/commit/a8d87d664d21de1f24ec586c9c39be1b5135b905))


## v1.15.1-rc.1 (2026-07-29)

### Bug Fixes

- Stop SSH log streams promptly
  ([`dc2c890`](https://github.com/ascii-supply-networks/dagster-slurm/commit/dc2c8902a50f4a81b2ae7e41367df40df5db55c0))


## v1.15.1 (2026-07-24)


## v1.15.0-rc.1 (2026-07-24)

### Bug Fixes

- Pass release token to semantic-release
  ([`e4cb06c`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e4cb06cf7ac79c872003932c46bc5f45d9fa5792))


## v1.15.0 (2026-07-24)


## v1.14.0-rc.9 (2026-07-24)

### Bug Fixes

- Stage remote pack inputs as archive
  ([`9eacafb`](https://github.com/ascii-supply-networks/dagster-slurm/commit/9eacafb066432aa8d68cf15c8e83a016432c076e))


## v1.14.0-rc.8 (2026-07-24)

### Bug Fixes

- Env wiring slurm remote base
  ([`739bc06`](https://github.com/ascii-supply-networks/dagster-slurm/commit/739bc066da10452358acb0577fd908daf9f1b0df))

- Remote packaging
  ([`519e70e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/519e70ebf3b8315a3d055499a9152398ccfe023a))

### Documentation

- Updated the getting started documentation for the beginners
  ([#163](https://github.com/ascii-supply-networks/dagster-slurm/pull/163),
  [`b14daac`](https://github.com/ascii-supply-networks/dagster-slurm/commit/b14daac142b1a3e0e8cb3dda078e93ac6d275fd1))

### Features

- Upgrade stax to standard release
  ([`29ab859`](https://github.com/ascii-supply-networks/dagster-slurm/commit/29ab859cfa77fc0d475aabdb0e53488b6ed98535))


## v1.14.0-rc.7 (2026-07-20)

### Bug Fixes

- **ci**: Hotfix release
  ([`7fcfed4`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7fcfed4830a6abe3a95373a7e9aca971aa1a5e8e))


## v1.14.0-rc.6 (2026-07-19)

### Continuous Integration

- Verify built release distributions
  ([#159](https://github.com/ascii-supply-networks/dagster-slurm/pull/159),
  [`1a625a9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/1a625a987bdd23109f0372fe4206b4ad5d9ba487))

### Documentation

- Improve landing page discoverability
  ([#161](https://github.com/ascii-supply-networks/dagster-slurm/pull/161),
  [`137e7e4`](https://github.com/ascii-supply-networks/dagster-slurm/commit/137e7e4167b70bc576bb8c2de003f637f288582c))

### Refactoring

- Replace gensim with scikit-learn
  ([#157](https://github.com/ascii-supply-networks/dagster-slurm/pull/157),
  [`8330aca`](https://github.com/ascii-supply-networks/dagster-slurm/commit/8330aca85f67aaec3052230980735cee6bef3665))


## v1.14.0-rc.5 (2026-07-19)

### Bug Fixes

- Use build output path for releases
  ([#158](https://github.com/ascii-supply-networks/dagster-slurm/pull/158),
  [`78e1fe1`](https://github.com/ascii-supply-networks/dagster-slurm/commit/78e1fe1e7d4c5e16f2216d635ac1d2abfedc7ee0))


## v1.14.0-rc.4 (2026-07-18)

### Bug Fixes

- Address topic modeling review comments
  ([#153](https://github.com/ascii-supply-networks/dagster-slurm/pull/153),
  [`2bd8df3`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2bd8df395693c64c88d6c65fd0cdc6775e03dea5))

- Load native libraries from package
  ([#153](https://github.com/ascii-supply-networks/dagster-slurm/pull/153),
  [`2bd8df3`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2bd8df395693c64c88d6c65fd0cdc6775e03dea5))

- Reuse existing environment cache
  ([#153](https://github.com/ascii-supply-networks/dagster-slurm/pull/153),
  [`2bd8df3`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2bd8df395693c64c88d6c65fd0cdc6775e03dea5))

### Features

- Rapids topic modeling example
  ([#153](https://github.com/ascii-supply-networks/dagster-slurm/pull/153),
  [`2bd8df3`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2bd8df395693c64c88d6c65fd0cdc6775e03dea5))


## v1.14.0-rc.3 (2026-07-18)

### Bug Fixes

- Load native libraries from package
  ([#156](https://github.com/ascii-supply-networks/dagster-slurm/pull/156),
  [`d02ba1d`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d02ba1d88a2f6c75e983f7eaab260fedbd75da55))


## v1.14.0-rc.2 (2026-07-18)

### Bug Fixes

- **deps**: Native and pixi named platforms
  ([#155](https://github.com/ascii-supply-networks/dagster-slurm/pull/155),
  [`23805a2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/23805a203b10930538bfe7fdaa7d589c9ca595a8))


## v1.14.0-rc.1 (2026-07-18)

### Continuous Integration

- Add devenv shell bootstrap
  ([#149](https://github.com/ascii-supply-networks/dagster-slurm/pull/149),
  [`427ec8f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/427ec8f0dc925ea055abdbc58152c0096809a88a))

- Add supported Python test matrix
  ([#150](https://github.com/ascii-supply-networks/dagster-slurm/pull/150),
  [`c3c2a92`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c3c2a92a884d4ff516c892f90453c2e9a5e9a1db))

- Update actions and dependency pins
  ([#148](https://github.com/ascii-supply-networks/dagster-slurm/pull/148),
  [`523a024`](https://github.com/ascii-supply-networks/dagster-slurm/commit/523a024a6750d3ea40359ce9f9788ab596d073b3))

### Features

- **infra**: Cleanup ci and releases
  ([#151](https://github.com/ascii-supply-networks/dagster-slurm/pull/151),
  [`a8cc9b4`](https://github.com/ascii-supply-networks/dagster-slurm/commit/a8cc9b43cc13a7cec135a05078ea197a8fbea05d))


## v1.14.0 (2026-05-29)


## v1.13.0-rc.6 (2026-05-29)

### Features

- Single slurm allocation ([#146](https://github.com/ascii-supply-networks/dagster-slurm/pull/146),
  [`f649b44`](https://github.com/ascii-supply-networks/dagster-slurm/commit/f649b44ff5b7e850486a23279d2258af965f852a))


## v1.13.0-rc.5 (2026-05-14)

### Bug Fixes

- Concurrent executions of asset
  ([#143](https://github.com/ascii-supply-networks/dagster-slurm/pull/143),
  [`2188025`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2188025263074e13bb15e8b05d6053dbf849cc7d))

- Improve resiliency ([#143](https://github.com/ascii-supply-networks/dagster-slurm/pull/143),
  [`2188025`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2188025263074e13bb15e8b05d6053dbf849cc7d))

### Features

- Remote packaging ([#143](https://github.com/ascii-supply-networks/dagster-slurm/pull/143),
  [`2188025`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2188025263074e13bb15e8b05d6053dbf849cc7d))


## v1.13.0-rc.4 (2026-05-14)

### Bug Fixes

- Concurrent executions of asset
  ([#144](https://github.com/ascii-supply-networks/dagster-slurm/pull/144),
  [`d15a32e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d15a32e107edb1f535c39757db7e8d5619312eac))

- Improve resiliency ([#144](https://github.com/ascii-supply-networks/dagster-slurm/pull/144),
  [`d15a32e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d15a32e107edb1f535c39757db7e8d5619312eac))

- Resiliency ([#144](https://github.com/ascii-supply-networks/dagster-slurm/pull/144),
  [`d15a32e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d15a32e107edb1f535c39757db7e8d5619312eac))


## v1.13.0-rc.3 (2026-05-14)

### Chores

- Cleanup ([#135](https://github.com/ascii-supply-networks/dagster-slurm/pull/135),
  [`cf51a11`](https://github.com/ascii-supply-networks/dagster-slurm/commit/cf51a110ee153c9c4cabdf9395ce58c8bf9b7059))

- Docs favicon more ([#134](https://github.com/ascii-supply-networks/dagster-slurm/pull/134),
  [`94b5bb9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/94b5bb968b15c27bd29234cd2c2a4dcca15fa332))

- Favicon for slides ([#133](https://github.com/ascii-supply-networks/dagster-slurm/pull/133),
  [`bafde12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/bafde12755747b124acb8f06c1bab45b07d23464))

- Refine slides ([#136](https://github.com/ascii-supply-networks/dagster-slurm/pull/136),
  [`49bf5c1`](https://github.com/ascii-supply-networks/dagster-slurm/commit/49bf5c1803ba5f5cc8e68c144559d5e8ca5e8e90))

### Features

- Remote packaging ([#140](https://github.com/ascii-supply-networks/dagster-slurm/pull/140),
  [`0baec8a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/0baec8a8694df856bb28eb363a66b70271b96922))

- Update things ([#139](https://github.com/ascii-supply-networks/dagster-slurm/pull/139),
  [`f1cd690`](https://github.com/ascii-supply-networks/dagster-slurm/commit/f1cd690165f6a305b581f94a0aec2c8665309be9))


## v1.13.0-rc.2 (2026-04-19)

### Bug Fixes

- **slides**: Replace cross-repo symlinks with real files + add compute-flex slide
  ([#131](https://github.com/ascii-supply-networks/dagster-slurm/pull/131),
  [`f5ae0ea`](https://github.com/ascii-supply-networks/dagster-slurm/commit/f5ae0ea759a191a4afed9ac39c57634996432cda))

### Chores

- Docs build and slides html
  ([#132](https://github.com/ascii-supply-networks/dagster-slurm/pull/132),
  [`1aa4ded`](https://github.com/ascii-supply-networks/dagster-slurm/commit/1aa4ded15f013dcee4144dce216db9ff278f5aff))

- Reifne docs for step-oidc auth
  ([#130](https://github.com/ascii-supply-networks/dagster-slurm/pull/130),
  [`07b9fe5`](https://github.com/ascii-supply-networks/dagster-slurm/commit/07b9fe5f3db25277ba6ddbf0c0253f04d992497f))

- Scipi-2026-slides v1 ([#131](https://github.com/ascii-supply-networks/dagster-slurm/pull/131),
  [`f5ae0ea`](https://github.com/ascii-supply-networks/dagster-slurm/commit/f5ae0ea759a191a4afed9ac39c57634996432cda))


## v1.13.0-rc.1 (2026-04-17)

### Bug Fixes

- Demo more on musica after ASC infra now is hotfixed
  ([#129](https://github.com/ascii-supply-networks/dagster-slurm/pull/129),
  [`b3885b1`](https://github.com/ascii-supply-networks/dagster-slurm/commit/b3885b1a81e3669e16ded1a613b066bd2d584e6f))

- More demo datalab ([#129](https://github.com/ascii-supply-networks/dagster-slurm/pull/129),
  [`b3885b1`](https://github.com/ascii-supply-networks/dagster-slurm/commit/b3885b1a81e3669e16ded1a613b066bd2d584e6f))


## v1.13.0 (2026-04-17)

### Bug Fixes

- Docling demo musica more ([#127](https://github.com/ascii-supply-networks/dagster-slurm/pull/127),
  [`fa0f80f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/fa0f80f91ebe936f5ee4cfa5375f72e43f20b9fa))

### Chores

- Cleanup ([#128](https://github.com/ascii-supply-networks/dagster-slurm/pull/128),
  [`e473800`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e4738008e208700a0bde32e0eb0282f4df683ac0))


## v1.12.0-rc.5 (2026-04-16)

### Bug Fixes

- Demo more on musica after ASC infra now is hotfixed
  ([#126](https://github.com/ascii-supply-networks/dagster-slurm/pull/126),
  [`42b44b2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/42b44b2bafcaa50c4c30fbe751ff6e652d15ab20))

### Features

- Add branch cleaning ([#125](https://github.com/ascii-supply-networks/dagster-slurm/pull/125),
  [`61b578d`](https://github.com/ascii-supply-networks/dagster-slurm/commit/61b578df830c7f2e7afede29f9e2470aa6989f91))


## v1.12.0-rc.4 (2026-04-16)

### Bug Fixes

- Demo on musica ([#124](https://github.com/ascii-supply-networks/dagster-slurm/pull/124),
  [`73398ea`](https://github.com/ascii-supply-networks/dagster-slurm/commit/73398ea7398896c5c29ac9a829ac3d9cfed5d359))


## v1.12.0-rc.3 (2026-04-16)

### Bug Fixes

- Concurrent asset execution
  ([#123](https://github.com/ascii-supply-networks/dagster-slurm/pull/123),
  [`273cd5e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/273cd5ef9594ca04a7c3a5f7d892bf6b749b8bb8))


## v1.12.0-rc.2 (2026-04-16)

### Bug Fixes

- Demo ([#118](https://github.com/ascii-supply-networks/dagster-slurm/pull/118),
  [`e43e085`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e43e085e89b6a8dd93a86d61650b2d448895ec6e))

- Refresh token ([#117](https://github.com/ascii-supply-networks/dagster-slurm/pull/117),
  [`6735414`](https://github.com/ascii-supply-networks/dagster-slurm/commit/6735414cf7261a9ec38644d1a4051ae6baef9d28))

- Streamline token rotation for all cases
  ([#121](https://github.com/ascii-supply-networks/dagster-slurm/pull/121),
  [`f6d8d20`](https://github.com/ascii-supply-networks/dagster-slurm/commit/f6d8d2009111db776d8f580ae95281bc80df3733))

### Features

- Version updates ([#120](https://github.com/ascii-supply-networks/dagster-slurm/pull/120),
  [`085a609`](https://github.com/ascii-supply-networks/dagster-slurm/commit/085a60962eaefcd19e199cbb2f54747d37a2b480))

- Webinar reservation demo ([#119](https://github.com/ascii-supply-networks/dagster-slurm/pull/119),
  [`eabd465`](https://github.com/ascii-supply-networks/dagster-slurm/commit/eabd4656f0187e45266be1a418d1a7a1670b7a50))


## v1.12.0-rc.1 (2026-04-04)

### Bug Fixes

- Slides ([#115](https://github.com/ascii-supply-networks/dagster-slurm/pull/115),
  [`71e4b30`](https://github.com/ascii-supply-networks/dagster-slurm/commit/71e4b30d3f17aab886d5271425fdb21d8d3763e1))

### Features

- Updates ([#116](https://github.com/ascii-supply-networks/dagster-slurm/pull/116),
  [`0d04a96`](https://github.com/ascii-supply-networks/dagster-slurm/commit/0d04a96e6ef851a20879caba0920a073632d6b89))


## v1.12.0 (2026-03-24)


## v1.11.1-rc.2 (2026-03-24)

### Chores

- Add additional test for ComputeResource.run -> client.run hop
  ([#113](https://github.com/ascii-supply-networks/dagster-slurm/pull/113),
  [`fa9a312`](https://github.com/ascii-supply-networks/dagster-slurm/commit/fa9a3120c25294d0b36721cd05baed1cce72cbbc))

### Features

- Expose poll_timeout parameter
  ([#113](https://github.com/ascii-supply-networks/dagster-slurm/pull/113),
  [`fa9a312`](https://github.com/ascii-supply-networks/dagster-slurm/commit/fa9a3120c25294d0b36721cd05baed1cce72cbbc))


## v1.11.2 (2026-03-19)

### Features

- Expose `poll_timeout` parameter through public API (`ComputeResource.run()`,
  `SlurmPipesClient.run()`, `_execute_standalone()`) so callers can override the
  default 1-hour timeout for long-running Slurm jobs. The reattach code path also
  forwards the parameter.

## v1.11.1-rc.1 (2026-03-20)

### Bug Fixes

- Refine ray slurm ci ([#114](https://github.com/ascii-supply-networks/dagster-slurm/pull/114),
  [`1528fda`](https://github.com/ascii-supply-networks/dagster-slurm/commit/1528fda1682c642cda95bb87c1c5eeb716fd735f))

- Robustify ray CI ([#114](https://github.com/ascii-supply-networks/dagster-slurm/pull/114),
  [`1528fda`](https://github.com/ascii-supply-networks/dagster-slurm/commit/1528fda1682c642cda95bb87c1c5eeb716fd735f))

### Features

- Justfile commands ([#114](https://github.com/ascii-supply-networks/dagster-slurm/pull/114),
  [`1528fda`](https://github.com/ascii-supply-networks/dagster-slurm/commit/1528fda1682c642cda95bb87c1c5eeb716fd735f))


## v1.11.1 (2026-03-19)


## v1.11.0-rc.1 (2026-03-19)

### Bug Fixes

- Linting after upgrade ([#112](https://github.com/ascii-supply-networks/dagster-slurm/pull/112),
  [`c9b6e9a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c9b6e9a19ecd14cd16f0b5f3ba3c257f48c04a6d))


## v1.11.0 (2026-02-25)

### Bug Fixes

- Exclude changelog from dprint
  ([#106](https://github.com/ascii-supply-networks/dagster-slurm/pull/106),
  [`9431b5f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/9431b5f8ed7f164a606d548b32255ac59a2c1297))


## v1.10.0-rc.4 (2026-02-24)

### Bug Fixes

- Slides merged ([#104](https://github.com/ascii-supply-networks/dagster-slurm/pull/104),
  [`7d4b9c8`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7d4b9c8c9d646bb1c9c48354e7beb56c859337a0))

- Slides merged (#104) ([#105](https://github.com/ascii-supply-networks/dagster-slurm/pull/105),
  [`5d87531`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5d8753104c43cacfa6b9bd90927d187be783456f))


## v1.10.0-rc.3 (2026-02-23)

### Bug Fixes

- Add missing dependency ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Generate data also on slurm
  ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Make it work somehow ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Refine cancellation vs automatic continuation for long running jobs where dagster is restarted
  ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Refine cancellation vs automatic continuation for long running jobs where dagster is restarted
  ([#98](https://github.com/ascii-supply-networks/dagster-slurm/pull/98),
  [`93e6a2f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/93e6a2f3e3ac63361f2ee63225b397a068e97b44))

- Update deps for demo ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

### Features

- Add agster skills ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Cancellation AND continued remote execution when dagster is restarted
  ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Cancellation AND continued remote execution when dagster is restarted
  ([#98](https://github.com/ascii-supply-networks/dagster-slurm/pull/98),
  [`93e6a2f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/93e6a2f3e3ac63361f2ee63225b397a068e97b44))

- Docling example complete (for local execution)
  ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- First step towards skills here
  ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Metaxy basic version ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Mini ray example ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Slides first version ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

- Webinar demonstration multimodal document processing
  ([#101](https://github.com/ascii-supply-networks/dagster-slurm/pull/101),
  [`c561bbe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c561bbe5cdcb2fd10b7d74121a065030a7aa5e1b))

## v1.10.0-rc.2 (2026-02-18)

### Bug Fixes

- Address reviewers comments ([#97](https://github.com/ascii-supply-networks/dagster-slurm/pull/97),
  [`530e8ab`](https://github.com/ascii-supply-networks/dagster-slurm/commit/530e8ab4a52ff69bd257f4a48686cf80282bac33))

- Revision 1 ([#97](https://github.com/ascii-supply-networks/dagster-slurm/pull/97),
  [`530e8ab`](https://github.com/ascii-supply-networks/dagster-slurm/commit/530e8ab4a52ff69bd257f4a48686cf80282bac33))

## v1.10.0-rc.1 (2026-01-23)

### Features

- Example docling ray ([#90](https://github.com/ascii-supply-networks/dagster-slurm/pull/90),
  [`39a9cc7`](https://github.com/ascii-supply-networks/dagster-slurm/commit/39a9cc7a43aa17d888a6f094ddfccc73d181f80f))

## v1.10.0 (2026-01-12)

## v1.9.0-rc.1 (2026-01-12)

### Features

- Musica ([#88](https://github.com/ascii-supply-networks/dagster-slurm/pull/88),
  [`40b6065`](https://github.com/ascii-supply-networks/dagster-slurm/commit/40b6065b85a09cb998af5526de15fc299cdf9c31))

## v1.9.0 (2025-12-19)

### Bug Fixes

- Add ty directly from pixi
  ([`45673ce`](https://github.com/ascii-supply-networks/dagster-slurm/commit/45673ce7af680709b1319d9402b5de781a99bd0d))

- Direct lint env path ([#87](https://github.com/ascii-supply-networks/dagster-slurm/pull/87),
  [`3e3dbd7`](https://github.com/ascii-supply-networks/dagster-slurm/commit/3e3dbd7645c6b2a8d905877137db7f473fe94308))

- Try pre-creating env
  ([`b3f803a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/b3f803a234370b1add420f6b3def4eb266fcc856))

- Uv without pixi
  ([`0b0cb3e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/0b0cb3ef891936bf0282132134b49ab37b83470a))

### Features

- Update deps ([#86](https://github.com/ascii-supply-networks/dagster-slurm/pull/86),
  [`6b75b3a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/6b75b3a9fb60f341e19b55a8a4eb60dc0c19218d))

## v1.8.0-rc.1 (2025-12-19)

### Bug Fixes

- A) configurable cache b) selective cache invalidation
  ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Address reviewers comments ([#81](https://github.com/ascii-supply-networks/dagster-slurm/pull/81),
  [`e37d6fc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e37d6fc23476e1edf27ec0c2b259f6e6f7f8d2fe))

- Caching and cache invalidation
  ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Multi asset subselection ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- New testing ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Ray lint; feat: allow asset specific ENV overrides
  ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Reduce hasattr ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Refine for multi-asset ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Try fixing tests? ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

### Features

- Agent definitions ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Agent definitions ([#84](https://github.com/ascii-supply-networks/dagster-slurm/pull/84),
  [`983d6ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/983d6adcef8ed8bfa6fc64b5165c353aaf023794))

- Deps upgrades ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Deps upgrades ([#84](https://github.com/ascii-supply-networks/dagster-slurm/pull/84),
  [`983d6ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/983d6adcef8ed8bfa6fc64b5165c353aaf023794))

- Direct submission ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Enable TU Wien datalab ([#83](https://github.com/ascii-supply-networks/dagster-slurm/pull/83),
  [`5e4ad71`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5e4ad7168ddcb2b06edef451787e5cf939dfe623))

- Refine slides ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Refine slides ([#84](https://github.com/ascii-supply-networks/dagster-slurm/pull/84),
  [`983d6ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/983d6adcef8ed8bfa6fc64b5165c353aaf023794))

- Refine slides ([#81](https://github.com/ascii-supply-networks/dagster-slurm/pull/81),
  [`e37d6fc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e37d6fc23476e1edf27ec0c2b259f6e6f7f8d2fe))

- Refine slides v2 ([#84](https://github.com/ascii-supply-networks/dagster-slurm/pull/84),
  [`983d6ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/983d6adcef8ed8bfa6fc64b5165c353aaf023794))

- Tu datalab ([#83](https://github.com/ascii-supply-networks/dagster-slurm/pull/83),
  [`5e4ad71`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5e4ad7168ddcb2b06edef451787e5cf939dfe623))

- Up upgrade ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Up upgrade ([#84](https://github.com/ascii-supply-networks/dagster-slurm/pull/84),
  [`983d6ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/983d6adcef8ed8bfa6fc64b5165c353aaf023794))

- Upgrade deps ([#83](https://github.com/ascii-supply-networks/dagster-slurm/pull/83),
  [`5e4ad71`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5e4ad7168ddcb2b06edef451787e5cf939dfe623))

- Use prek ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

- Use prek ([#84](https://github.com/ascii-supply-networks/dagster-slurm/pull/84),
  [`983d6ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/983d6adcef8ed8bfa6fc64b5165c353aaf023794))

- Use ty instead of pyright ([#85](https://github.com/ascii-supply-networks/dagster-slurm/pull/85),
  [`97204ad`](https://github.com/ascii-supply-networks/dagster-slurm/commit/97204ad7ede6bbefd4db3048da42628d315ad294))

## v1.8.0 (2025-11-01)

## v1.7.0-rc.3 (2025-11-01)

### Bug Fixes

- Linting after upgrade ([#80](https://github.com/ascii-supply-networks/dagster-slurm/pull/80),
  [`48d976f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/48d976f953c848e54cf64edf26ebc7642f6d98c0))

### Features

- Change logo ([#79](https://github.com/ascii-supply-networks/dagster-slurm/pull/79),
  [`3845b2c`](https://github.com/ascii-supply-networks/dagster-slurm/commit/3845b2cf2c83884cdfe9bb12b001eb447e420a15))

- Logo ([#79](https://github.com/ascii-supply-networks/dagster-slurm/pull/79),
  [`3845b2c`](https://github.com/ascii-supply-networks/dagster-slurm/commit/3845b2cf2c83884cdfe9bb12b001eb447e420a15))

## v1.7.0-rc.2 (2025-11-01)

### Bug Fixes

- Use custom dns ([#78](https://github.com/ascii-supply-networks/dagster-slurm/pull/78),
  [`ec25dd8`](https://github.com/ascii-supply-networks/dagster-slurm/commit/ec25dd81a311ba4c487600e9295d1fa1e1565b0e))

### Features

- Change baseurl ([#78](https://github.com/ascii-supply-networks/dagster-slurm/pull/78),
  [`ec25dd8`](https://github.com/ascii-supply-networks/dagster-slurm/commit/ec25dd81a311ba4c487600e9295d1fa1e1565b0e))

- Refine telemetry ([#78](https://github.com/ascii-supply-networks/dagster-slurm/pull/78),
  [`ec25dd8`](https://github.com/ascii-supply-networks/dagster-slurm/commit/ec25dd81a311ba4c487600e9295d1fa1e1565b0e))

## v1.7.0-rc.1 (2025-11-01)

### Bug Fixes

- Ci mmanualy ([#74](https://github.com/ascii-supply-networks/dagster-slurm/pull/74),
  [`49f796a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/49f796ae4724330cdb78dae7ebc0aaf4edd783c9))

- Code layout ([#74](https://github.com/ascii-supply-networks/dagster-slurm/pull/74),
  [`49f796a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/49f796ae4724330cdb78dae7ebc0aaf4edd783c9))

### Chores

- Add resources
  ([`e139a89`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e139a89b673e0a87a69567505dcbf4011a1bce58))

- Readme
  ([`d89e12a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d89e12aae58b702fa04ec8463388b1ad33642247))

### Documentation

- Docs suggestions ([#73](https://github.com/ascii-supply-networks/dagster-slurm/pull/73),
  [`4c1df97`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4c1df97cd2e234803388e61d6e38d35f68b3a2fc))

### Features

- Also pypi ([#77](https://github.com/ascii-supply-networks/dagster-slurm/pull/77),
  [`0c6f80b`](https://github.com/ascii-supply-networks/dagster-slurm/commit/0c6f80b63edbf6eef608412bec508db6214df270))

- Analytics ([#77](https://github.com/ascii-supply-networks/dagster-slurm/pull/77),
  [`0c6f80b`](https://github.com/ascii-supply-networks/dagster-slurm/commit/0c6f80b63edbf6eef608412bec508db6214df270))

- Refine paper 1 ([#74](https://github.com/ascii-supply-networks/dagster-slurm/pull/74),
  [`49f796a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/49f796ae4724330cdb78dae7ebc0aaf4edd783c9))

## v1.7.0 (2025-10-23)

### Bug Fixes

- Refine slides ([#69](https://github.com/ascii-supply-networks/dagster-slurm/pull/69),
  [`50a054b`](https://github.com/ascii-supply-networks/dagster-slurm/commit/50a054bfe82f441942f84582142eeab6a68087af))

- Revert -see comment ([#65](https://github.com/ascii-supply-networks/dagster-slurm/pull/65),
  [`d9e39a0`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d9e39a036d4a542da47066d474cb5686c7c108ef))

### Chores

- After merge ([#65](https://github.com/ascii-supply-networks/dagster-slurm/pull/65),
  [`d9e39a0`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d9e39a036d4a542da47066d474cb5686c7c108ef))

### Features

- Issue 42 ([#65](https://github.com/ascii-supply-networks/dagster-slurm/pull/65),
  [`d9e39a0`](https://github.com/ascii-supply-networks/dagster-slurm/commit/d9e39a036d4a542da47066d474cb5686c7c108ef))

## v1.6.0-rc.3 (2025-10-23)

### Bug Fixes

- Refine readmes ([#67](https://github.com/ascii-supply-networks/dagster-slurm/pull/67),
  [`f06fd8e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/f06fd8e27c48742513f8e6e6450ed09740d3ec52))

### Features

- Refine 8 ([#67](https://github.com/ascii-supply-networks/dagster-slurm/pull/67),
  [`f06fd8e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/f06fd8e27c48742513f8e6e6450ed09740d3ec52))

## v1.6.0-rc.2 (2025-10-22)

### Chores

- Refine slides ([#66](https://github.com/ascii-supply-networks/dagster-slurm/pull/66),
  [`5825519`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5825519be0adc121414c364c6847d62ef4707866))

- Small docs refinement ([#63](https://github.com/ascii-supply-networks/dagster-slurm/pull/63),
  [`4a1ed64`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4a1ed6452163281728b0f396727ebc5166c1d8c6))

### Documentation

- Doc improvments ([#63](https://github.com/ascii-supply-networks/dagster-slurm/pull/63),
  [`4a1ed64`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4a1ed6452163281728b0f396727ebc5166c1d8c6))

### Features

- Refine 7 ([#66](https://github.com/ascii-supply-networks/dagster-slurm/pull/66),
  [`5825519`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5825519be0adc121414c364c6847d62ef4707866))

- Upgrade pixi; refine docs ([#66](https://github.com/ascii-supply-networks/dagster-slurm/pull/66),
  [`5825519`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5825519be0adc121414c364c6847d62ef4707866))

## v1.6.0-rc.1 (2025-10-22)

### Bug Fixes

- Lint refinement ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

- Only bind none for leonardo seems to work
  ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

- Ray for leonardo and CPU binding more
  ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

- Refine logo ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

### Chores

- Refine docs, slides ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

- Refine paper ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

- Refine text ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

### Features

- Nobinding for leonardo ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

- Refine 6 ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

- Upgrade pixi ([#60](https://github.com/ascii-supply-networks/dagster-slurm/pull/60),
  [`7114b12`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7114b1271e91b7833633c2cd504577103761d1c4))

## v1.6.0 (2025-10-21)

## v1.5.0-rc.3 (2025-10-21)

### Bug Fixes

- CI even more fixing ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Ci more ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Explore fixing ssh tests ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Interactive session ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Make tests green again ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Message reader (local) ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- No gpus in dev queue on VSC5
  ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine CI ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine CI more ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine execution on VSC5 ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine leonardo configs ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Reinfe message reading (a bit not yet fully)
  ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Still failing, but now better error of missing budget
  ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Test more ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Tests in CI ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- VSC5 ssh connection ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

### Chores

- Begin exploring leonardo ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Cleanup, disable debug mode
  ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine docs ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

### Features

- Explore leonardo ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Make jump host approach more flexible
  ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Package upgrades ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Prep launch for real HPC systems
  ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine 5 ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine docs ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine paper ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Refine SSH further for VSC5
  ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

- Use other more free nodes ([#58](https://github.com/ascii-supply-networks/dagster-slurm/pull/58),
  [`444e2d9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/444e2d928b1588ed70b54be1d4f65996991ae657))

## v1.5.0-rc.2 (2025-10-17)

### Bug Fixes

- Ssh controlmaster fallback to plain if permission denied
  ([#55](https://github.com/ascii-supply-networks/dagster-slurm/pull/55),
  [`c9c91fe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c9c91fe6d365c12af37a4ed96e992ae6fa2ed8e1))

### Features

- Refine 4 ([#55](https://github.com/ascii-supply-networks/dagster-slurm/pull/55),
  [`c9c91fe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c9c91fe6d365c12af37a4ed96e992ae6fa2ed8e1))

- Refine docs ([#55](https://github.com/ascii-supply-networks/dagster-slurm/pull/55),
  [`c9c91fe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c9c91fe6d365c12af37a4ed96e992ae6fa2ed8e1))

## v1.5.0-rc.1 (2025-10-15)

### Features

- Add new key of Hernan ([#53](https://github.com/ascii-supply-networks/dagster-slurm/pull/53),
  [`5c474dc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5c474dcd02712ded54aeabb574af0aefa66c68ba))

- Refine 3 ([#53](https://github.com/ascii-supply-networks/dagster-slurm/pull/53),
  [`5c474dc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5c474dcd02712ded54aeabb574af0aefa66c68ba))

- Refine docs ([#53](https://github.com/ascii-supply-networks/dagster-slurm/pull/53),
  [`5c474dc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5c474dcd02712ded54aeabb574af0aefa66c68ba))

- Showcase asset tests; standardize on default pipes client protocol
  ([#53](https://github.com/ascii-supply-networks/dagster-slurm/pull/53),
  [`5c474dc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5c474dcd02712ded54aeabb574af0aefa66c68ba))

## v1.5.0 (2025-10-12)

## v1.4.0-rc.1 (2025-10-12)

### Features

- Enable CI ([#38](https://github.com/ascii-supply-networks/dagster-slurm/pull/38),
  [`df3bf3f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/df3bf3f2b84d68134214296e9545c914e204714c))

- Refine 2 ([#38](https://github.com/ascii-supply-networks/dagster-slurm/pull/38),
  [`df3bf3f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/df3bf3f2b84d68134214296e9545c914e204714c))

## v1.4.0 (2025-10-08)

## v1.3.1-rc.1 (2025-10-08)

### Bug Fixes

- Small fixes from review ([#24](https://github.com/ascii-supply-networks/dagster-slurm/pull/24),
  [`6c64099`](https://github.com/ascii-supply-networks/dagster-slurm/commit/6c64099c1c14c88072bbfb977b462b0419fbb27a))

### Chores

- Dependency upgrades ([#23](https://github.com/ascii-supply-networks/dagster-slurm/pull/23),
  [`09a70d2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/09a70d2622897c22fa5b11a2e735b04ae6724923))

- Fix typo ([#23](https://github.com/ascii-supply-networks/dagster-slurm/pull/23),
  [`09a70d2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/09a70d2622897c22fa5b11a2e735b04ae6724923))

- Improve image ([#23](https://github.com/ascii-supply-networks/dagster-slurm/pull/23),
  [`09a70d2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/09a70d2622897c22fa5b11a2e735b04ae6724923))

- Make license more clear ([#23](https://github.com/ascii-supply-networks/dagster-slurm/pull/23),
  [`09a70d2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/09a70d2622897c22fa5b11a2e735b04ae6724923))

- Refine slide ([#23](https://github.com/ascii-supply-networks/dagster-slurm/pull/23),
  [`09a70d2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/09a70d2622897c22fa5b11a2e735b04ae6724923))

- Update deps ([#24](https://github.com/ascii-supply-networks/dagster-slurm/pull/24),
  [`6c64099`](https://github.com/ascii-supply-networks/dagster-slurm/commit/6c64099c1c14c88072bbfb977b462b0419fbb27a))

- Use dark ([#23](https://github.com/ascii-supply-networks/dagster-slurm/pull/23),
  [`09a70d2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/09a70d2622897c22fa5b11a2e735b04ae6724923))

### Features

- Apply injection ([#24](https://github.com/ascii-supply-networks/dagster-slurm/pull/24),
  [`6c64099`](https://github.com/ascii-supply-networks/dagster-slurm/commit/6c64099c1c14c88072bbfb977b462b0419fbb27a))

- Build SLURM integration for dagster
  ([#19](https://github.com/ascii-supply-networks/dagster-slurm/pull/19),
  [`b77c30a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/b77c30ad26976aab34f3689ff1ac1e8e5e9d24c6))

- Dedicated external workload package, more clarity of where what is executed
  ([#23](https://github.com/ascii-supply-networks/dagster-slurm/pull/23),
  [`09a70d2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/09a70d2622897c22fa5b11a2e735b04ae6724923))

- Inject hpc workload ([#24](https://github.com/ascii-supply-networks/dagster-slurm/pull/24),
  [`6c64099`](https://github.com/ascii-supply-networks/dagster-slurm/commit/6c64099c1c14c88072bbfb977b462b0419fbb27a))

- Restructure ([#23](https://github.com/ascii-supply-networks/dagster-slurm/pull/23),
  [`09a70d2`](https://github.com/ascii-supply-networks/dagster-slurm/commit/09a70d2622897c22fa5b11a2e735b04ae6724923))

## v1.3.1 (2025-08-25)

## v1.3.0-rc.1 (2025-08-25)

### Bug Fixes

- Fix version increase for metadata
  ([`15d18bf`](https://github.com/ascii-supply-networks/dagster-slurm/commit/15d18bfe6247cd7e8212e411b9fb64622217cd3f))

### Chores

- Re-trigger CI
  ([`93e0200`](https://github.com/ascii-supply-networks/dagster-slurm/commit/93e0200bd3b40d2daeb4dd83a6244f4a64c37e02))

## v1.3.0 (2025-08-25)

## v1.2.0-rc.5 (2025-08-25)

### Chores

- Re-trigger CI
  ([`c93d035`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c93d0355dca5bf5971327dbb274b9e295602be4e))

## v1.2.0-rc.4 (2025-08-25)

### Chores

- Re-trigger CI
  ([`efcc128`](https://github.com/ascii-supply-networks/dagster-slurm/commit/efcc12885c1f9bb2f42839b3d454232aaaa3fb9d))

## v1.2.0-rc.3 (2025-08-25)

### Chores

- Re-trigger
  ([`5f18dda`](https://github.com/ascii-supply-networks/dagster-slurm/commit/5f18ddac5454235759b0746a539e1ce5c6f1509b))

- Re-trigger CI
  ([`0a7f1a9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/0a7f1a9f2bc348db3dbd28d8550c2b079fd4a738))

## v1.2.0-rc.2 (2025-08-25)

### Bug Fixes

- Add autoformatter for pre-commit
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Clean path ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Increase timeout ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Use frozen ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

### Chores

- Change port to 2223 ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Explore packaging of shared - fail to bdist it
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Pixi update ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Refine cleanup ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Refine documentation as per zach`s review
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Refine tests ignore beta warnings
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Try to package scripts ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Upgrade uv ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

### Features

- Add pixi to slides ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Add slides (mechanics) ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Add slurm container, refactor to dedicated shared library for the examples
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Basic slides blueprint ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Creating new dedicated integration packages
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Enable submission of ray to (local) slurm
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- More timeout ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Package aarch, instructions for copying stuff over
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Packaging of shared library
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Refine docs ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Refinments and cleanup ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

- Use dedicated shared volume - ./z_state/data_and_logs:/data_and_logs
  ([#20](https://github.com/ascii-supply-networks/dagster-slurm/pull/20),
  [`120b239`](https://github.com/ascii-supply-networks/dagster-slurm/commit/120b239b61b74b7703be9a4dc548692ff004be34))

## v1.2.0-rc.1 (2025-08-11)

### Chores

- Re-trigger
  ([`1bfff5f`](https://github.com/ascii-supply-networks/dagster-slurm/commit/1bfff5f5b2cca44c8c4ec2d75fda0d32e8f67b88))

- Use test pypi for preview deployments
  ([#17](https://github.com/ascii-supply-networks/dagster-slurm/pull/17),
  [`7b7d9dc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7b7d9dc092e50f7552f44d6dec9438a84181a48f))

### Features

- Demo fully enable dagster components
  ([#17](https://github.com/ascii-supply-networks/dagster-slurm/pull/17),
  [`7b7d9dc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7b7d9dc092e50f7552f44d6dec9438a84181a48f))

- Refine ([#17](https://github.com/ascii-supply-networks/dagster-slurm/pull/17),
  [`7b7d9dc`](https://github.com/ascii-supply-networks/dagster-slurm/commit/7b7d9dc092e50f7552f44d6dec9438a84181a48f))

## v1.2.0 (2025-08-10)

## v1.1.3-rc.3 (2025-08-10)

### Chores

- Refine readme
  ([`6c743fe`](https://github.com/ascii-supply-networks/dagster-slurm/commit/6c743fe98f95ff6752d350e611b3334ffd0e6183))

## v1.1.3-rc.2 (2025-08-10)

### Chores

- Grant more permissions ([#16](https://github.com/ascii-supply-networks/dagster-slurm/pull/16),
  [`53363ee`](https://github.com/ascii-supply-networks/dagster-slurm/commit/53363ee390384d2421c2be5df566f9d049e90620))

- Refine basic docs ([#16](https://github.com/ascii-supply-networks/dagster-slurm/pull/16),
  [`53363ee`](https://github.com/ascii-supply-networks/dagster-slurm/commit/53363ee390384d2421c2be5df566f9d049e90620))

- Trigger more often ([#16](https://github.com/ascii-supply-networks/dagster-slurm/pull/16),
  [`53363ee`](https://github.com/ascii-supply-networks/dagster-slurm/commit/53363ee390384d2421c2be5df566f9d049e90620))

### Features

- Refine basic docs ([#16](https://github.com/ascii-supply-networks/dagster-slurm/pull/16),
  [`53363ee`](https://github.com/ascii-supply-networks/dagster-slurm/commit/53363ee390384d2421c2be5df566f9d049e90620))

## v1.1.3-rc.1 (2025-08-10)

### Bug Fixes

- Test version increment
  ([`9cdb848`](https://github.com/ascii-supply-networks/dagster-slurm/commit/9cdb848a61557b011975be9cd91c56faef00414b))

## v1.1.3 (2025-08-10)

## v1.1.2-rc.2 (2025-08-10)

### Chores

- Re-try clean overall version
  ([`dbe3ca7`](https://github.com/ascii-supply-networks/dagster-slurm/commit/dbe3ca7a4f1549f8d97079ec400d51d69979fde2))

## v1.1.2-rc.1 (2025-08-10)

### Bug Fixes

- Cleanup
  ([`40d9431`](https://github.com/ascii-supply-networks/dagster-slurm/commit/40d94315db25d25c7f6c8d9c2e9a4c337017661b))

## v1.1.2 (2025-08-10)

## v1.1.1-rc.1 (2025-08-10)

### Bug Fixes

- Refine automated full release
  ([`c3dd953`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c3dd953cdf2a91377664299126d165a4eae7f3c9))

## v1.1.1 (2025-08-10)

## v1.1.0-rc.5 (2025-08-10)

### Chores

- Fix full release automation
  ([`55268b0`](https://github.com/ascii-supply-networks/dagster-slurm/commit/55268b0569d128f5f5858817092e33d41c272124))

## v1.1.0-rc.4 (2025-08-10)

### Bug Fixes

- Update lockfile version
  ([`f59d682`](https://github.com/ascii-supply-networks/dagster-slurm/commit/f59d6826974013b352db339d4d3080ed24c284f8))

## v1.1.0-rc.3 (2025-08-10)

### Bug Fixes

- Versions
  ([`ee1735e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/ee1735e199008231de751e0f3fd129eba9e0b1f8))

### Chores

- Update dagster-slurm lockfile entry [skip ci]
  ([`ed9661d`](https://github.com/ascii-supply-networks/dagster-slurm/commit/ed9661d66c98fe86afa7dd3018a1faf76c6a360f))

## v1.1.0-rc.2 (2025-08-10)

### Bug Fixes

- Full-retrigger
  ([`0630685`](https://github.com/ascii-supply-networks/dagster-slurm/commit/06306850446b8a5e8ba35097e586a4bb57508344))

### Chores

- Update dagster-slurm lockfile entry [skip ci]
  ([`c51f115`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c51f1158e823118deebbeaa3df3bae23c7aaff5c))

## v1.1.0-rc.1 (2025-08-10)

### Chores

- Improve readme with metadata
  ([`8bc2206`](https://github.com/ascii-supply-networks/dagster-slurm/commit/8bc220621dff08d70bda1d9ecd58268ac002c8b3))

- Locked vs frozen install
  ([`2f48f43`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2f48f433e9b0421d337eadc2a4f3256557544521))

- Update dagster-slurm lockfile entry [skip ci]
  ([`3721baf`](https://github.com/ascii-supply-networks/dagster-slurm/commit/3721baf3b15e1fe5b9ee855b3f49628aab3a668d))

## v1.1.0 (2025-08-10)

### Chores

- Update dagster-slurm lockfile entry [skip ci]
  ([`3e297ab`](https://github.com/ascii-supply-networks/dagster-slurm/commit/3e297abcc3702a6505a70ec8bf5d18d2c24253a1))

## v1.0.0-rc.3 (2025-08-10)

### Bug Fixes

- Upgrade lockfile as well
  ([`445e57e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/445e57e194d7965e8505ff2e4643f284a2f35f14))

### Chores

- Re-trigger
  ([`c159264`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c1592642657272641791e8b27784e8ab47e842f5))

### Features

- Enable skip-ci
  ([`88ac37a`](https://github.com/ascii-supply-networks/dagster-slurm/commit/88ac37a4a87def157bae1c96d1fb49a1fa09e6ee))

## v1.0.0-rc.2 (2025-08-10)

### Bug Fixes

- Automate-release
  ([`9b4915e`](https://github.com/ascii-supply-networks/dagster-slurm/commit/9b4915ea12a507c527f81f1a11fe42920cff18ac))

### Chores

- Refresh lockfile
  ([`3c14cd9`](https://github.com/ascii-supply-networks/dagster-slurm/commit/3c14cd91ca46d22bdc54140f2fb92fd92ec2d885))

## v1.0.0-rc.1 (2025-08-10)

### Bug Fixes

- Release and push
  ([`c718690`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c718690add213bed2f90a1782f8a11d0c7a6ff5b))

## v1.0.0 (2025-08-10)

## v0.0.1-rc.7 (2025-08-10)

### Bug Fixes

- Main release no automatic rewrite -> idempotency
  ([`dc3db38`](https://github.com/ascii-supply-networks/dagster-slurm/commit/dc3db381762484b96abb7d6abe80f6ec099d7eae))

## v0.0.1-rc.6 (2025-08-10)

### Chores

- Re-trigger
  ([`55f2ce8`](https://github.com/ascii-supply-networks/dagster-slurm/commit/55f2ce8073c0e2512848164d16e9482264416af7))

## v0.0.1-rc.5 (2025-08-10)

## v0.0.1-rc.4 (2025-08-10)

### Chores

- Fix pre-release to pypi write permissions
  ([`e61c18c`](https://github.com/ascii-supply-networks/dagster-slurm/commit/e61c18c6cc21dba607fbb455d9eb0493ac814f28))

## v0.0.1-rc.3 (2025-08-10)

### Features

- Publish pre-release to pypi
  ([`fcdfc66`](https://github.com/ascii-supply-networks/dagster-slurm/commit/fcdfc66d77e36a271ab5238d1b9f0f7e95785d66))

## v0.0.1-rc.2 (2025-08-10)

### Chores

- Automating semantic release
  ([`4e56b52`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4e56b52bc54d2b3e8c4b1a5ab52a28b66cfa612e))

## v0.0.1-rc.1 (2025-08-10)

### Bug Fixes

- Set up GHA permissions for semantic publishing
  ([`4470c45`](https://github.com/ascii-supply-networks/dagster-slurm/commit/4470c45a2d31c162a3468f571b7529d3fbe45f52))

### Chores

- Fix app registration
  ([`bdd3322`](https://github.com/ascii-supply-networks/dagster-slurm/commit/bdd33224b94c2c4ef10dff96a6510cdd09a2d49b))

- Re-trigger
  ([`c282945`](https://github.com/ascii-supply-networks/dagster-slurm/commit/c28294518a5613e5ef6262984d6095a9ff7b714b))

- Re-trigger CI
  ([`b499510`](https://github.com/ascii-supply-networks/dagster-slurm/commit/b4995108c999e1d728b0745b0f497dabaa7801cd))

### Features

- Automate release process ([#15](https://github.com/ascii-supply-networks/dagster-slurm/pull/15),
  [`2edaee5`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2edaee59ce83ecaf4c6d2107b31bfdb17068f22c))

- Basic setup ([#15](https://github.com/ascii-supply-networks/dagster-slurm/pull/15),
  [`2edaee5`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2edaee59ce83ecaf4c6d2107b31bfdb17068f22c))

- Commitizen setup ([#15](https://github.com/ascii-supply-networks/dagster-slurm/pull/15),
  [`2edaee5`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2edaee59ce83ecaf4c6d2107b31bfdb17068f22c))

- Initial example ([#15](https://github.com/ascii-supply-networks/dagster-slurm/pull/15),
  [`2edaee5`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2edaee59ce83ecaf4c6d2107b31bfdb17068f22c))

- Setup of semantic versioning
  ([#15](https://github.com/ascii-supply-networks/dagster-slurm/pull/15),
  [`2edaee5`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2edaee59ce83ecaf4c6d2107b31bfdb17068f22c))

- Title from branch ([#15](https://github.com/ascii-supply-networks/dagster-slurm/pull/15),
  [`2edaee5`](https://github.com/ascii-supply-networks/dagster-slurm/commit/2edaee59ce83ecaf4c6d2107b31bfdb17068f22c))

## v0.0.1 (2025-08-10)

- Initial Release
