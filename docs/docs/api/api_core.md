# API Reference

This is the auto-generated API reference for the `dagster-slurm` library.
It is generated directly from the docstrings in the Python source code.

## Core

This section covers the main components of the library.

<a id="module-dagster_slurm"></a>

Dagster Slurm Integration.

Run Dagster assets on Slurm clusters with support for:

- Local dev mode (no SSH/Slurm)
- Per-asset Slurm submission (staging)
- Session mode with operator fusion (production)
- Multiple launchers (Bash, Ray, Spark)

### *class* dagster_slurm.BashLauncher(\*\*data)

Bases: `ComputeLauncher`

Executes Python scripts via bash.

Uses the self-contained pixi environment extracted at runtime.
Sources the activation script provided by pixi-pack.

* **Parameters:**
  **data** (`Any`)

#### prepare_execution(payload_path, python_executable, working_dir, pipes_context, extra_env=None, allocation_context=None, activation_script=None)

Generate bash execution plan.

* **Parameters:**
  * **payload_path** (`str`) – Path to Python script on remote host
  * **python_executable** (`str`) – Python from extracted environment
  * **working_dir** (`str`) – Working directory
  * **pipes_context** (`Dict`[`str`, `str`]) – Dagster Pipes environment variables
  * **extra_env** (`Optional`[`Dict`[`str`, `str`]]) – Additional environment variables
  * **allocation_context** (`Optional`[`Dict`[`str`, `Any`]]) – Slurm allocation info (for session mode)
  * **activation_script** (`Optional`[`str`]) – Path to activation script (provided by pixi-pack)
* **Return type:**
  `ExecutionPlan`
* **Returns:**
  ExecutionPlan with shell script

### *class* dagster_slurm.ComputeResource(\*\*data)

Bases: `ConfigurableResource`

Unified compute resource - adapts to deployment.

This is the main facade that assets depend on.
Hides complexity of local vs Slurm vs session execution.

Usage:
: @asset
  def my_asset(context: AssetExecutionContext, compute: ComputeResource):
  <br/>
  > return compute.run(
  > : context=context,
  >   payload_path=”script.py”,
  >   launcher=RayLauncher(num_gpus_per_node=2)
  <br/>
  > )

Configuration Examples:

Local mode (dev):
: compute = ComputeResource(mode=”local”)

Slurm per-asset mode (staging):
: slurm = SlurmResource.from_env()
  compute = ComputeResource(mode=”slurm”, slurm=slurm)

Slurm session mode with cluster reuse (prod):
: slurm = SlurmResource.from_env()
  session = SlurmSessionResource(slurm=slurm, num_nodes=10)
  compute = ComputeResource(
  <br/>
  > mode=”slurm-session”,
  > slurm=slurm,
  > session=session,
  > enable_cluster_reuse=True,
  > cluster_reuse_tolerance=0.2,
  <br/>
  )

Heterogeneous job mode (optimal resource allocation):
: compute = ComputeResource(
  : mode=”slurm-hetjob”,
    slurm=slurm,
  <br/>
  )

* **Parameters:**
  **data** (`Any`)

#### auto_detect_platform *: `bool`*

#### cleanup_on_failure *: `bool`*

#### cluster_reuse_tolerance *: `float`*

#### debug_mode *: `bool`*

#### default_launcher *: `Optional`[`ComputeLauncher`]*

#### enable_cluster_reuse *: `bool`*

#### get_pipes_client(context, launcher=None)

Get appropriate Pipes client for this mode.

* **Parameters:**
  * **context** (`InitResourceContext`) – Dagster resource context
  * **launcher** (`Optional`[`ComputeLauncher`]) – Override launcher (uses default if None)
* **Returns:**
  LocalPipesClient or SlurmPipesClient

#### mode *: `ExecutionMode`*

#### model_post_init(\_ComputeResource_\_context)

Post-init setup - runs after Pydantic init.

#### pack_platform *: `Optional`[`str`]*

#### register_cluster(cluster_address, framework, cpus, gpus, memory_gb)

Register a newly created cluster for future reuse.

* **Parameters:**
  * **cluster_address** (`str`) – Address of the cluster (e.g., “10.0.0.1:6379”)
  * **framework** (`str`) – “ray” or “spark”
  * **cpus** (`int`) – Total CPUs in cluster
  * **gpus** (`int`) – Total GPUs in cluster
  * **memory_gb** (`int`) – Total memory in GB

#### run(context, payload_path, launcher=None, extra_slurm_opts=None, resource_requirements=None, \*\*kwargs)

Execute asset with optional resource overrides.

* **Parameters:**
  * **context** – Dagster execution context
  * **payload_path** (`str`) – Path to Python script
  * **launcher** (`Optional`[`ComputeLauncher`]) – Override launcher for this asset
  * **extra_slurm_opts** (`Optional`[`Dict`[`str`, `Any`]]) – Override Slurm options (non-session mode)
    - nodes: int
    - cpus_per_task: int
    - mem: str (e.g., “32G”)
    - gpus_per_node: int
    - time_limit: str (e.g., “02:00:00”)
  * **resource_requirements** (`Optional`[`Dict`[`str`, `Any`]]) – Resource requirements for cluster reuse (session mode)
    - cpus: int
    - gpus: int
    - memory_gb: int
    - framework: str (“ray” or “spark”)
  * **\*\*kwargs** – Passed to client.run()
* **Yields:**
  Dagster events

### Examples

```python
# Simple execution with default resources
yield from compute.run(context, "script.py")
```

```python
# Override launcher for this asset
ray_launcher = RayLauncher(num_gpus_per_node=4)
yield from compute.run(context, "script.py", launcher=ray_launcher)
```

```python
# Non-session mode: override Slurm resources
yield from compute.run(
    context,
    "script.py",
    extra_slurm_opts={"nodes": 1, "cpus_per_task": 16, "mem": "64G", "gpus_per_node": 2}
)
```

```python
# Session mode: specify resource requirements for cluster reuse
yield from compute.run(
    context,
    "script.py",
    launcher=RayLauncher(num_gpus_per_node=2),
    resource_requirements={"cpus": 32, "gpus": 2, "memory_gb": 128, "framework": "ray"}
)
```

#### run_hetjob(context, assets, launchers=None)

Run multiple assets as a heterogeneous Slurm job.

Submit all assets together with their specific resource requirements.
Only waits in queue ONCE, but each asset gets the resources it needs.

* **Parameters:**
  * **context** – Dagster execution context
  * **assets** (`List`[`Tuple`[`str`, `str`, `Dict`[`str`, `Any`]]]) – 

    List of (asset_key, payload_path, resource_requirements)
    resource_requirements:
    > - nodes: int (default: 1)
    > - cpus_per_task: int (default: 2)
    > - mem: str (default: “4G”)
    > - gpus_per_node: int (default: 0)
    > - time_limit: str (default: “01:00:00”)
  * **launchers** (`Optional`[`Dict`[`str`, `ComputeLauncher`]]) – Optional dict mapping asset_key to ComputeLauncher
* **Yields:**
  Dagster events

### Example

```python
compute.run_hetjob(
    context,
    assets=[
        ("prep", "prep.py", {"nodes": 1, "cpus_per_task": 8, "mem": "32G"}),
        ("train", "train.py", {"nodes": 4, "cpus_per_task": 32, "mem": "128G", "gpus_per_node": 2}),
        ("infer", "infer.py", {"nodes": 8, "cpus_per_task": 16, "mem": "64G", "gpus_per_node": 1}),
    ],
    launchers={
        "train": RayLauncher(num_gpus_per_node=2),
        "infer": RayLauncher(num_gpus_per_node=1),
    }
)
```

#### session *: `Optional`[[`SlurmSessionResource`](#id62)]*

#### slurm *: `Optional`[[`SlurmResource`](#id30)]*

#### teardown(context)

Teardown method called by Dagster at end of run.
Ensures session resources and clusters are cleaned up.

* **Parameters:**
  **context** (`InitResourceContext`)

#### validate_configuration()

Validate configuration - runs during Pydantic validation.

* **Return type:**
  [`ComputeResource`](#id0)

### *class* dagster_slurm.LocalPipesClient(launcher, base_dir='/tmp/dagster_local_runs', require_pixi=True)

Bases: `PipesClient`

Pipes client for local execution (dev mode).
No SSH, no Slurm - just runs scripts locally via subprocess.

* **Parameters:**
  * **launcher** (`ComputeLauncher`)
  * **base_dir** (`str`)
  * **require_pixi** (`bool`)

#### run(context, , payload_path, python_executable=None, extra_env=None, extras=None, extra_slurm_opts=None)

Execute payload locally.

* **Parameters:**
  * **context** (`AssetExecutionContext`) – Dagster execution context
  * **payload_path** (`str`) – Path to Python script to execute
  * **python_executable** (`Optional`[`str`]) – Python interpreter (defaults to current)
  * **extra_env** (`Optional`[`Dict`[`str`, `str`]]) – Additional environment variables
  * **extras** (`Optional`[`Dict`[`str`, `Any`]]) – Extra data to pass via Pipes
  * **extra_slurm_opts** (`Optional`[`Dict`[`str`, `Any`]])
* **Yields:**
  Dagster events (materializations, logs, etc.)
* **Return type:**
  `Iterator`

### *class* dagster_slurm.RayLauncher(\*\*data)

Bases: `ComputeLauncher`

Ray distributed computing launcher.

Features:
- Robust cluster startup with sentinel-based shutdown
- Graceful cleanup on SIGTERM/SIGINT
- Worker registration monitoring
- Automatic head node detection
- IPv4/IPv6 normalization

Modes:
- Local: Single-node Ray
- Cluster: Multi-node Ray cluster across Slurm allocation (via allocation_context)
- Connect: Connect to existing cluster (via ray_address)

* **Parameters:**
  **data** (`Any`)

#### dashboard_port *: `int`*

#### grace_period *: `int`*

#### head_startup_timeout *: `int`*

#### num_gpus_per_node *: `int`*

#### object_store_memory_gb *: `Optional`[`int`]*

#### prepare_execution(payload_path, python_executable, working_dir, pipes_context, extra_env=None, allocation_context=None, activation_script=None)

Generate Ray execution plan.

* **Parameters:**
  * **payload_path** (`str`)
  * **python_executable** (`str`)
  * **working_dir** (`str`)
  * **pipes_context** (`Dict`[`str`, `str`])
  * **extra_env** (`Optional`[`Dict`[`str`, `str`]])
  * **allocation_context** (`Optional`[`Dict`[`str`, `Any`]])
  * **activation_script** (`Optional`[`str`])
* **Return type:**
  `ExecutionPlan`

#### ray_address *: `Optional`[`str`]*

#### ray_port *: `int`*

#### redis_password *: `Optional`[`str`]*

#### worker_startup_delay *: `int`*

### *class* dagster_slurm.SSHConnectionResource(\*\*data)

Bases: `ConfigurableResource`

SSH connection settings.

Supports two authentication methods:
1. SSH key (recommended for automation)
2. Password (for interactive use or when keys unavailable)

Either key_path OR password must be provided (not both).

### Examples

```python
# Key-based auth
ssh = SSHConnectionResource(
    host="cluster.example.com",
    user="username",
    key_path="~/.ssh/id_rsa",
)
```

```python
# Password-based auth
ssh = SSHConnectionResource(
    host="cluster.example.com",
    user="username",
    password="secret123",
)
```

```python
```

# From environment variables
ssh = SSHConnectionResource.from_env()

* **Parameters:**
  **data** (`Any`)

#### extra_opts *: `List`[`str`]*

#### *classmethod* from_env(prefix='SLURM_SSH')

Create from environment variables.

Environment variables:
: ```
  ``
  ```
  <br/>
  {prefix}\`\`_HOST - SSH hostname (required)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_PORT - SSH port (optional, default: 22)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_USER - SSH username (required)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_KEY - Path to SSH key (optional)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_PASSWORD - SSH password (optional)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_OPTS_EXTRA - Additional SSH options (optional)

Either KEY or PASSWORD must be set (not both).

* **Parameters:**
  **prefix** (`str`) – Environment variable prefix (default: “SLURM_SSH”)
* **Return type:**
  [`SSHConnectionResource`](#id36)
* **Returns:**
  SSHConnectionResource instance
* **Raises:**
  **ValueError** – If required variables missing or both auth methods specified

Example:

```bash
export SLURM_SSH_HOST=cluster.example.com
export SLURM_SSH_USER=username
export SLURM_SSH_KEY=~/.ssh/id_rsa
# OR
export SLURM_SSH_PASSWORD=secret123
```

…

#### get_remote_target()

Get the remote target string for SCP commands.

* **Return type:**
  `str`
* **Returns:**
  String in format [‘user@host](mailto:'user@host)’

### Example

[‘username@cluster.example.com](mailto:'username@cluster.example.com)’

#### get_scp_base_command()

Build base SCP command for file transfers.

* **Return type:**
  `List`[`str`]
* **Returns:**
  List of command arguments (without source/dest)

#### NOTE
For password authentication, password handling is done via pexpect.

### Example

[‘scp’, ‘-P’, ‘22’, ‘-i’, ‘/path/to/key’]
# OR (password auth)
[‘scp’, ‘-P’, ‘22’]

#### get_ssh_base_command()

Build base SSH command for subprocess.

* **Return type:**
  `List`[`str`]
* **Returns:**
  List of command arguments for subprocess.run()

#### NOTE
For password authentication, this returns the base command.
Password handling is done separately via pexpect in SSHConnectionPool.

This method is used by SSHMessageReader which uses ControlMaster,
so password auth is already handled by the ControlMaster connection.

### Example

[‘ssh’, ‘-p’, ‘22’, ‘-i’, ‘/path/to/key’, [‘user@host](mailto:'user@host)’]
# OR (password auth via ControlMaster)
[‘ssh’, ‘-p’, ‘22’, ‘-o’, ‘ControlPath=/tmp/…’, [‘user@host](mailto:'user@host)’]

#### host *: `str`*

#### key_path *: `Optional`[`str`]*

#### password *: `Optional`[`str`]*

#### port *: `int`*

#### user *: `str`*

#### *property* uses_key_auth *: bool*

Returns True if using key-based authentication.

#### *property* uses_password_auth *: bool*

Returns True if using password-based authentication.

#### validate_auth_method()

Ensure exactly one authentication method is provided.

### *class* dagster_slurm.SlurmAllocation(slurm_job_id, nodes, working_dir, config)

Bases: `object`

Represents a running Slurm allocation.

* **Parameters:**
  * **slurm_job_id** (`int`)
  * **nodes** (`List`[`str`])
  * **working_dir** (`str`)
  * **config** ([`SlurmSessionResource`](#id62))

#### cancel(ssh_pool)

Cancel the allocation.

* **Parameters:**
  **ssh_pool** (`SSHConnectionPool`)

#### execute(execution_plan, asset_key, ssh_pool)

Execute plan in this allocation via srun.

* **Parameters:**
  * **execution_plan** (`ExecutionPlan`)
  * **asset_key** (`str`)
  * **ssh_pool** (`SSHConnectionPool`)
* **Return type:**
  `int`

#### get_failed_nodes()

Get list of failed nodes.

* **Return type:**
  `List`[`str`]

#### is_healthy(ssh_pool)

Check if allocation and nodes are healthy.

* **Parameters:**
  **ssh_pool** (`SSHConnectionPool`)
* **Return type:**
  `bool`

### *class* dagster_slurm.SlurmPipesClient(slurm_resource, launcher, session_resource=None, cleanup_on_failure=True, debug_mode=False, auto_detect_platform=True, pack_platform=None)

Bases: `PipesClient`

Pipes client for Slurm execution with real-time log streaming and cancellation support.

Features:
- Real-time stdout/stderr streaming to Dagster logs
- Packaging environment with pixi pack
- Auto-reconnect message reading
- Metrics collection
- Graceful cancellation with Slurm job termination

Works in two modes:
1. Standalone: Each asset = separate sbatch job
2. Session: Multiple assets share a Slurm allocation (operator fusion)

* **Parameters:**
  * **slurm_resource** ([`SlurmResource`](#id30))
  * **launcher** (`ComputeLauncher`)
  * **session_resource** (`Optional`[[`SlurmSessionResource`](#id62)])
  * **cleanup_on_failure** (`bool`)
  * **debug_mode** (`bool`)
  * **auto_detect_platform** (`bool`)
  * **pack_platform** (`Optional`[`str`])

#### run(context, , payload_path, extra_env=None, extras=None, use_session=False, extra_slurm_opts=None, \*\*kwargs)

Execute payload on Slurm cluster with real-time log streaming.

* **Parameters:**
  * **context** (`AssetExecutionContext`) – Dagster execution context
  * **payload_path** (`str`) – Local path to Python script
  * **launcher** – Ignored (launcher is set at client construction time)
  * **extra_env** (`Optional`[`Dict`[`str`, `str`]]) – Additional environment variables
  * **extras** (`Optional`[`Dict`[`str`, `Any`]]) – Extra data to pass via Pipes
  * **use_session** (`bool`) – If True and session_resource provided, use shared allocation
  * **extra_slurm_opts** (`Optional`[`Dict`[`str`, `Any`]]) – Override Slurm options (non-session mode)
  * **\*\*kwargs** – Additional arguments (ignored, for forward compatibility)
* **Yields:**
  Dagster events
* **Return type:**
  `Iterator`

### *class* dagster_slurm.SlurmQueueConfig(\*\*data)

Bases: `ConfigurableResource`

Default Slurm job submission parameters.
These can be overridden per-asset via metadata or function arguments.

* **Parameters:**
  **data** (`Any`)

#### cpus *: `int`*

#### gpus_per_node *: `int`*

#### mem *: `str`*

#### mem_per_cpu *: `str`*

#### num_nodes *: `int`*

#### partition *: `str`*

#### time_limit *: `str`*

### *class* dagster_slurm.SlurmResource(\*\*data)

Bases: `ConfigurableResource`

Complete Slurm cluster configuration.
Combines SSH connection, queue defaults, and cluster-specific paths.

* **Parameters:**
  **data** (`Any`)

#### *classmethod* from_env()

Create from environment variables.

* **Return type:**
  [`SlurmResource`](#id30)

#### *classmethod* from_env_slurm(ssh)

Create a SlurmResource by populating most fields from environment variables,
but requires an explicit, pre-configured SSHConnectionResource to be provided.

* **Parameters:**
  **ssh** ([`SSHConnectionResource`](#id36)) – A fully configured SSHConnectionResource instance.
* **Return type:**
  [`SlurmResource`](#id30)

#### queue *: `Annotated`[`Union`[[`SlurmQueueConfig`](#id74), `PartialResource`]]*

#### remote_base *: `Optional`[`str`]*

#### ssh *: `Annotated`[`Union`[[`SSHConnectionResource`](#id36), `PartialResource`]]*

### *class* dagster_slurm.SlurmSessionResource(\*\*data)

Bases: `ConfigurableResource`

Slurm session resource for operator fusion.

This is a proper Dagster resource that manages the lifecycle
of a Slurm allocation across multiple assets in a run.

Usage in definitions.py:
: session = SlurmSessionResource(
  : slurm=slurm,
    num_nodes=4,
    time_limit=”04:00:00”,
  <br/>
  )

* **Parameters:**
  **data** (`Any`)

#### enable_health_checks *: `bool`*

#### enable_session *: `bool`*

#### execute_in_session(execution_plan, asset_key)

Execute workload in the shared allocation.
Thread-safe for parallel asset execution.

* **Parameters:**
  * **execution_plan** (`ExecutionPlan`)
  * **asset_key** (`str`)
* **Return type:**
  `int`

#### max_concurrent_jobs *: `int`*

#### model_post_init(context,)

This function is meant to behave like a BaseModel method to initialise private attributes.

It takes context as an argument since that’s what pydantic-core passes when calling it.

* **Parameters:**
  * **self** (`BaseModel`) – The BaseModel instance.
  * **context** (`Any`) – The context.
* **Return type:**
  `None`

#### num_nodes *: `int`*

#### partition *: `Optional`[`str`]*

#### setup_for_execution(context)

Called by Dagster when resource is initialized for a run.
This is the proper Dagster resource lifecycle hook.

* **Parameters:**
  **context** (`InitResourceContext`)
* **Return type:**
  [`SlurmSessionResource`](#id62)

#### slurm *: SlurmResource*

#### teardown_after_execution(context)

Called by Dagster when resource is torn down after run completion.
This is the proper Dagster resource lifecycle hook.

* **Parameters:**
  **context** (`InitResourceContext`)
* **Return type:**
  `None`

#### time_limit *: `str`*

### *class* dagster_slurm.SparkLauncher(\*\*data)

Bases: `ComputeLauncher`

Apache Spark launcher.

Modes:
- Local: Single-node Spark (no allocation_context)
- Cluster: Spark cluster across Slurm allocation (via allocation_context)
- Standalone: Connect to existing Spark cluster (via master_url)

* **Parameters:**
  **data** (`Any`)

#### driver_memory *: `str`*

#### executor_cores *: `int`*

#### executor_memory *: `str`*

#### master_url *: `Optional`[`str`]*

#### num_executors *: `Optional`[`int`]*

#### prepare_execution(payload_path, python_executable, working_dir, pipes_context, extra_env=None, allocation_context=None, activation_script=None)

Generate Spark execution plan.

* **Parameters:**
  * **payload_path** (`str`)
  * **python_executable** (`str`)
  * **working_dir** (`str`)
  * **pipes_context** (`Dict`[`str`, `str`])
  * **extra_env** (`Optional`[`Dict`[`str`, `str`]])
  * **allocation_context** (`Optional`[`Dict`[`str`, `Any`]])
  * **activation_script** (`Optional`[`str`])
* **Return type:**
  `ExecutionPlan`

#### spark_home *: `str`*

### *class* dagster_slurm.ComputeResource(\*\*data)

Bases: `ConfigurableResource`

Unified compute resource - adapts to deployment.

This is the main facade that assets depend on.
Hides complexity of local vs Slurm vs session execution.

Usage:
: @asset
  def my_asset(context: AssetExecutionContext, compute: ComputeResource):
  <br/>
  > return compute.run(
  > : context=context,
  >   payload_path=”script.py”,
  >   launcher=RayLauncher(num_gpus_per_node=2)
  <br/>
  > )

Configuration Examples:

Local mode (dev):
: compute = ComputeResource(mode=”local”)

Slurm per-asset mode (staging):
: slurm = SlurmResource.from_env()
  compute = ComputeResource(mode=”slurm”, slurm=slurm)

Slurm session mode with cluster reuse (prod):
: slurm = SlurmResource.from_env()
  session = SlurmSessionResource(slurm=slurm, num_nodes=10)
  compute = ComputeResource(
  <br/>
  > mode=”slurm-session”,
  > slurm=slurm,
  > session=session,
  > enable_cluster_reuse=True,
  > cluster_reuse_tolerance=0.2,
  <br/>
  )

Heterogeneous job mode (optimal resource allocation):
: compute = ComputeResource(
  : mode=”slurm-hetjob”,
    slurm=slurm,
  <br/>
  )

* **Parameters:**
  **data** (`Any`)

#### auto_detect_platform *: `bool`*

#### cleanup_on_failure *: `bool`*

#### cluster_reuse_tolerance *: `float`*

#### debug_mode *: `bool`*

#### default_launcher *: `Optional`[`ComputeLauncher`]*

#### enable_cluster_reuse *: `bool`*

#### get_pipes_client(context, launcher=None)

Get appropriate Pipes client for this mode.

* **Parameters:**
  * **context** (`InitResourceContext`) – Dagster resource context
  * **launcher** (`Optional`[`ComputeLauncher`]) – Override launcher (uses default if None)
* **Returns:**
  LocalPipesClient or SlurmPipesClient

#### mode *: `ExecutionMode`*

#### model_post_init(\_ComputeResource_\_context)

Post-init setup - runs after Pydantic init.

#### pack_platform *: `Optional`[`str`]*

#### register_cluster(cluster_address, framework, cpus, gpus, memory_gb)

Register a newly created cluster for future reuse.

* **Parameters:**
  * **cluster_address** (`str`) – Address of the cluster (e.g., “10.0.0.1:6379”)
  * **framework** (`str`) – “ray” or “spark”
  * **cpus** (`int`) – Total CPUs in cluster
  * **gpus** (`int`) – Total GPUs in cluster
  * **memory_gb** (`int`) – Total memory in GB

#### run(context, payload_path, launcher=None, extra_slurm_opts=None, resource_requirements=None, \*\*kwargs)

Execute asset with optional resource overrides.

* **Parameters:**
  * **context** – Dagster execution context
  * **payload_path** (`str`) – Path to Python script
  * **launcher** (`Optional`[`ComputeLauncher`]) – Override launcher for this asset
  * **extra_slurm_opts** (`Optional`[`Dict`[`str`, `Any`]]) – Override Slurm options (non-session mode)
    - nodes: int
    - cpus_per_task: int
    - mem: str (e.g., “32G”)
    - gpus_per_node: int
    - time_limit: str (e.g., “02:00:00”)
  * **resource_requirements** (`Optional`[`Dict`[`str`, `Any`]]) – Resource requirements for cluster reuse (session mode)
    - cpus: int
    - gpus: int
    - memory_gb: int
    - framework: str (“ray” or “spark”)
  * **\*\*kwargs** – Passed to client.run()
* **Yields:**
  Dagster events

### Examples

```python
# Simple execution with default resources
yield from compute.run(context, "script.py")
```

```python
# Override launcher for this asset
ray_launcher = RayLauncher(num_gpus_per_node=4)
yield from compute.run(context, "script.py", launcher=ray_launcher)
```

```python
# Non-session mode: override Slurm resources
yield from compute.run(
    context,
    "script.py",
    extra_slurm_opts={"nodes": 1, "cpus_per_task": 16, "mem": "64G", "gpus_per_node": 2}
)
```

```python
# Session mode: specify resource requirements for cluster reuse
yield from compute.run(
    context,
    "script.py",
    launcher=RayLauncher(num_gpus_per_node=2),
    resource_requirements={"cpus": 32, "gpus": 2, "memory_gb": 128, "framework": "ray"}
)
```

#### run_hetjob(context, assets, launchers=None)

Run multiple assets as a heterogeneous Slurm job.

Submit all assets together with their specific resource requirements.
Only waits in queue ONCE, but each asset gets the resources it needs.

* **Parameters:**
  * **context** – Dagster execution context
  * **assets** (`List`[`Tuple`[`str`, `str`, `Dict`[`str`, `Any`]]]) – 

    List of (asset_key, payload_path, resource_requirements)
    resource_requirements:
    > - nodes: int (default: 1)
    > - cpus_per_task: int (default: 2)
    > - mem: str (default: “4G”)
    > - gpus_per_node: int (default: 0)
    > - time_limit: str (default: “01:00:00”)
  * **launchers** (`Optional`[`Dict`[`str`, `ComputeLauncher`]]) – Optional dict mapping asset_key to ComputeLauncher
* **Yields:**
  Dagster events

### Example

```python
compute.run_hetjob(
    context,
    assets=[
        ("prep", "prep.py", {"nodes": 1, "cpus_per_task": 8, "mem": "32G"}),
        ("train", "train.py", {"nodes": 4, "cpus_per_task": 32, "mem": "128G", "gpus_per_node": 2}),
        ("infer", "infer.py", {"nodes": 8, "cpus_per_task": 16, "mem": "64G", "gpus_per_node": 1}),
    ],
    launchers={
        "train": RayLauncher(num_gpus_per_node=2),
        "infer": RayLauncher(num_gpus_per_node=1),
    }
)
```

#### session *: `Optional`[[`SlurmSessionResource`](#id62)]*

#### slurm *: `Optional`[[`SlurmResource`](#id30)]*

#### teardown(context)

Teardown method called by Dagster at end of run.
Ensures session resources and clusters are cleaned up.

* **Parameters:**
  **context** (`InitResourceContext`)

#### validate_configuration()

Validate configuration - runs during Pydantic validation.

* **Return type:**
  [`ComputeResource`](#id0)

### *class* dagster_slurm.SlurmResource(\*\*data)

Bases: `ConfigurableResource`

Complete Slurm cluster configuration.
Combines SSH connection, queue defaults, and cluster-specific paths.

* **Parameters:**
  **data** (`Any`)

#### *classmethod* from_env()

Create from environment variables.

* **Return type:**
  [`SlurmResource`](#id30)

#### *classmethod* from_env_slurm(ssh)

Create a SlurmResource by populating most fields from environment variables,
but requires an explicit, pre-configured SSHConnectionResource to be provided.

* **Parameters:**
  **ssh** ([`SSHConnectionResource`](#id36)) – A fully configured SSHConnectionResource instance.
* **Return type:**
  [`SlurmResource`](#id30)

#### queue *: `Annotated`[`Union`[[`SlurmQueueConfig`](#id74), `PartialResource`]]*

#### remote_base *: `Optional`[`str`]*

#### ssh *: `Annotated`[`Union`[[`SSHConnectionResource`](#id36), `PartialResource`]]*

### *class* dagster_slurm.SSHConnectionResource(\*\*data)

Bases: `ConfigurableResource`

SSH connection settings.

Supports two authentication methods:
1. SSH key (recommended for automation)
2. Password (for interactive use or when keys unavailable)

Either key_path OR password must be provided (not both).

### Examples

```python
# Key-based auth
ssh = SSHConnectionResource(
    host="cluster.example.com",
    user="username",
    key_path="~/.ssh/id_rsa",
)
```

```python
# Password-based auth
ssh = SSHConnectionResource(
    host="cluster.example.com",
    user="username",
    password="secret123",
)
```

```python
```

# From environment variables
ssh = SSHConnectionResource.from_env()

* **Parameters:**
  **data** (`Any`)

#### extra_opts *: `List`[`str`]*

#### *classmethod* from_env(prefix='SLURM_SSH')

Create from environment variables.

Environment variables:
: ```
  ``
  ```
  <br/>
  {prefix}\`\`_HOST - SSH hostname (required)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_PORT - SSH port (optional, default: 22)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_USER - SSH username (required)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_KEY - Path to SSH key (optional)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_PASSWORD - SSH password (optional)
  <br/>
  ```
  ``
  ```
  <br/>
  {prefix}\`\`_OPTS_EXTRA - Additional SSH options (optional)

Either KEY or PASSWORD must be set (not both).

* **Parameters:**
  **prefix** (`str`) – Environment variable prefix (default: “SLURM_SSH”)
* **Return type:**
  [`SSHConnectionResource`](#id36)
* **Returns:**
  SSHConnectionResource instance
* **Raises:**
  **ValueError** – If required variables missing or both auth methods specified

Example:

```bash
export SLURM_SSH_HOST=cluster.example.com
export SLURM_SSH_USER=username
export SLURM_SSH_KEY=~/.ssh/id_rsa
# OR
export SLURM_SSH_PASSWORD=secret123
```

…

#### get_remote_target()

Get the remote target string for SCP commands.

* **Return type:**
  `str`
* **Returns:**
  String in format [‘user@host](mailto:'user@host)’

### Example

[‘username@cluster.example.com](mailto:'username@cluster.example.com)’

#### get_scp_base_command()

Build base SCP command for file transfers.

* **Return type:**
  `List`[`str`]
* **Returns:**
  List of command arguments (without source/dest)

#### NOTE
For password authentication, password handling is done via pexpect.

### Example

[‘scp’, ‘-P’, ‘22’, ‘-i’, ‘/path/to/key’]
# OR (password auth)
[‘scp’, ‘-P’, ‘22’]

#### get_ssh_base_command()

Build base SSH command for subprocess.

* **Return type:**
  `List`[`str`]
* **Returns:**
  List of command arguments for subprocess.run()

#### NOTE
For password authentication, this returns the base command.
Password handling is done separately via pexpect in SSHConnectionPool.

This method is used by SSHMessageReader which uses ControlMaster,
so password auth is already handled by the ControlMaster connection.

### Example

[‘ssh’, ‘-p’, ‘22’, ‘-i’, ‘/path/to/key’, [‘user@host](mailto:'user@host)’]
# OR (password auth via ControlMaster)
[‘ssh’, ‘-p’, ‘22’, ‘-o’, ‘ControlPath=/tmp/…’, [‘user@host](mailto:'user@host)’]

#### host *: `str`*

#### key_path *: `Optional`[`str`]*

#### password *: `Optional`[`str`]*

#### port *: `int`*

#### user *: `str`*

#### *property* uses_key_auth *: bool*

Returns True if using key-based authentication.

#### *property* uses_password_auth *: bool*

Returns True if using password-based authentication.

#### validate_auth_method()

Ensure exactly one authentication method is provided.

### *class* dagster_slurm.SlurmSessionResource(\*\*data)

Bases: `ConfigurableResource`

Slurm session resource for operator fusion.

This is a proper Dagster resource that manages the lifecycle
of a Slurm allocation across multiple assets in a run.

Usage in definitions.py:
: session = SlurmSessionResource(
  : slurm=slurm,
    num_nodes=4,
    time_limit=”04:00:00”,
  <br/>
  )

* **Parameters:**
  **data** (`Any`)

#### enable_health_checks *: `bool`*

#### enable_session *: `bool`*

#### execute_in_session(execution_plan, asset_key)

Execute workload in the shared allocation.
Thread-safe for parallel asset execution.

* **Parameters:**
  * **execution_plan** (`ExecutionPlan`)
  * **asset_key** (`str`)
* **Return type:**
  `int`

#### max_concurrent_jobs *: `int`*

#### model_post_init(context,)

This function is meant to behave like a BaseModel method to initialise private attributes.

It takes context as an argument since that’s what pydantic-core passes when calling it.

* **Parameters:**
  * **self** (`BaseModel`) – The BaseModel instance.
  * **context** (`Any`) – The context.
* **Return type:**
  `None`

#### num_nodes *: `int`*

#### partition *: `Optional`[`str`]*

#### setup_for_execution(context)

Called by Dagster when resource is initialized for a run.
This is the proper Dagster resource lifecycle hook.

* **Parameters:**
  **context** (`InitResourceContext`)
* **Return type:**
  [`SlurmSessionResource`](#id62)

#### slurm *: SlurmResource*

#### teardown_after_execution(context)

Called by Dagster when resource is torn down after run completion.
This is the proper Dagster resource lifecycle hook.

* **Parameters:**
  **context** (`InitResourceContext`)
* **Return type:**
  `None`

#### time_limit *: `str`*

### *class* dagster_slurm.SlurmQueueConfig(\*\*data)

Bases: `ConfigurableResource`

Default Slurm job submission parameters.
These can be overridden per-asset via metadata or function arguments.

* **Parameters:**
  **data** (`Any`)

#### cpus *: `int`*

#### gpus_per_node *: `int`*

#### mem *: `str`*

#### mem_per_cpu *: `str`*

#### num_nodes *: `int`*

#### partition *: `str`*

#### time_limit *: `str`*
