# """Assets using Spark launcher for batch processing."""

# import dagster as dg
# from dagster_slurm import ComputeResource, SparkLauncher


# @dg.asset
# def spark_etl_small(
#     context: dg.AssetExecutionContext,
#     compute_spark: ComputeResource,  # Use compute_spark resource
# ) -> dg.Output:
#     """
#     Small Spark ETL job - uses default Spark launcher.
#     """
#     script_path = dg.file_relative_path(
#         __file__,
#         "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/spark/etl_spark.py",
#     )

#     _ = list(
#         compute_spark.run(
#             context=context,
#             payload_path=script_path,
#             extra_env={
#                 "INPUT_PATH": "/data/input",
#                 "OUTPUT_PATH": "/data/output",
#             },
#         )
#     )

#     return dg.Output(value={"output_path": "/data/output"})


# @dg.asset
# def spark_etl_large(
#     context: dg.AssetExecutionContext,
#     compute_spark: ComputeResource,
# ) -> dg.Output:
#     """
#     Large Spark ETL - OVERRIDE launcher with more resources.
#     """
#     script_path = dg.file_relative_path(
#         __file__,
#         "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/spark/etl_spark.py",
#     )

#     # Override for large dataset
#     large_spark_launcher = SparkLauncher(
#         driver_memory="32g",
#         executor_memory="64g",
#         num_executors=20,
#         executor_cores=8,
#     )

#     _ = list(
#         compute_spark.run(
#             context=context,
#             payload_path=script_path,
#             launcher=large_spark_launcher,  # Override
#             extra_env={
#                 "INPUT_PATH": "/data/large_input",
#                 "OUTPUT_PATH": "/data/large_output",
#             },
#         )
#     )

#     return dg.Output(value={"output_path": "/data/large_output"})


# """Pipeline mixing shell, Ray, and Spark."""

# import dagster as dg
# from dagster_slurm import ComputeResource


# @dg.asset
# def download_data(
#     context: dg.AssetExecutionContext,
#     compute: ComputeResource,  # Shell/bash compute
# ) -> dg.Output:
#     """Download data using simple shell script."""
#     script_path = dg.file_relative_path(__file__, "scripts/download.py")

#     _ = list(compute.run(context=context, payload_path=script_path))

#     return dg.Output(value={"data_path": "/data/raw"})


# @dg.asset
# def preprocess_spark(
#     context: dg.AssetExecutionContext,
#     compute_spark: ComputeResource,  # Spark compute
#     download_data,
# ) -> dg.Output:
#     """Preprocess with Spark."""
#     script_path = dg.file_relative_path(__file__, "scripts/preprocess.py")

#     _ = list(
#         compute_spark.run(
#             context=context,
#             payload_path=script_path,
#             extra_env={"INPUT_PATH": download_data["data_path"]},
#         )
#     )

#     return dg.Output(value={"processed_path": "/data/processed"})


# @dg.asset
# def train_ray(
#     context: dg.AssetExecutionContext,
#     compute_ray: ComputeResource,  # Ray compute
#     preprocess_spark,
# ) -> dg.Output:
#     """Train with Ray."""
#     script_path = dg.file_relative_path(__file__, "scripts/train.py")

#     _ = list(
#         compute_ray.run(
#             context=context,
#             payload_path=script_path,
#             extra_env={"DATA_PATH": preprocess_spark["processed_path"]},
#         )
#     )

#     return dg.Output(value={"model_path": "/models/trained"})
