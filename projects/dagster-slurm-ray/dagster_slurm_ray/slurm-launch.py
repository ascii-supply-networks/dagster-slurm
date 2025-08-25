# slurm-launch.py
# Usage with dry run:
# python slurm-launch.py --exp-name test \
#     --activation-script /data/activate.sh \
#     --command "python my_script.py" --dry-run

import argparse
import subprocess
import sys
import time
from pathlib import Path

# Assumes slurm-template.sh is in the same directory as this script
template_file = Path(__file__).parent / "slurm-template.sh"

# Placeholders in the template file
JOB_NAME = "${JOB_NAME}"
NUM_NODES = "${NUM_NODES}"
NUM_GPUS_PER_NODE = "${NUM_GPUS_PER_NODE}"
PARTITION_OPTION = "${PARTITION_OPTION}"
COMMAND_PLACEHOLDER = "${COMMAND_PLACEHOLDER}"
GIVEN_NODE = "${GIVEN_NODE}"
LOAD_ENV = "${LOAD_ENV}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--exp-name",
        type=str,
        required=True,
        help="The job name and path to logging file (exp_name.log).",
    )
    parser.add_argument(
        "--num-nodes", "-n", type=int, default=1, help="Number of nodes to use."
    )
    parser.add_argument(
        "--node",
        "-w",
        type=str,
        help="The specified nodes to use. Same format as the "
        "return of 'sinfo'. Default: ''.",
    )
    parser.add_argument(
        "--num-gpus",
        type=int,
        default=0,
        help="Number of GPUs to use in each node. (Default: 0)",
    )
    parser.add_argument(
        "--partition",
        "-p",
        type=str,
    )
    parser.add_argument(
        "--activation-script",
        type=str,
        help="The full path to the shell script to source for activating the environment (e.g., /data/activate.sh).",
        default="",
    )
    parser.add_argument(
        "--command",
        type=str,
        required=True,
        help="The command you wish to execute. For example: "
        " --command 'python test.py'. "
        "Note that the command must be a string.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If set, generate the submission script but do not submit it.",
    )

    args = parser.parse_args()

    if args.node:
        node_info = "#SBATCH -w {}".format(args.node)
    else:
        node_info = ""

    job_name = "{}_{}".format(
        args.exp_name, time.strftime("%m%d-%H%M", time.localtime())
    )

    partition_option = (
        "#SBATCH --partition={}".format(args.partition) if args.partition else ""
    )

    if args.activation_script:
        load_env_command = f"source {args.activation_script}"
    else:
        load_env_command = "# No activation script provided."

    if not template_file.is_file():
        print(f"Error: Template file not found at {template_file}")
        sys.exit(1)

    with open(template_file, "r") as f:
        text = f.read()

    text = text.replace(JOB_NAME, job_name)
    text = text.replace(NUM_NODES, str(args.num_nodes))
    text = text.replace(NUM_GPUS_PER_NODE, str(args.num_gpus))
    text = text.replace(PARTITION_OPTION, partition_option)
    text = text.replace(COMMAND_PLACEHOLDER, str(args.command))
    text = text.replace(LOAD_ENV, load_env_command)
    text = text.replace(GIVEN_NODE, node_info)
    text = text.replace(
        "# THIS FILE IS A TEMPLATE AND IT SHOULD NOT BE DEPLOYED TO PRODUCTION!",
        "# THIS FILE IS MODIFIED AUTOMATICALLY FROM TEMPLATE AND SHOULD BE RUNNABLE!",
    )

    script_file = "{}.sh".format(job_name)
    with open(script_file, "w") as f:
        f.write(text)

    if args.dry_run:
        print("--- DRY RUN MODE ---")
        print(f"Job script '{script_file}' has been generated but NOT submitted.")
        print("You can inspect the script and submit it manually with:")
        print(f"  sbatch {script_file}")
    else:
        print("Starting to submit job!")
        result = subprocess.run(
            ["sbatch", script_file], capture_output=True, text=True, check=True
        )
        print(result.stdout.strip())
        print(
            "Job submitted! Script file is at: <{}>. Log file is at: <{}>".format(
                script_file, "{}.log".format(job_name)
            )
        )

    sys.exit(0)
