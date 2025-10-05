import argparse
import os
import subprocess
import sys
from loguru import logger
import json

from dagster_slurm import SSHConnectionResource
from dagster_slurm.helpers.env_packaging import pack_environment_with_pixi
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from pathlib import Path

def get_git_commit_hash() -> str:
    """Retrieves the current git commit hash."""
    try:
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.PIPE
        ).decode("utf-8").strip()
        logger.info(f"Retrieved git commit hash: {commit_hash}")
        return commit_hash
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error("Could not retrieve git commit hash. Is this a git repository?")
        raise RuntimeError("Failed to get git commit hash") from e

def main():
    """
    Main function to pack and deploy the environment.
    Reads SSH credentials and deployment path from environment variables.
    """
    parser = argparse.ArgumentParser(description="Pack and deploy a pixi environment to an HPC system.")
    parser.add_argument(
        "--platform",
        help="Specify the target platform for pixi pack (e.g., 'linux-aarch64', 'linux-64').",
        default="linux-64",
    )
    parser.add_argument(
        "--output-json",
        help="Path to write the deployment metadata JSON file.",
        type=Path,
        default="deployment_metadata.json",
    )
    args = parser.parse_args()

    try:
        base_path = os.environ["SLURM_DEPLOYMENT_BASE_PATH"]
        ssh_host = os.environ["SLURM_EDGE_NODE_HOST"]
        ssh_port = int(os.environ["SLURM_EDGE_NODE_PORT"])
        ssh_user = os.environ["SLURM_EDGE_NODE_USER"]
        
        key_path = os.getenv("SLURM_EDGE_NODE_KEY_PATH")
        password = os.getenv("SLURM_EDGE_NODE_PASSWORD")

        if not key_path and not password:
            raise ValueError("Authentication failed: You must set either SLURM_EDGE_NODE_KEY_PATH or SLURM_EDGE_NODE_PASSWORD.")
        if key_path and password:
            raise ValueError("Authentication conflict: You cannot set both SLURM_EDGE_NODE_KEY_PATH and SLURM_EDGE_NODE_PASSWORD.")

        ssh_config = SSHConnectionResource(
            host=ssh_host,
            port=ssh_port,
            user=ssh_user,
            key_path=key_path,
            password=password,
        )
        auth_method = "key-based" if key_path else "password-based"
        logger.info(f"Loaded SSH configuration for {ssh_config.user}@{ssh_config.host} (using {auth_method} auth).")

    except (KeyError, ValueError) as e:
        logger.error(f"Configuration error: {e}")
        logger.error("\nPlease ensure the following environment variables are set correctly:")
        logger.error(" - SLURM_DEPLOYMENT_BASE_PATH")
        logger.error(" - SLURM_EDGE_NODE")
        logger.error(" - SLURM_EDGE_NODE_USER")
        logger.error(" - and EITHER SLURM_EDGE_NODE_KEY_PATH OR SLURM_EDGE_NODE_PASSWORD")
        sys.exit(1)
    
    commit_hash = get_git_commit_hash()
    deployment_path = f"{base_path}/{commit_hash}"
    logger.info(f"Target deployment path on remote: {deployment_path}")

    logger.info("Packing environment using pixi...")
    pack_cmd_map = {
        "linux-64": ["pixi", "run", "--frozen", "pack"],
        "linux-aarch64": ["pixi", "run", "--frozen", "pack-aarch"],
    }
    pack_cmd = pack_cmd_map.get(args.platform) if args.platform else ["pixi", "run", "--frozen", "pack"]
    logger.info(f"Using pack command: {pack_cmd}")
    
    try:
        packed_file_path = pack_environment_with_pixi(pack_cmd=pack_cmd)
    except Exception as e:
        logger.error(f"Failed to pack environment: {e}")
        sys.exit(1)

    with SSHConnectionPool(ssh_config) as ssh_pool:
        try:
            logger.info(f"Creating remote deployment directory: {deployment_path}")
            ssh_pool.run(f"mkdir -p {deployment_path}")

            remote_pack_file = f"{deployment_path}/{packed_file_path.name}"
            logger.info(f"Uploading {packed_file_path.name} to {remote_pack_file}...")
            ssh_pool.upload_file(str(packed_file_path.absolute()), remote_pack_file)
            logger.info("Upload complete.")

            logger.info("Extracting environment on remote host...")
            ssh_pool.run(f"chmod +x {remote_pack_file}")
            extract_cmd = f"cd {deployment_path} && ./{packed_file_path.name}"
            ssh_pool.run(extract_cmd, timeout=600)

            env_dir = f"{deployment_path}/env"
            activation_script = f"{deployment_path}/activate.sh"
            python_executable = f"{env_dir}/bin/python"
            
            verify_cmd = f"test -f {activation_script} && test -f {python_executable}"
            ssh_pool.run(verify_cmd)
            logger.info("Environment extracted and verified successfully.")

            logger.info(f"Deployed to path: {deployment_path}")

            metadata = {
                "git_commit_full": commit_hash,
                "git_commit_short": commit_hash[:7],
                "deployment_path": deployment_path,
                "platform": args.platform or "default",
            }

            print("xxxxyyy")
            
            with open(args.output_json, "w") as f:
                json.dump(metadata, f, indent=2)
            print("xxxx")
            logger.info(f"Successfully wrote deployment metadata to {args.output_json}")

        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            try:
                logger.warning(f"Attempting to clean up failed deployment at {deployment_path}")
                ssh_pool.run(f"rm -rf {deployment_path}")
            except Exception as cleanup_err:
                logger.error(f"Cleanup failed: {cleanup_err}")
            sys.exit(1)

if __name__ == "__main__":
    main()