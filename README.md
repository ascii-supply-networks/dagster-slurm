# dagster-slurm

A Dagster integration for launching and managing jobs on SLURM clusters.

## Overview

This project provides tools to submit and monitor Dagster jobs on SLURM-managed high-performance computing clusters. It supports both local testing with a SLURM minicluster and production deployment on actual supercomputers.

## Installation

### Prerequisites

- Python 3.7 or higher
- Access to a SLURM cluster (or Docker for local testing)
- SSH access to your production cluster (if applicable)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/VYaswanthKumar/dagster-slurm.git
   cd dagster-slurm
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your environment:
   - For production: Set up SSH keys for cluster access
   - For local testing: Start the Docker SLURM minicluster (see below)

## Usage

### Local Testing with Docker

1. Start the SLURM minicluster container:
   ```bash
   docker-compose up -d
   ```

2. Access the container:
   ```bash
   docker exec -it <container_id> bash
   ```

3. Test your job locally (single node):
   ```bash
   python my_mini_job.py
   ```

### Submitting Jobs to SLURM

1. **Dry run** (preview the job script without submitting):
   ```bash
   python slurm-launch.py --exp-name test \
     --command "python my_mini_job.py" \
     --num-nodes 2 \
     --activation-script /home/submitter/activate.sh \
     --dry-run
   ```

   Output:
   ```
   # --- DRY RUN MODE ---
   # Job script 'test_0823-0707.sh' has been generated but NOT submitted.
   # You can inspect the script and submit it manually with:
   #   sbatch test_0823-0707.sh
   ```

2. **Inspect the generated script**:
   ```bash
   cat test_0823-0707.sh
   ```

3. **Submit the job** (remove the `--dry-run` flag):
   ```bash
   python slurm-launch.py --exp-name test \
     --command "python my_mini_job.py" \
     --num-nodes 2 \
     --activation-script /home/submitter/activate.sh
   ```

### Production Deployment

Connect to your production supercomputer via SSH and follow the same submission steps above.

## Monitoring SLURM Jobs

### Check Job Queue

View your running and pending jobs:
```bash
squeue -u submitter
```

### Check Job Status After Completion

By job ID:
```bash
sacct -j <job_id>
```

By user:
```bash
sacct -u submitter
```

All jobs:
```bash
sacct
```

### View Job Details and Logs

While the job is running:
```bash
# Show job details
scontrol show job <job_id>

# View stdout log
cat $(scontrol show job <job_id> | grep -oP 'StdOut=\K\S+')
```

**Note:** Job logs via `scontrol` are only available while the job is running, not after completion.

## Debugging

### Common Debugging Commands

```bash
# Install process utilities (if needed)
yum install procps

# Check running processes
ps aux | grep ray

# Cancel a stuck or unwanted job
scancel <job_id>
```

## Contributing

Contributions are welcome! Please follow these guidelines:

1. **Fork the repository** and create a new branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** with clear, descriptive commits:
   ```bash
   git commit -m "feat: add new feature" # or "fix:", "docs:", etc.
   ```

3. **Test your changes** thoroughly:
   - Run existing tests
   - Test locally with the Docker minicluster
   - Ensure your code follows the project's style guidelines

4. **Submit a pull request**:
   - Provide a clear description of your changes
   - Reference any related issues
   - Include examples or screenshots if applicable

5. **Code Review**: Be responsive to feedback and be prepared to make adjustments

### Areas for Contribution

- Documentation improvements
- Bug fixes
- New features (discuss in issues first)
- Test coverage
- Performance optimizations

## License

This project is open source. Please check the LICENSE file for details.

## Support

For questions, issues, or feature requests, please open an issue on GitHub.

## Acknowledgments

Built with [Dagster](https://dagster.io/) and [SLURM](https://slurm.schedmd.com/).
