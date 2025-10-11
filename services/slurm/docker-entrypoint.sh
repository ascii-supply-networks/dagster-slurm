#!/bin/bash
set -e

echo "---> Ensuring correct permissions for Slurm directories..."
mkdir -p /var/spool/slurmd /var/run/slurmd /var/lib/slurmd /var/log/slurm
chown -R slurm:slurm /var/spool/slurmd /var/run/slurmd /var/lib/slurmd /var/log/slurm /etc/slurm

echo "---> Starting MUNGE service..."
gosu munge /usr/sbin/munged

# If the command is a SLURM daemon, also start SSHD.
if [[ "$1" == "slurmctld" || "$1" == "slurmd" ]]; then
    echo "---> Starting SSH service..."
    /usr/sbin/sshd
fi

echo "---> Executing primary command: $@"
if [[ "$1" == "slurmdbd" || "$1" == "slurmctld" || "$1" == "slurmd" ]]; then
    exec gosu slurm "/usr/sbin/$1" -Dvvv
else
    exec "$@"
fi