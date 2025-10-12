#!/bin/bash
set -e

mkdir -p \
  /var/spool/slurmd /var/run/slurmd /var/lib/slurmd \
  /var/spool/slurmctld /var/run/slurmctld /var/lib/slurm \
  /var/log/slurm

chown -R slurm:slurm \
  /var/spool/slurmd /var/run/slurmd /var/lib/slurmd \
  /var/spool/slurmctld /var/run/slurmctld /var/lib/slurm \
  /var/log/slurm /etc/slurm

if [ "$1" = "slurmdbd" ]
then
    echo "---> Starting the MUNGE Authentication service (munged) ..."
    gosu munge /usr/sbin/munged

    echo "---> Starting the Slurm Database Daemon (slurmdbd) ..."

    {
        . /etc/slurm/slurmdbd.conf
        until echo "SELECT 1" | mysql -h $StorageHost -u$StorageUser -p$StoragePass 2>&1 > /dev/null
        do
            echo "-- Waiting for database to become active ..."
            sleep 2
        done
    }
    echo "-- Database is now active ..."

    exec gosu slurm /usr/sbin/slurmdbd -Dvvv
fi

if [ "$1" = "slurmctld" ]
then
    echo "---> Starting the MUNGE Authentication service (munged) ..."
    gosu munge /usr/sbin/munged &

    echo "---> Starting the SSH daemon (sshd) in debug mode to capture auth logs..."
    # The '-d' flag runs sshd in the foreground for one connection. 
    # The '-e' flag sends all logs to stderr instead of syslog.
    # The '&' at the end runs the process in the background so the script can continue.
    # All sshd logs will now appear in `docker logs slurmctld`.
    /usr/sbin/sshd -D -e &

    sleep 2 

    # Verify sshd started and is listening on port 22
    if ! ss -ltn | grep -q ':22 '; then
        echo "❌ CRITICAL: SSH daemon failed to start or is not listening on port 22."
        # If it failed, its error message will be in the container logs.
        # No need to dump logs here as this script's output is already part of them.
        exit 1
    fi
    echo "---> SSH daemon started successfully and is logging to stderr."

    echo "---> Waiting for slurmdbd to become active before starting slurmctld ..."
    until 2>/dev/null >/dev/tcp/slurmdbd/6819
    do
        echo "-- slurmdbd is not available.  Sleeping ..."
        sleep 2
    done
    echo "-- slurmdbd is now active ..."

    echo "---> Starting the Slurm Controller Daemon (slurmctld) ..."
    # The 'exec' command replaces this script with the slurmctld process.
    # The backgrounded sshd process will continue to run alongside it.
    exec gosu slurm /usr/sbin/slurmctld -Dvvv
fi

if [ "$1" = "slurmd" ]
then
    # This block is for compute nodes (c1, c2).
    # Notice there is NO sshd command here.

    echo "---> Starting the MUNGE Authentication service (munged) ..."
    gosu munge /usr/sbin/munged

    echo "---> Waiting for slurmctld to become active before starting slurmd..."
    until 2>/dev/null >/dev/tcp/slurmctld/6817
    do
        echo "-- slurmctld is not available.  Sleeping ..."
        sleep 2
    done
    echo "-- slurmctld is now active ..."

    echo "---> Starting the Slurm Node Daemon (slurmd) ..."
    exec /usr/sbin/slurmd -Dvvv
fi

exec "$@"