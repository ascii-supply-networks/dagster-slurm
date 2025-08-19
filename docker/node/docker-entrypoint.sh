#!/usr/bin/env bash
set -euo pipefail

echo "=== NODE DEBUGGING MUNGE SETUP ==="

# make sure the munge user exists FIRST
getent passwd munge >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin munge

# Get the actual munge user/group IDs
MUNGE_UID=$(id -u munge)
MUNGE_GID=$(id -g munge)
echo "Munge UID: $MUNGE_UID, GID: $MUNGE_GID"

# --- MUNGE setup ---
echo "Creating munge directories..."
mkdir -p /etc/munge /var/run/munge /run/munge

# Set ownership
chown -R ${MUNGE_UID}:${MUNGE_GID} /etc/munge /var/run/munge /run/munge

# IMPORTANT: Set permissions correctly for munge
echo "Setting correct permissions for munge directories..."
chmod 755 /run/munge           # KEY FIX: 755 not 700!
chmod 700 /etc/munge
chmod 700 /var/run/munge

# Verify
echo "Final verification:"
ls -ld /run/munge /etc/munge /var/run/munge

# Check if munge key exists
if [[ ! -s /etc/munge/munge.key ]]; then
  echo "ERROR: /etc/munge/munge.key not found!"
  exit 1
else
  chown ${MUNGE_UID}:${MUNGE_GID} /etc/munge/munge.key
  chmod 400 /etc/munge/munge.key
  echo "Munge key permissions:"
  ls -la /etc/munge/munge.key
fi

echo "=== STARTING MUNGE ==="
# Start munged with syslog
echo "Starting munged..."
/usr/sbin/munged --syslog

# Wait and check
sleep 3

if pgrep munged > /dev/null; then
    echo "✅ Munge started successfully"
    echo "Testing munge authentication:"
    munge -n | unmunge | grep STATUS
else
    echo "❌ Munge failed to start"
    echo "Process list:"
    ps aux | grep munge
    exit 1
fi

echo "=== STARTING SLURM NODE ==="
# --- Slurm runtime dirs for NODE ---
mkdir -p /var/spool /var/spool/slurmd /var/log/slurm-llnl /var/log/slurm
chown -R slurm:slurm /var/spool /var/spool/slurmd /var/log/slurm-llnl /var/log/slurm || true

# Wait for the master to be ready
echo "Waiting for slurmmaster to be ready..."
sleep 10

# Test connection to master
echo "Testing connection to master..."
ping -c 2 slurmmaster || echo "Cannot ping slurmmaster"

export SLURM_CONF=/etc/slurm-llnl/slurm.conf

echo "Starting slurmd (node daemon)..."
exec /usr/sbin/slurmd -Dvv