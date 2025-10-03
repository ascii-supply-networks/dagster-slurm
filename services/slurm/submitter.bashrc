#!/bin/bash

# Source global definitions
if [ -f /etc/bashrc ]; then
    . /etc/bashrc
fi

# --- Slurm Completion Guard ---
# Only source completion scripts in INTERACTIVE shells.
# This prevents errors in non-interactive sbatch jobs.
if [[ $- == *i* ]]; then
    if [ -d /etc/profile.d ]; then
        for i in /etc/profile.d/*.sh; do
            if [ -r $i ]; then
                . $i
            fi
        done
        unset i
    fi
fi