---
sidebar_position: 99
title: VSC-5 SSH Keepalive
---

To keep the VSC-5 login chain responsive, configure SSH keepalives for the `vmos` bastion and `vsc5` login node:

```sshconfig title="~/.ssh/config"
Host vmos
    HostName vmos.vsc.ac.at
    User dagster01
    ServerAliveInterval 120
    ServerAliveCountMax 2

Host vsc5
    HostName vsc5.vsc.ac.at
    User dagster01
    ProxyJump vmos
    ServerAliveInterval 120
    ServerAliveCountMax 2
```

Replace the usernames with your account. These settings dramatically reduce dropped ControlMaster tunnels and `ssh_exchange_identification` failures when submitting many jobs through `dagster-slurm`.
