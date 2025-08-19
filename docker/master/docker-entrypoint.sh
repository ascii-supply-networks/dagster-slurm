#!/bin/bash

# sudo chmod 400 /etc/munge/munge.key
# sudo chown munge:munge /etc/munge/munge.key

# user home permission
sudo chown admin:admin -R /home/admin

# start services
sudo service munge start
echo "---> MUNGE status ..."
munge -n | unmunge | grep STATUS

sudo service slurmctld start

# source /etc/profile.d/lmod.sh
# module --version

# Create missing slurm log directories
sudo mkdir -p /var/log/slurm
sudo chown slurm:slurm /var/log/slurm
sudo chmod 755 /var/log/slurm


#environment check spack
source /tools/spack/share/spack/setup-env.sh
spack --version

#environment check easybuild
export PATH="${PATH}:/tools/easybuild/bin"
export PYTHONPATH="/tools/easybuild/lib/python3.9/site-packages:${PYTHONPATH}"
sudo -u admin bash -c 'export PATH="${PATH}:/tools/easybuild/bin" && export PYTHONPATH="/tools/easybuild/lib/python3.9/site-packages:${PYTHONPATH}" && eb --version'
sudo -u admin bash -c 'export PATH="${PATH}:/tools/easybuild/bin" && export PYTHONPATH="/tools/easybuild/lib/python3.9/site-packages:${PYTHONPATH}" && eb --show-system-info'
sudo -u admin bash -c 'export PATH="${PATH}:/tools/easybuild/bin" && export PYTHONPATH="/tools/easybuild/lib/python3.9/site-packages:${PYTHONPATH}" && eb --show-config'

# sudo -u admin eb bzip2-1.0.6.eb 

#environment check lmod
source /etc/profile.d/lmod.sh
#source /opt/focal/lmod/lmod/init/bash
module --version
#module use ~/.local/easybuild/modules/
module avail

sleep 3
sudo service slurmctld restart

tail -f /dev/null