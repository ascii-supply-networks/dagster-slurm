FROM docker.io/nvidia/cuda:11.0.3-base-ubuntu20.04
#FROM nvidia/cuda:11.0.3-base-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive

ENV EB_ROOT=/tools/easybuild
ENV EB_PYTHON=python3.9
ENV EB_MODULEPATH="~/.local/easybuild/modules"
ENV LMOD_ROOT=/tools/focal/lmod
ENV LMOD_CMD=/tools/focal/lmod/lmod/libexec/lmod
ENV LMOD_COLORIZE="YES"
ENV MODULEPATH_ROOT="/tools/focal/modulefiles"
ENV MODULEPATH="/tools/spack/share/spack/lmod:~/.local/easybuild/modules:/tools/uw/modulefiles:/tools/spack/share/spack/modules"
ENV SPACK_ROOT=/tools/spack
ENV PATH="${PATH}:${SPACK_ROOT}/bin:${EB_ROOT}/bin"
ENV SPACK_LMOD_MODULES='/tools/spack/bin/spack module lmod refresh'
ENV SPACK_TCL_MODULES='/tools/spack/bin/spack module tcl refresh'
ENV SPACK_SEARCH_MODULES='/tools/spack/bin/spack list'

RUN apt update -y && apt install munge -y && apt install vim -y && apt install build-essential -y && apt install git -y && apt-get install mariadb-server -y && apt install wget -y && apt install nfs-common -y

RUN apt install slurmd slurm-client slurmctld -y

# Make Python3 available as `python` also ensure that pip points to pip3
RUN apt install sudo -y && apt install python3.9 python3.9-dev python3-pip -y && ln -s /usr/bin/python3.9 /usr/bin/python 

RUN rm /usr/bin/python3 && ln -s /usr/bin/python3.9 /usr/bin/python3

RUN useradd -m admin -s /usr/bin/bash -d /home/admin && echo "admin:admin" | chpasswd && adduser admin sudo && echo "admin     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers 

RUN apt update -y && apt install libopenmpi-dev -y && pip3 install mpi4py
##RUN apt update -y && apt install net-tools -y && apt install iputils-ping -y && apt install iperf3 -y && apt install nano -y && apt install iotop -y
RUN apt update -y && apt install net-tools -y && apt install iputils-ping -y && apt install iperf3 -y && apt install nano -y && apt install iotop -y && apt install htop -y && apt install fish -y


RUN mkdir -p /etc/slurm-llnl
RUN chown -R slurm:slurm /etc/slurm-llnl

# configuration
#COPY /etc/gres.conf /etc/slurm-llnl/
#COPY /etc/slurm.conf /etc/slurm-llnl/
#COPY /etc/cgroup.conf /etc/slurm-llnl/
#COPY docker-entrypoint.sh /etc/slurm-llnl/

## Authentication                              
# RUN mkdir -p /etc/munge
# RUN chown -R munge:munge /etc/munge
# # Authentication
COPY munge.key /etc/munge/
RUN sudo chmod 400 /etc/munge/munge.key
RUN sudo chown munge:munge /etc/munge/munge.key

# lmod tool --START
RUN apt install bc unzip lua5.3 liblua5.3-0 liblua5.3-dev lua-filesystem-dev lua-posix-dev -y
RUN apt install tcl tcl8.6-dev libtcl8.6 -y
RUN ln -s /usr/include/tcl8.6 /usr/include/tcl && ls /usr/bin/lua && ls /usr/include/tcl

RUN cd /tmp && git clone https://github.com/TACC/Lmod.git && cd Lmod && ./configure --prefix=/tools/focal/ && make install

## modules files
RUN mkdir -p /tools/uw/modulefiles
RUN touch /tools/focal/lmod/lmod/init/.modulespath
RUN echo "/tools/focal/modulefiles" >>/tools/focal/lmod/lmod/init/.modulespath
RUN echo "/tools/uw/modulefiles" >>/tools/focal/lmod/lmod/init/.modulespath
RUN echo "/tools/spack/share/spack/modules" >>/tools/focal/lmod/lmod/init/.modulespath
RUN echo "~/.local/easybuild/modules" >>/tools/focal/lmod/lmod/init/.modulespath

RUN ln -s /tools/focal/lmod/lmod/init/profile /etc/profile.d/lmod.sh
RUN ln -s /tools/focal/lmod/lmod/init/cshrc /etc/profile.d/lmod.csh
# lmod tool --FINISH

## Utilities
RUN apt update -y && apt install net-tools -y && apt install iputils-ping -y && apt install iperf3 -y && apt install nano -y && apt install iotop -y && apt install htop -y && apt install fish -y && apt install tmux -y

## Shell tool
RUN curl -sLf https://spacevim.org/install.sh | bash
#RUN curl -sLF https://starship.rs/install.sh | bash

# Spack tool
RUN mkdir -p /tools && cd /tools && git clone --depth=100 --branch=releases/v0.22 https://github.com/spack/spack.git /tools/spack && cd /tools/spack/bin && ./spack install zlib

# Spack miniconda3 installation
RUN cd /tools/spack && . share/spack/setup-env.sh && spack list conda && spack install miniconda3 && spack load miniconda3 && conda --version

# RUN /tools/spack/bin/spack install python  
# Spack generate module files for all future installations
#RUN /tools/spack/bin/spack module tcl refresh -y && /tools/spack/bin/spack config add modules:default:enable:[tcl]
RUN /tools/spack/bin/spack module lmod refresh -y && /tools/spack/bin/spack config add modules:default:enable:[lmod]

## EasyBuild tool
RUN mkdir -p /tools/easybuild && pip3 install --prefix /tools/easybuild easybuild 

ENV PYTHONPATH=/tools/easybuild/lib/python3.9/site-packages:$PYTHONPATH

COPY docker-entrypoint.sh /etc/slurm-llnl/docker-entrypoint.sh
RUN chmod +x /etc/slurm-llnl/docker-entrypoint.sh

EXPOSE 6817 6818 6819

WORKDIR /home/admin

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]