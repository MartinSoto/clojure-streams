#! /bin/bash -e

set -o pipefail

setup_user() {
    home_base=/container_home

    useradd_params=''
    if [ ! -z "$USER_UID" ]; then
        useradd_params="$useradd_params --uid $USER_UID"
    fi
    if [ ! -z "$USER_GID" ]; then
        useradd_params="$useradd_params --gid $USER_GID"
    fi
    if [ ! -z "$USER_DIR" ]; then
        mkdir -p $USER_DIR
    fi
    if [ -z "$USER_NAME" ]; then
        USER_NAME=build
    fi
    useradd_params="$useradd_params -m --home-dir $home_base/$USER_NAME $USER_NAME"

    mkdir -p $home_base
    useradd $useradd_params

    docker_group=$(stat -c '%G' /var/run/docker.sock)
    usermod -aG $docker_group $USER_NAME
}

setup_user

exec gosu $USER_NAME "$@"
