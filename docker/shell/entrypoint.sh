#! /bin/bash -e

set -o pipefail

setup_user() {
    useradd_params=''
    if [ ! -z "$USER_UID" ]; then
        useradd_params="$useradd_params --uid $USER_UID"
    fi
    if [ ! -z "$USER_GID" ]; then
        useradd_params="$useradd_params --gid $USER_GID"
    fi
    if [ ! -z "$USER_DIR" ]; then
        mkdir -p $USER_DIR
        useradd_params="$useradd_params --home-dir $USER_DIR"
    fi
    if [ -z "$USER_NAME" ]; then
        USER_NAME=build
    fi
    useradd_params="$useradd_params $USER_NAME"

    useradd $useradd_params

    docker_group=$(stat -c '%G' /var/run/docker.sock)
    usermod -aG $docker_group $USER_NAME
}

setup_user

exec gosu $USER_NAME "$@"
