#! /bin/bash -e

set -o pipefail

if [ -z "$USER_NAME" ]; then
    USER_NAME=build
fi

home_base=/container_home

setup_user() {
    useradd_params=''
    if [ ! -z "$USER_UID" ]; then
        useradd_params="$useradd_params --uid $USER_UID"
    fi
    if [ ! -z "$USER_GID" ]; then
        groupadd --force --gid $USER_GID $USER_NAME
        useradd_params="$useradd_params --gid $USER_GID"
    fi
    useradd_params="$useradd_params -m --home-dir $home_base/$USER_NAME $USER_NAME"

    mkdir -p $home_base
    useradd $useradd_params
}

setup_maven_repo() {
    maven_dir=/var/lib/maven
    maven_repo=$maven_dir/m2

    mkdir -p $maven_repo
    chown $USER_NAME: $maven_repo

    ln -s $maven_repo $home_base/$USER_NAME/.m2
}

setup_user
setup_maven_repo

exec gosu $USER_NAME "$@"
