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

setup_gradle_dir() {
    gradle_dir=/var/lib/gradle
    gradle_data=$gradle_dir/data

    mkdir -p $gradle_data
    chown $USER_NAME: $gradle_data

    ln -s $gradle_data $home_base/$USER_NAME/.gradle
}

setup_user
setup_maven_repo
setup_gradle_dir

exec gosu $USER_NAME "$@"
