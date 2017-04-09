#! /bin/bash -e

set -o pipefail

exec kafkacat -b kafka "$@"
