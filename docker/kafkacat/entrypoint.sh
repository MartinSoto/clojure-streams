#! /bin/sh -e

set -o pipefail

exec kafkacat -b kafka "$@"
