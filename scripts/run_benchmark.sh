#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

# Set magic variables for current file & dir
__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
__file="${__dir}/$(basename "${BASH_SOURCE[0]}")"
__base="$(basename ${__file} .sh)"
__root="$(cd "$(dirname "${__dir}")" && pwd)"

ACCOUNT_NAME=$1
ACCOUNT_KEY=$2
TABLE_NAME_PREFIX=${3:-"log"}

AZP="$__dir/../bin/azp"
NUM_WORKERS=8 16 32 64 128

$AZP table populate \
    --account-name $ACCOUNT_NAME  \
    --account-key $ACCOUNT_KEY \
    --table-name $TABLE_NAME 
    -v info 
    --max-num-entities 1000
    --start-year 2019
