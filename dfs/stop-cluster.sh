#!/usr/bin/env bash
set -x

script_dir="$(cd "$(dirname "$0")" && pwd)"
log_dir="${script_dir}/logs"

source "${script_dir}/nodes.sh"

echo "Stopping controller..."
ssh "${controller}" 'pkill -u "$(whoami)" controller'

echo "Stopping computation manager..."
ssh "${computationManager}" 'pkill -u "$(whoami)" computationManager'

echo "Stopping Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" 'pkill -u "$(whoami)" storagenode'
done

echo "Done!"
