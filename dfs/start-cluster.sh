#!/usr/bin/env bash
set -x

script_dir="$(cd "$(dirname "$0")" && pwd)"
log_dir="${script_dir}/logs"

source "${script_dir}/nodes.sh"

echo "Installing..."
go install controller/controller.go   || exit 1 # Exit if compile+install fails
go install storage/storagenode.go || exit 1 # Exit if compile+install fails
echo "Done!"

echo "Creating log directory: ${log_dir}"
mkdir -pv "${log_dir}"

echo "Starting Controller..."
ssh "${controller}" "${HOME}/go/bin/controller" &> "${log_dir}/controller.log" &

# give 2 seconds for controller to fully start
sleep 2

echo "Starting Computation Manager..."
ssh "${computationManager}" "${HOME}/go/bin/computationManager" &> "${log_dir}/computationManager.log" &

echo "Starting Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    me=$(whoami)
    ssh "${node}" "${HOME}/go/bin/storagenode /bigdata/${me}/storage ${controller} :20110" &> "${log_dir}/${node}.log" &
done

echo "Startup complete!"
