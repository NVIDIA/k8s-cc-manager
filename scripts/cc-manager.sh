#!/bin/bash

set -x

NODE_NAME=${NODE_NAME:?"Missing NODE_NAME env"}
CC_CAPABLE_DEVICE_IDS=${CC_CAPABLE_DEVICE_IDS:?"Missing CC_CAPABLE_DEVICE_IDS env"}
SANDBOX_VALIDATOR_DEPLOYED=""
SANDBOX_PLUGIN_DEPLOYED=""
VGPU_DEVICE_MANAGER_DEPLOYED=""
VGPU_MANAGER_DEPLOYED=""
VFIO_MANAGER_DEPLOYED=""
PAUSED_STR="paused-for-cc-mode-change"

# declare an empty array for cc capable GPUs
gpus=()
# device-ids of cc capable GPUs
device_ids=()

_populate_cc_capable_device_ids() {
    # split CC_CAPABLE_DEVICE_IDS and populate device_ids
    device_ids=($(echo $CC_CAPABLE_DEVICE_IDS | tr "," "\n"))
    if [ ${#device_ids[@]} -eq 0 ]; then
        echo "no cc capable device ids were passed"
        return 1
    fi
    return 0
}

_reset_gpu_after_cc_mode() {
    local gpu=$1
    python3 /usr/bin/gpu_cc_tool.py --reset-after-cc-mode-switch --gpu-bdf=$gpu
    if [ $? -ne 0 ]; then
        echo "unable to reset gpu $gpu for cc mode switch, output"
        return 1
    fi
    echo "successfully reset gpu $gpu after cc mode switch"
    return 0
}

_unbind_device_from_driver() {
    local gpu=$1
    local existing_driver_name
    local existing_driver
    [ -e "/sys/bus/pci/devices/$gpu/driver" ] || return 0
    existing_driver=$(readlink -f "/sys/bus/pci/devices/$gpu/driver")
    existing_driver_name=$(basename "$existing_driver")
    echo "unbinding device $gpu from driver $existing_driver_name"
    echo "$gpu" > "$existing_driver/unbind"
    echo > /sys/bus/pci/devices/$gpu/driver_override
}

_unbind_device() {
    local gpu=$1
    echo "unbinding device $gpu"
    _unbind_device_from_driver $gpu
}

_get_all_cc_capable_gpus() {
    # identify cc capable gpus on the node
    for dev in /sys/bus/pci/devices/*; do
        read vendor < $dev/vendor
        if [ "$vendor" != "0x10de" ]; then
            continue
        fi
        read class < $dev/class
        if [ "$class" != "0x030000" ] && [ "$class" != "0x030200" ]; then
            continue
        fi
        local pci_bdf=$(basename $dev)

        if ! _array_contains gpus "$pci_bdf"; then
            # Add new element at the end of the array
            gpus+=("$pci_bdf")
        fi
    done
}

_get_device_id() {
    local $gpu=$1
    local dev_path=/sys/bus/pci/devices/$gpu/device 
    if [ -e $dev_path ]; then
        device_id=$(cat $dev_path)
        return 0
    fi
    echo "device path $dev_path doesn't exist for $gpu"
    return 1
}

_array_contains () {
    local array="$1[@]"
    local seeking=$2
    local in=1
    for element in "${!array}"; do
        if [[ $element == "$seeking" ]]; then
            in=0
            break
        fi
    done
    return $in
}

_is_cc_capable_gpu() {
    local $gpu=$1
    device_id=$(_get_device_id $gpu)
    if [ $? -ne 0 ]; then
        return 1
    fi
    _array_contains device_ids "$device_id" && return 0 || return 1
}

_is_valid_mode() {
    local mode=$1

    case $mode in
    "on" | "off" | "devtools")
        return 0
        ;;
    *)
        echo "unknown mode $mode"
        return 1
        ;;
    esac
}

_parse_mode() {
    output=$1
    mode=$(echo $output | grep "CC mode is" | sed 's/^.* CC mode is/CC mode is/g' | awk '{print $4}')
    if _is_valid_mode $mode; then
        echo $mode
        return 0
    fi
    echo "error parsing CC mode from the output $outupt, invalid mode $mode obtained"
    return 1
}

_assert_cc_mode() {
    local mode=$1
    for gpu in "${gpus[@]}"
    do
        echo "asserting cc mode $CC_MODE of gpu $gpu"
        if ! _assert_gpu_cc_mode $gpu $mode; then
            return 1
        fi
    done
    return 0
}

_assert_gpu_cc_mode() {
    local gpu=$1
    local mode=$2

    output=$(python3 /usr/bin/gpu_cc_tool.py --query-cc-mode --gpu-bdf=$gpu 2>&1)
    if [ $? -ne 0 ]; then
        _exit_failed
    fi
    current_mode=$(_parse_mode "$output")
    if [ $? -ne 0 ]; then
        return 1
    fi

    if [ "$current_mode" = "$mode" ]; then
        echo "current mode of $gpu is asserted to be mode $mode"
        return 0
    fi

    echo "current mode $current_mode of $gpu is asserted to be different than $mode"
    return 1
}

# Only return 'paused-*' if the value passed in is != 'false'. It should only
# be 'false' if some external entity has forced it to this value, at which point
# we want to honor it's existing value and not change it.
_maybe_set_paused() {
    local current_value="${1}"
    if [  "${current_value}" = "" ]; then
        # disabled by user with empty value, retain it
        echo ""
    elif [  "${current_value}" = "false" ]; then
        # disabled by user
        echo "false"
    elif [  "${current_value}" = "true" ]; then
        # disable
        echo "${PAUSED_STR}"
    elif [[ "${current_value}" == *"${PAUSED_STR}"* ]]; then
        # already added paused status for driver upgrade
        echo "${current_value}"
    else
        # append paused status for driver upgrade
        echo "${current_value}_${PAUSED_STR}"
    fi
}

# Only return 'true' if the value passed in is != 'false'. It should only
# be 'false' if some external entity has forced it to this value, at which point
# we want to honor it's existing value and not change it.
_maybe_set_true() {
    local current_value="${1}"
    if [ "${current_value}" = "false" ]; then
        # disabled by user
        echo "false"
    elif [ "${current_value}" = "${PAUSED_STR}" ]; then
        # enable the component
        echo "true"
    else
        # revert back to original label
        echo "${current_value}" | sed -r "s/${PAUSED_STR}//g" | tr -d "_"
    fi
}

_exit_failed() {
    # reschedule all operands during any failures
    _reschedule_gpu_operator_components

    exit 1
}

_fetch_current_labels() {
    echo "Getting current value of the 'nvidia.com/gpu.deploy.vfio-manager' node label"
    VFIO_MANAGER_DEPLOYED=$(kubectl get nodes ${NODE_NAME} -o=jsonpath='{$.metadata.labels.nvidia\.com/gpu\.deploy\.vfio-manager}')
    if [ "${?}" != "0" ]; then
        echo "Unable to get the value of the 'nvidia.com/gpu.deploy.vfio-manager' label"
        exit 1
    fi
    echo "Current value of 'nvidia.com/gpu.deploy.vfio-manager=${VFIO_MANAGER_DEPLOYED}'"

    echo "Getting current value of the 'nvidia.com/gpu.deploy.vgpu-manager' node label"
    VGPU_MANAGER_DEPLOYED=$(kubectl get nodes ${NODE_NAME} -o=jsonpath='{$.metadata.labels.nvidia\.com/gpu\.deploy\.vgpu-manager}')
    if [ "${?}" != "0" ]; then
        echo "Unable to get the value of the 'nvidia.com/gpu.deploy.vgpu-manager' label"
        exit 1
    fi
    echo "Current value of 'nvidia.com/gpu.deploy.vgpu-manager=${VGPU_MANAGER_DEPLOYED}'"

    echo "Getting current value of the 'nvidia.com/gpu.deploy.sandbox-validator' node label"
    SANDBOX_VALIDATOR_DEPLOYED=$(kubectl get nodes ${NODE_NAME} -o=jsonpath='{$.metadata.labels.nvidia\.com/gpu\.deploy\.sandbox-validator}')
    if [ "${?}" != "0" ]; then
        echo "Unable to get the value of the 'nvidia.com/gpu.deploy.sandbox-validator' label"
        exit 1
    fi
    echo "Current value of 'nvidia.com/gpu.deploy.sandbox-validator=${SANDBOX_VALIDATOR_DEPLOYED}'"

    echo "Getting current value of the 'nvidia.com/gpu.deploy.sandbox-device-plugin' node label"
    SANDBOX_PLUGIN_DEPLOYED=$(kubectl get nodes ${NODE_NAME} -o=jsonpath='{$.metadata.labels.nvidia\.com/gpu\.deploy\.sandbox-device-plugin}')
    if [ "${?}" != "0" ]; then
        echo "Unable to get the value of the 'nvidia.com/gpu.deploy.sandbox-device-plugin' label"
        exit 1
    fi
    echo "Current value of 'nvidia.com/gpu.deploy.sandbox-device-plugin=${SANDBOX_PLUGIN_DEPLOYED}'"

    echo "Getting current value of the 'nvidia.com/gpu.deploy.vgpu-device-manager' node label"
    VGPU_DEVICE_MANAGER_DEPLOYED=$(kubectl get nodes ${NODE_NAME} -o=jsonpath='{$.metadata.labels.nvidia\.com/gpu\.deploy\.vgpu-device-manager}')
    if [ "${?}" != "0" ]; then
        echo "Unable to get the value of the 'nvidia.com/gpu.deploy.vgpu-device-manager' label"
        exit 1
    fi
    echo "Current value of 'nvidia.com/gpu.deploy.vgpu-device-manager=${VGPU_DEVICE_MANAGER_DEPLOYED}'"
}

_evict_gpu_operator_components() {
    echo "Shutting down all GPU clients on the current node by disabling their component-specific nodeSelector labels"
    kubectl label --overwrite \
        node ${NODE_NAME} \
        nvidia.com/gpu.deploy.vfio-manager=$(_maybe_set_paused ${VFIO_MANAGER_DEPLOYED}) \
        nvidia.com/gpu.deploy.vgpu-manager=$(_maybe_set_paused ${VGPU_MANAGER_DEPLOYED}) \
        nvidia.com/gpu.deploy.sandbox-device-plugin=$(_maybe_set_paused ${SANDBOX_PLUGIN_DEPLOYED}) \
        nvidia.com/gpu.deploy.sandbox-validator=$(_maybe_set_paused ${SANDBOX_VALIDATOR_DEPLOYED}) \
        nvidia.com/gpu.deploy.vgpu-device-manager=$(_maybe_set_paused ${VGPU_DEVICE_MANAGER_DEPLOYED})

    if [ "$?" != "0" ]; then
        return 1
    fi

    if [ "${VFIO_MANAGER_DEPLOYED}" != "" ]; then
        echo "Waiting for vfio-manager to shutdown"
        kubectl wait --for=delete pod \
            --timeout=5m \
            --field-selector "spec.nodeName=${NODE_NAME}" \
            -n ${OPERATOR_NAMESPACE} \
            -l app=nvidia-vfio-manager
    fi

    if [ "${VGPU_MANAGER_DEPLOYED}" != "" ]; then
        echo "Waiting for vgpu-manager to shutdown"
        kubectl wait --for=delete pod \
            --timeout=5m \
            --field-selector "spec.nodeName=${NODE_NAME}" \
            -n ${OPERATOR_NAMESPACE} \
            -l app=nvidia-vgpu-manager
    fi

    if [ "${SANDBOX_VALIDATOR_DEPLOYED}" != "" ]; then
        echo "Waiting for sandbox-validator to shutdown"
        kubectl wait --for=delete pod \
            --timeout=5m \
            --field-selector "spec.nodeName=${NODE_NAME}" \
            -n ${OPERATOR_NAMESPACE} \
            -l app=nvidia-sandbox-validator
    fi

    if [ "${SANDBOX_PLUGIN_DEPLOYED}" != "" ]; then
        echo "Waiting for sandbox-device-plugin to shutdown"
        kubectl wait --for=delete pod \
            --timeout=5m \
            --field-selector "spec.nodeName=${NODE_NAME}" \
            -n ${OPERATOR_NAMESPACE} \
            -l app=nvidia-sandbox-device-plugin-daemonset
    fi

    if [ "${VGPU_DEVICE_MANAGER_DEPLOYED}" != "" ]; then
        echo "Waiting for vgpu-device-manager to shutdown"
        kubectl wait --for=delete pod \
            --timeout=5m \
            --field-selector "spec.nodeName=${NODE_NAME}" \
            -n ${OPERATOR_NAMESPACE} \
            -l app=nvidia-vgpu-device-manager
    fi
    return 0
}

_reschedule_gpu_operator_components() {
    echo "Rescheduling all GPU clients on the current node by enabling their component-specific nodeSelector labels"
    kubectl label --overwrite \
        node ${NODE_NAME} \
        nvidia.com/gpu.deploy.vfio-manager=$(_maybe_set_true ${VFIO_MANAGER_DEPLOYED}) \
        nvidia.com/gpu.deploy.vgpu-manager=$(_maybe_set_true ${VGPU_MANAGER_DEPLOYED}) \
        nvidia.com/gpu.deploy.sandbox-validator=$(_maybe_set_true ${SANDBOX_VALIDATOR_DEPLOYED}) \
        nvidia.com/gpu.deploy.sandbox-device-plugin=$(_maybe_set_true ${SANDBOX_PLUGIN_DEPLOYED}) \
        nvidia.com/gpu.deploy.vgpu-device-manager=$(_maybe_set_true ${VGPU_DEVICE_MANAGER_DEPLOYED})

    if [ "$?" != "0" ]; then
        return 1
    fi
    return 0
}

set_cc_mode() {
    # return here is no cc capable gpus are available on the node
    if [ ${#gpus[@]} -eq 0 ]; then
        return 0
    fi

    # assert current mode on all capable gpus
    if _assert_cc_mode $CC_MODE; then
        echo "all capable gpus already have the cc mode setting of $CC_MODE"
        return 0
    fi

    # evict all operands before cc mode changes
    _evict_gpu_operator_components || return 1

    for gpu in "${gpus[@]}"
    do
        echo "unbinding gpu $gpu"
        if ! _unbind_device $gpu; then
            _exit_failed
        fi
        echo "setting cc mode of gpu $gpu to $CC_MODE"
        if ! set_gpu_cc_mode $gpu; then
            echo "Changing the 'nvidia.com/cc.mode.state' node label to 'failed'"
            kubectl label --overwrite  \
                node ${NODE_NAME} \
                nvidia.com/cc.mode.state="failed"
            if [ "${?}" != "0" ]; then
                echo "Unable to set the value of 'nvidia.com/cc.mode.state' to 'failed'"
            fi
            _exit_failed
        fi
    done

    echo "Changing the 'nvidia.com/cc.mode.state' node label to 'success'"
    kubectl label --overwrite  \
        node ${NODE_NAME} \
        nvidia.com/cc.mode.state="success"
    if [ "${?}" != "0" ]; then
        echo "Unable to set the value of 'nvidia.com/cc.mode.state' to 'success'"
    fi

    # reschedule all operands
    _reschedule_gpu_operator_components || return 1

    return 0
}

set_gpu_cc_mode() {
    local gpu=$1
    local mode=$CC_MODE

    if ! _assert_gpu_cc_mode $gpu $mode; then
        output=$(python3 /usr/bin/gpu_cc_tool.py --set-cc-mode=$mode --reset-after-cc-mode-switch --gpu-bdf=$gpu 2>&1)
        if [ $? -ne 0 ]; then
            echo "unable to set cc mode of gpu $gpu to $mode, output $output"
            return 1
        fi

        if _assert_gpu_cc_mode $gpu $mode; then
            echo "successfully set cc mode of gpu $gpu to $mode"
            return 0
        fi
        echo "failed to set cc mode of gpu $gpu to $mode"
        return 1
    else
        echo "cc mode of gpu $gpu is already set to $mode"
        return 0
    fi
}

get_cc_mode() {
    local mode=""

    # return here is no cc capable gpus are available on the node
    if [ ${#gpus[@]} -eq 0 ]; then
        echo "off"
        return 0
    fi

    for gpu in "${gpus[@]}"
    do
        echo "getting cc mode of gpu $gpu"
        current_mode=$(get_gpu_cc_mode $gpu)
        if [ $? -ne 0 ]; then
            echo "unable to get cc mode of gpu $gpu"
            return 1
        fi
        if [ "$mode" != "" ] && [ "$current_mode" != "$mode" ]; then
            echo "gpus have different cc mode on the node $mode, $current_mode"
            return 1
        fi
        mode=$current_mode
    done

    echo "current cc mode of all supported gpus is $mode"
    return 0
}

get_gpu_cc_mode() {
    local gpu=$1
    output=$(python3 /usr/bin/gpu_cc_tool.py --query-cc-mode --gpu-bdf=$gpu 2>&1)
    if [ $? -ne 0 ]; then
        echo "unable to get cc mode of gpu $gpu, output $output"
        return 1
    fi
    mode=$(_parse_mode "$output")
    if [ $? -ne 0 ]; then
        echo "unable to parse cc mode of gpu $gpu, output $output"
        return 1
    fi

    echo "$mode"
    return 0
}

handle_set_cc_mode() {
    if [ "$DEVICE_ID" != "" ]; then
        set_gpu_cc_mode $DEVICE_ID
    elif [ "$ALL_DEVICES" = "true" ]; then
        set_cc_mode
    else
        usage
    fi
}

handle_get_cc_mode() {
    if [ "$DEVICE_ID" != "" ]; then
        get_gpu_cc_mode $DEVICE_ID
    elif [ "$ALL_DEVICES" = "true" ]; then
        get_cc_mode
    else
        usage
    fi
}

usage() {
    cat >&2 <<-EOF
    Usage: $0 COMMAND [ARG...]

    Commands:
    set-cc-mode [-a | --all] [-d | --device-id] [-m | --mode]
    get-cc-mode [-a | --all] [-d | --device-id]
    help [-h]
EOF
    exit 0
}

if [ $# -eq 0 ]; then
    usage
fi

command=$1; shift
case "${command}" in
    set-cc-mode) options=$(getopt -o ad:m: --long all,device-id,mode: -- "$@");;
    get-cc-mode ) options=$(getopt -o ad: --long all,device-id: -- "$@");;
    help) options="" ;;
    *) usage ;;
esac
if [ $? -ne 0 ]; then
    usage
fi

eval set -- "${options}"

DEVICE_ID=""
for opt in ${options}; do
    case "$opt" in
    -a | --all) ALL_DEVICES=true; shift 1 ;;
    -d | --device-id) DEVICE_ID=$2; shift 2 ;;
    -m | --mode) CC_MODE=$2; shift 2 ;;
    -h | --help) shift;;
    --) shift; break ;;
    esac
done
if [ $# -ne 0 ]; then
    usage
fi

# populate all cc capable gpus that we are interested in
_populate_cc_capable_device_ids || exit 1

# get all cc capable gpus
_get_all_cc_capable_gpus || exit 1

# fetch current values of operand deployment labels
_fetch_current_labels || exit 1

if [ "$command" = "help" ]; then
    usage
elif [ "$command" = "set-cc-mode" ]; then
    handle_set_cc_mode || exit 1
elif [ "$command" = "get-cc-mode" ]; then
    handle_get_cc_mode || exit 1
else
    echo "Unknown function: $command"
    exit 1
fi

# indicate cc manager readiness
touch /run/nvidia/validations/.cc-manager-ctr-ready
