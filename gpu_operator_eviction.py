"""
GPU Operator Component Eviction

This module provides functionality to evict GPU operator components
before CC mode changes and reschedule them afterward.

This is needed because GPU operator components (device plugin, vfio-manager, etc.)
need to be stopped before changing GPU CC mode.
"""

import logging
import time
from typing import Dict, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException


logger = logging.getLogger(__name__)


# GPU operator component labels
COMPONENT_LABELS = [
    'nvidia.com/gpu.deploy.vfio-manager',
    'nvidia.com/gpu.deploy.vgpu-manager',
    'nvidia.com/gpu.deploy.sandbox-validator',
    'nvidia.com/gpu.deploy.sandbox-device-plugin',
    'nvidia.com/gpu.deploy.vgpu-device-manager',
]

# Corresponding app labels for waiting
COMPONENT_APP_LABELS = {
    'nvidia.com/gpu.deploy.vfio-manager': 'nvidia-vfio-manager',
    'nvidia.com/gpu.deploy.vgpu-manager': 'nvidia-vgpu-manager',
    'nvidia.com/gpu.deploy.sandbox-validator': 'nvidia-sandbox-validator',
    'nvidia.com/gpu.deploy.sandbox-device-plugin': 'nvidia-sandbox-device-plugin-daemonset',
    'nvidia.com/gpu.deploy.vgpu-device-manager': 'nvidia-vgpu-device-manager',
}

PAUSED_STR = 'paused-for-cc-mode-change'


def _maybe_set_paused(current_value: Optional[str]) -> str:
    """
    Convert a component label value to its 'paused' equivalent.
    
    Only pauses if not explicitly disabled by user (i.e., != 'false').
    
    Args:
        current_value: Current label value
        
    Returns:
        Paused version of the label value
    """
    if current_value == '' or current_value is None:
        # Disabled by user with empty value, retain it
        return ''
    elif current_value == 'false':
        # Disabled by user
        return 'false'
    elif current_value == 'true':
        # Enable -> pause
        return PAUSED_STR
    elif PAUSED_STR in current_value:
        # Already paused
        return current_value
    else:
        # Append paused status
        return f"{current_value}_{PAUSED_STR}"


def _maybe_set_unpaused(current_value: Optional[str]) -> str:
    """
    Convert a 'paused' component label value back to its enabled equivalent.
    
    Only unpause if not explicitly disabled by user (i.e., != 'false').
    
    Args:
        current_value: Current label value
        
    Returns:
        Unpaused version of the label value
    """
    if current_value == 'false':
        # Disabled by user, keep disabled
        return 'false'
    elif current_value == PAUSED_STR:
        # Paused -> enable
        return 'true'
    elif current_value and PAUSED_STR in current_value:
        # Revert to original label (remove pause suffix)
        return current_value.replace(f"_{PAUSED_STR}", '').replace(PAUSED_STR, '').strip('_')
    else:
        # Return as-is
        return current_value or ''


def fetch_current_component_labels(v1: client.CoreV1Api, node_name: str) -> Dict[str, str]:
    """
    Fetch current values of GPU operator component deployment labels.
    
    Args:
        v1: Kubernetes CoreV1Api client
        node_name: Name of the node
        
    Returns:
        Dictionary mapping label names to their current values
        
    Raises:
        ApiException: If unable to read node labels
    """
    logger.info(f"Fetching GPU operator component labels from node '{node_name}'")
    
    try:
        node = v1.read_node(node_name)
        labels = node.metadata.labels or {}
        
        component_labels = {}
        for label_name in COMPONENT_LABELS:
            value = labels.get(label_name, '')
            component_labels[label_name] = value
            logger.info(f"  {label_name}={value}")
        
        return component_labels
        
    except ApiException as e:
        logger.error(f"Failed to fetch node labels: {e}")
        raise


def evict_gpu_operator_components(
    v1: client.CoreV1Api,
    node_name: str,
    operator_namespace: str,
    current_labels: Dict[str, str],
    timeout: int = 300
) -> bool:
    """
    Evict GPU operator components by pausing their deployment labels.
    
    This sets component deployment labels to 'paused' values, causing
    the operator to delete the corresponding pods on this node.
    
    Args:
        v1: Kubernetes CoreV1Api client
        node_name: Name of the node
        operator_namespace: Namespace where GPU operator is deployed (e.g., 'gpu-operator')
        current_labels: Current component label values (from fetch_current_component_labels)
        timeout: Timeout in seconds to wait for pods to be deleted
        
    Returns:
        True if successful, False otherwise
    """
    logger.info("Evicting GPU operator components by setting deployment labels to 'paused'")
    
    try:
        # Prepare paused label values
        paused_labels = {}
        for label_name, current_value in current_labels.items():
            paused_value = _maybe_set_paused(current_value)
            paused_labels[label_name] = paused_value
            logger.debug(f"  {label_name}: '{current_value}' -> '{paused_value}'")
        
        # Apply paused labels
        node = v1.read_node(node_name)
        if node.metadata.labels is None:
            node.metadata.labels = {}
        
        node.metadata.labels.update(paused_labels)
        v1.patch_node(node_name, node)
        logger.info("Successfully set deployment labels to 'paused' values")
        
        # Wait for pods to be deleted
        for label_name, current_value in current_labels.items():
            if not current_value:
                # Component not deployed, skip
                continue
            
            app_label = COMPONENT_APP_LABELS.get(label_name)
            if not app_label:
                continue
            
            logger.info(f"Waiting for {app_label} pods to be deleted...")
            
            # Wait for pods with this app label on this node to be deleted
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    pods = v1.list_namespaced_pod(
                        namespace=operator_namespace,
                        field_selector=f'spec.nodeName={node_name}',
                        label_selector=f'app={app_label}'
                    )
                    
                    if len(pods.items) == 0:
                        logger.info(f"  {app_label} pods deleted")
                        break
                    else:
                        logger.debug(f"  Still waiting for {len(pods.items)} {app_label} pod(s)...")
                        time.sleep(2)
                        
                except ApiException as e:
                    logger.warning(f"Error checking pod status: {e}")
                    time.sleep(2)
            else:
                logger.warning(f"Timeout waiting for {app_label} pods to be deleted")
                # Don't fail - continue anyway
        
        logger.info("All GPU operator components evicted")
        return True
        
    except ApiException as e:
        logger.error(f"Failed to evict GPU operator components: {e}")
        return False


def reschedule_gpu_operator_components(
    v1: client.CoreV1Api,
    node_name: str,
    original_labels: Dict[str, str]
) -> bool:
    """
    Reschedule GPU operator components by restoring their deployment labels.
    
    This restores component deployment labels from 'paused' to their original values,
    causing the operator to reschedule the corresponding pods on this node.
    
    Args:
        v1: Kubernetes CoreV1Api client
        node_name: Name of the node
        original_labels: Original component label values (from fetch_current_component_labels)
        
    Returns:
        True if successful, False otherwise
    """
    logger.info("Rescheduling GPU operator components by restoring deployment labels")
    
    try:
        # Prepare unpaused label values
        unpaused_labels = {}
        for label_name, original_value in original_labels.items():
            unpaused_value = _maybe_set_unpaused(original_value)
            unpaused_labels[label_name] = unpaused_value
            logger.debug(f"  {label_name}: '{original_value}' -> '{unpaused_value}'")
        
        # Apply unpaused labels
        node = v1.read_node(node_name)
        if node.metadata.labels is None:
            node.metadata.labels = {}
        
        node.metadata.labels.update(unpaused_labels)
        v1.patch_node(node_name, node)
        logger.info("Successfully restored deployment labels")
        
        return True
        
    except ApiException as e:
        logger.error(f"Failed to reschedule GPU operator components: {e}")
        return False


def set_cc_mode_state_label(v1: client.CoreV1Api, node_name: str, state: str) -> bool:
    """
    Set the nvidia.com/cc.mode.state label on the node.
    
    Args:
        v1: Kubernetes CoreV1Api client
        node_name: Name of the node
        state: State value ('success' or 'failed')
        
    Returns:
        True if successful, False otherwise
    """
    try:
        node = v1.read_node(node_name)
        if node.metadata.labels is None:
            node.metadata.labels = {}
        
        node.metadata.labels['nvidia.com/cc.mode.state'] = state
        v1.patch_node(node_name, node)
        logger.info(f"Set nvidia.com/cc.mode.state={state}")
        return True
        
    except ApiException as e:
        logger.error(f"Failed to set cc.mode.state label: {e}")
        return False

