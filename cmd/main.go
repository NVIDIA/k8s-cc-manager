/*
 * Copyright (c) 2021, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"

	log "github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	ResourceNodes     = "nodes"
	CCModeConfigLabel = "nvidia.com/cc.mode"
)

var (
	kubeconfigFlag    string
	defaultCCModeFlag string
)

type SyncableCCModeConfig struct {
	cond     *sync.Cond
	mutex    sync.Mutex
	current  string
	lastRead string
}

func NewSyncableCCModeConfig() *SyncableCCModeConfig {
	var m SyncableCCModeConfig
	m.cond = sync.NewCond(&m.mutex)
	return &m
}

func (m *SyncableCCModeConfig) Set(value string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.current = value
	m.cond.Broadcast()
}

func (m *SyncableCCModeConfig) Get() string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.lastRead == m.current {
		m.cond.Wait()
	}
	m.lastRead = m.current
	return m.lastRead
}

func main() {
	c := cli.NewApp()
	c.Before = validateFlagsOrEnv
	c.Action = start

	c.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "kubeconfig",
			Value:       "",
			Usage:       "absolute path to the kubeconfig file",
			Destination: &kubeconfigFlag,
			EnvVars:     []string{"KUBECONFIG"},
		},
		&cli.StringFlag{
			Name:        "default-cc-mode",
			Aliases:     []string{"m"},
			Value:       "",
			Usage:       "cc mode to be set by default when node label nvidia.com/cc.mode is not applied",
			Destination: &defaultCCModeFlag,
			EnvVars:     []string{"DEFAULT_CC_MODE"},
		},
	}

	err := c.Run(os.Args)
	if err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}

func validateFlagsOrEnv(c *cli.Context) error {
	if os.Getenv("NODE_NAME") == "" {
		return fmt.Errorf("NODE_NAME env must be set for k8s-cc-manager")
	}
	if os.Getenv("CC_CAPABLE_DEVICE_IDS") == "" {
		return fmt.Errorf("CC_CAPABLE_DEVICE_IDS env must be set for k8s-cc-manager")
	}
	return nil
}

func start(c *cli.Context) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigFlag)
	if err != nil {
		return fmt.Errorf("error building kubernetes clientcmd config: %s", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error building kubernetes clientset from config: %s", err)
	}

	// obtain CC mode label for the current node
	node, err := clientset.CoreV1().Nodes().Get(context.Background(), os.Getenv("NODE_NAME"), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error obtaining node labels from config: %s", err)
	}

	nodeLabels := node.GetLabels()
	if value, ok := nodeLabels[CCModeConfigLabel]; !ok || value == "" {
		// apply default CC mode config when per node nvidia.com/cc.mode label is not present or set to empty
		if defaultCCModeFlag != "" {
			log.Infof("Updating CC mode to : %s", defaultCCModeFlag)
			err := runScript(defaultCCModeFlag)
			if err != nil {
				log.Printf("Error: %v", err)
				os.Exit(1)
			}
			log.Infof("Successfuly updated to CC mode to %s", defaultCCModeFlag)
		}
	}

	ccModeConfig := NewSyncableCCModeConfig()
	stop := ContinuouslySyncCCModeConfigChanges(clientset, ccModeConfig)
	defer close(stop)

	// now watch for node specific label
	for {
		log.Infof("Waiting for change to '%s' label", CCModeConfigLabel)
		value := ccModeConfig.Get()
		if value == "" {
			// assume CC mode as default mode provided when the node label is deleted or set to empty
			value = defaultCCModeFlag
		}
		log.Infof("Updating CC mode to : %s", value)
		err := runScript(value)
		if err != nil {
			log.Errorf("Error: %s", err)
			continue
		}
		log.Infof("Successfully updated to CC mode to %s", value)
	}
}

func runScript(ccMode string) error {
	args := []string{
		"set-cc-mode",
		"-a",
		"-m", ccMode,
	}
	cmd := exec.Command("/usr/bin/cc-manager.sh", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func ContinuouslySyncCCModeConfigChanges(clientset *kubernetes.Clientset, ccModeConfig *SyncableCCModeConfig) chan struct{} {
	listWatch := cache.NewListWatchFromClient(
		clientset.CoreV1().RESTClient(),
		ResourceNodes,
		v1.NamespaceAll,
		fields.OneTermEqualSelector("metadata.name", os.Getenv("NODE_NAME")),
	)

	_, controller := cache.NewInformer(
		listWatch, &v1.Node{}, 0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ccModeConfig.Set(obj.(*v1.Node).Labels[CCModeConfigLabel])
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldLabel := oldObj.(*v1.Node).Labels[CCModeConfigLabel]
				newLabel := newObj.(*v1.Node).Labels[CCModeConfigLabel]
				if oldLabel != newLabel {
					ccModeConfig.Set(newLabel)
				}
			},
		},
	)

	stop := make(chan struct{})
	go controller.Run(stop)
	return stop
}
