/*
Copyright 2022 The Koordinator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nm

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/pleg"
	statesinformer "github.com/koordinator-sh/koordinator/pkg/koordlet/statesinformer/impl"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/util/system"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/goyarn/pkg/yarn/copilot/utils"
)

const (
	MemoryMoveChargeAtImmigrateName = "memory.move_charge_at_immigrate"
)

type NodeMangerOperator struct {
	CgroupRoot string
	CgroupPath string

	SyncMemoryCgroup bool

	containerWatch pleg.Watcher
	nmPodWatcher   *NMPodWatcher
	NMEndpoint     string //localhost:8042
	client         *resty.Client
	ticker         *time.Ticker
	nmTicker       *time.Ticker
}

func NewNodeMangerOperator(cgroupRoot string, cgroupPath string, syncMemoryCgroup bool, endpoint string, syncPeriod time.Duration, kubelet statesinformer.KubeletStub) (*NodeMangerOperator, error) {
	watcher, err := pleg.NewWatcher()
	if err != nil {
		return nil, err
	}
	cli := resty.New()
	cli.SetBaseURL(fmt.Sprintf("http://%s", endpoint))
	w := NewNMPodWater(kubelet)
	return &NodeMangerOperator{
		CgroupRoot:       cgroupRoot,
		CgroupPath:       cgroupPath,
		SyncMemoryCgroup: syncMemoryCgroup,
		containerWatch:   watcher,
		NMEndpoint:       endpoint,
		client:           cli,
		nmPodWatcher:     w,
		ticker:           time.NewTicker(syncPeriod),
		nmTicker:         time.NewTicker(time.Second),
	}, nil
}

func (n *NodeMangerOperator) Run(stop <-chan struct{}) error {
	klog.Infof("Run node manager operator")
	if n.SyncMemoryCgroup {
		return n.syncMemoryCgroup(stop)
	}
	return nil
}

func (n *NodeMangerOperator) syncMemoryCgroup(stop <-chan struct{}) error {
	cpuDir := filepath.Join(n.CgroupRoot, system.CgroupCPUDir, n.CgroupPath)
	if err := n.ensureCgroupDir(cpuDir); err != nil {
		klog.Error(err)
		return err
	}
	if err := n.containerWatch.AddWatch(cpuDir); err != nil {
		return err
	}
	klog.Infof("watch dir %s", cpuDir)
	memoryDir := filepath.Join(n.CgroupRoot, system.CgroupMemDir, n.CgroupPath)
	if err := n.ensureCgroupDir(memoryDir); err != nil {
		klog.Error(err)
		return err
	}
	for {
		select {
		case event := <-n.containerWatch.Event():
			switch pleg.TypeOf(event) {
			case pleg.DirCreated:
				n.createMemoryCgroup(event.Name)
			case pleg.DirRemoved:
				n.removeMemoryCgroup(event.Name)
			default:
				klog.V(5).Infof("skip %v unknown event", event.Name)
			}
		case <-n.ticker.C:
			n.syncNoneProcCgroup()
			n.syncAllCgroup()
		case <-n.nmTicker.C:
			n.syncNMEndpoint()
		case <-stop:
			return nil
		}
	}
}

func (n *NodeMangerOperator) syncNoneProcCgroup() {
	klog.V(5).Info("syncNoneProcCgroup")
	cpuPath := n.GenerateCgroupFullPath(system.CgroupCPUDir)
	_ = filepath.Walk(cpuPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			klog.Warningf("ignore file %s error:%s", path, err.Error())
			return err
		}
		if info.IsDir() && path != cpuPath {
			read, err := system.CommonFileRead(filepath.Join(path, system.CPUProcsName))
			if err != nil {
				klog.Error(err)
				return filepath.SkipDir
			}
			if len(read) != 0 {
				return filepath.SkipDir
			}
			klog.V(5).Infof("detect anomaly cgroup path: %s, try to remove", path)
			if err = os.RemoveAll(path); err != nil {
				klog.Error(err)
				return filepath.SkipDir
			}
			return filepath.SkipDir
		}
		return nil
	})
}

func (n *NodeMangerOperator) syncNMEndpoint() {
	endpoint, exist, err := n.nmPodWatcher.GetNMPodEndpoint()
	if err != nil {
		klog.Error(err)
		return
	}
	if n.NMEndpoint == endpoint {
		return
	}
	n.NMEndpoint = endpoint
	klog.V(5).Infof("found endpoint %s", endpoint)
	if exist {
		n.client.SetBaseURL(fmt.Sprintf("http://%s", endpoint))
	}
}

func (n *NodeMangerOperator) syncAllCgroup() {
	subDirFunc := func(dir string) map[string]struct{} {
		res := map[string]struct{}{}
		_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				klog.Warningf("ignore file %s error:%s", path, err.Error())
				return err
			}
			if info.IsDir() && path != dir {
				res[path] = struct{}{}
				return filepath.SkipDir
			}
			return nil
		})
		return res
	}
	cpuList := subDirFunc(filepath.Join(n.CgroupRoot, system.CgroupCPUDir, n.CgroupPath))
	memList := subDirFunc(filepath.Join(n.CgroupRoot, system.CgroupMemDir, n.CgroupPath))
	toCreate, toDelete := utils.DiffMap(cpuList, memList)
	for path := range toCreate {
		n.createMemoryCgroup(path)
	}
	for path := range toDelete {
		n.removeMemoryCgroup(path)
	}
}

func (n *NodeMangerOperator) removeMemoryCgroup(fileName string) {
	klog.V(5).Infof("receive file delete event %s", fileName)
	basename := filepath.Base(fileName)
	if !strings.HasPrefix(basename, "container_") {
		klog.V(5).Infof("skip file %s, which is not a yarn container file", basename)
		return
	}
	memCgroupPath := filepath.Join(n.CgroupRoot, system.CgroupMemDir, n.CgroupPath, basename)
	if err := os.RemoveAll(memCgroupPath); err != nil {
		klog.Error("fail to remove memory dir: %s, error: %s", memCgroupPath, err.Error())
		return
	}
	klog.V(5).Infof("yarn container dir %v removed", basename)
}

func (n *NodeMangerOperator) createMemoryCgroup(fileName string) {
	klog.V(5).Infof("receive file create event %s", fileName)
	basename := filepath.Base(fileName)
	if !strings.HasPrefix(basename, "container_") {
		klog.V(5).Infof("skip file %s, which is not a yarn container file", basename)
		return
	}
	memCgroupPath := filepath.Join(n.CgroupRoot, system.CgroupMemDir, n.CgroupPath, basename)
	if err := os.Mkdir(memCgroupPath, 0644); err != nil {
		klog.Error("fail to create memory dir: %s, error: %s", memCgroupPath, err.Error())
		return
	}
	if _, err := system.CommonFileWriteIfDifferent(filepath.Join(memCgroupPath, MemoryMoveChargeAtImmigrateName), "3"); err != nil {
		klog.Error(err)
		return
	}
	if _, err := system.CommonFileWriteIfDifferent(filepath.Join(memCgroupPath, system.MemoryOomGroupName), "1"); err != nil {
		klog.Error(err)
		return
	}
	cpuCgroupPath := filepath.Join(n.CgroupRoot, system.CgroupCPUDir, n.CgroupPath, basename)
	pids, err := utils.GetPids(cpuCgroupPath)
	if err != nil {
		klog.Error(err)
		return
	}
	for _, pid := range pids {
		if err := system.CommonFileWrite(filepath.Join(memCgroupPath, system.CPUProcsName), strconv.Itoa(pid)); err != nil {
			klog.Error(err)
			return
		}
	}

	klog.V(5).Infof("yarn container dir %v created, sync pid", memCgroupPath)
	container, err := n.GetContainer(basename)
	if err != nil {
		klog.Error(err)
		return
	}
	memLimit := container.TotalMemoryNeededMB * 1024 * 1024
	_, err = system.CommonFileWriteIfDifferent(filepath.Join(memCgroupPath, system.MemoryLimitName), strconv.Itoa(memLimit))
	if err != nil {
		klog.Error(err)
		return
	}
	klog.V(5).Infof("set memory %s limit_in_bytes as %d", memCgroupPath, memLimit)
}

func (n *NodeMangerOperator) ensureCgroupDir(dir string) error {
	klog.V(5).Infof("ensure cgroup dir %s", dir)
	f, err := os.Stat(dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return err
		}
		return os.Chmod(dir, 0777)
	}
	if f.Mode().Perm() != 0777 {
		return os.Chmod(dir, 0777)
	}
	return nil
}

// KillContainer kill process group for target container
func (n *NodeMangerOperator) KillContainer(containerID string) error {
	processGroupID := n.getProcessGroupID(containerID)
	if processGroupID <= 1 {
		return fmt.Errorf("invalid process group pid(%d) for container %s", processGroupID, containerID)
	}
	return syscall.Kill(-processGroupID, syscall.SIGKILL)
}

func (n *NodeMangerOperator) getProcessGroupID(containerID string) int {
	containerCgroupPath := filepath.Join(n.CgroupRoot, "cpu", n.CgroupPath, containerID)
	pids, err := utils.GetPids(containerCgroupPath)
	if err != nil {
		klog.Error(err)
		return 0
	}
	if len(pids) == 0 {
		return 0
	}
	return pids[0]
}

type Containers struct {
	Containers struct {
		Items []YarnContainer `json:"container"`
	} `json:"containers"`
}

func (n *NodeMangerOperator) ListContainers() (*Containers, error) {
	var res Containers
	resp, err := n.client.R().SetResult(&res).Get("/ws/v1/node/containers")
	if err != nil {
		return nil, err
	}
	if !resp.IsSuccess() {
		return nil, fmt.Errorf("code for ListContainer is %d", resp.StatusCode())
	}
	return &res, nil
}

func (n *NodeMangerOperator) GetContainer(containerID string) (*YarnContainer, error) {
	listContainers, err := n.ListContainers()
	if err != nil {
		return nil, err
	}
	for _, c := range listContainers.Containers.Items {
		if c.Id == containerID {
			return &c, nil
		}
	}
	return nil, fmt.Errorf("container Not Found")
}

func (n *NodeMangerOperator) GenerateCgroupPath(containerID string) string {
	return filepath.Join(n.CgroupPath, containerID)
}

func (n *NodeMangerOperator) GenerateCgroupFullPath(cgroupSubSystem string) string {
	return filepath.Join(n.CgroupRoot, cgroupSubSystem, n.CgroupPath)
}
