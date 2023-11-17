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

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/util/system"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/goyarn/pkg/copilot-agent/nm"
)

type YarnCopilotServer struct {
	mgr      *nm.NodeMangerOperator
	unixPath string
}

func NewYarnCopilotServer(mgr *nm.NodeMangerOperator, unixPath string) *YarnCopilotServer {
	return &YarnCopilotServer{mgr: mgr, unixPath: unixPath}
}

func (y *YarnCopilotServer) Run(ctx context.Context) error {
	e := gin.New()
	e.GET("/health", y.Health)
	e.GET("/information", y.Information)
	e.GET("/v1/container", y.GetContainer)
	e.GET("/v1/containers", y.ListContainers)
	e.POST("/v1/killContainer", y.KillContainer)
	e.POST("/v1/killContainersByResource", y.KillContainerByResource)

	server := &http.Server{
		Handler: e,
	}
	sockDir := filepath.Dir(y.unixPath)
	_ = os.MkdirAll(sockDir, os.ModePerm)
	if system.FileExists(y.unixPath) {
		_ = os.Remove(y.unixPath)
	}
	listener, err := net.Listen("unix", y.unixPath)
	if err != nil {
		fmt.Printf("Failed to listen UNIX socket: %v", err)
		os.Exit(1)
	}
	defer func() {
		_ = os.Remove(y.unixPath)
	}()
	go func() {
		_ = server.Serve(listener)
	}()
	//for {
	//	select {
	//	case <-ctx.Done():
	//
	//	}
	//}
	for range ctx.Done() {
		klog.Info("graceful shutdown")
		if err := server.Shutdown(ctx); err != nil {
			klog.Errorf("Server forced to shutdown: %v", err)
			return err
		}
	}
	return nil
}

func (y *YarnCopilotServer) Health(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, "ok")
}

type PluginInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func (y *YarnCopilotServer) Information(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, &PluginInfo{
		Name:    "yarn",
		Version: "v1",
	})
}

func (y *YarnCopilotServer) ListContainers(ctx *gin.Context) {
	listContainers, err := y.mgr.ListContainers()
	if err != nil {
		klog.Error(err)
		ctx.JSON(http.StatusBadRequest, err)
		return
	}
	res := make([]*ContainerInfo, 0, len(listContainers.Containers.Items))
	for _, container := range listContainers.Containers.Items {
		if container.IsFinalState() {
			continue
		}
		res = append(res, ParseContainerInfo(&container, y.mgr))
	}
	ctx.JSON(http.StatusOK, res)
}

func (y *YarnCopilotServer) GetContainer(ctx *gin.Context) {
	containerID := ctx.Query("containerID")
	container, err := y.mgr.GetContainer(containerID)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, err)
		return
	}
	ctx.JSON(http.StatusOK, ParseContainerInfo(container, y.mgr))
}

type KillRequest struct {
	ContainerID string          `json:"containerID,omitempty"`
	Resources   v1.ResourceList `json:"resources,omitempty"`
}

type KillInfo struct {
	Items []*ContainerInfo `json:"items,omitempty"`
}

type ContainerInfo struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	UID             string            `json:"uid"`
	Labels          map[string]string `json:"labels"`
	Annotations     map[string]string `json:"annotations"`
	Priority        int32             `json:"priority"`
	CreateTimestamp time.Time         `json:"createTimestamp"`

	CgroupDir   string                  `json:"cgroupDir"`
	HostNetwork bool                    `json:"hostNetwork"`
	Resources   v1.ResourceRequirements `json:"resources"`
}

func (y *YarnCopilotServer) KillContainer(ctx *gin.Context) {
	var kr KillRequest
	if err := ctx.BindJSON(&kr); err != nil {
		ctx.JSON(http.StatusBadRequest, err)
		return
	}
	container, err := y.mgr.GetContainer(kr.ContainerID)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, err)
		return
	}
	if err := y.mgr.KillContainer(kr.ContainerID); err != nil {
		ctx.JSON(http.StatusBadRequest, err)
		return
	}
	ctx.JSON(http.StatusOK, KillInfo{Items: []*ContainerInfo{ParseContainerInfo(container, y.mgr)}})
}

func (y *YarnCopilotServer) KillContainerByResource(ctx *gin.Context) {
}
