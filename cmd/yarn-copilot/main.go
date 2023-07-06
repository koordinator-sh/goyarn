package main

import (
	"flag"
	"os"
	"time"

	"github.com/koordinator-sh/koordinator/pkg/koordlet/statesinformer"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"github.com/koordinator-sh/goyarn/cmd/yarn-copilot/options"
	"github.com/koordinator-sh/goyarn/pkg/yarn/copilot/nm"
	"github.com/koordinator-sh/goyarn/pkg/yarn/copilot/server"
)

func main() {
	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	conf := options.NewConfiguration()
	klog.InitFlags(f)
	f.StringVar(&conf.ServerEndpoint, "server-endpoint", conf.ServerEndpoint, "yarn copilot server endpoint.")
	f.StringVar(&conf.YarnContainerCgroupPath, "yarn-container-cgroup-path", conf.YarnContainerCgroupPath, "yarn container cgroup path.")
	f.StringVar(&conf.NodeMangerEndpoint, "node-manager-endpoint", conf.NodeMangerEndpoint, "node manger endpoint")
	f.BoolVar(&conf.SyncMemoryCgroup, "sync-memory-cgroup", conf.SyncMemoryCgroup, "true to sync cpu cgroup info to memory, used for hadoop 2.x")
	f.DurationVar(&conf.SyncCgroupPeriod, "sync-cgroup-period", conf.SyncCgroupPeriod, "period of resync all cpu/memory cgroup")
	f.StringVar(&conf.CgroupRootDir, "cgroup-root-dir", conf.CgroupRootDir, "cgroup root directory")
	help := f.Bool("help", false, "help information")

	if err := f.Parse(os.Args[1:]); err != nil {
		klog.Fatal(err)
	}
	if *help {
		f.Usage()
		os.Exit(0)
	}
	f.VisitAll(func(f *flag.Flag) {
		klog.Infof("args: %s = %s", f.Name, f.Value)
	})
	stopCtx := signals.SetupSignalHandler()
	kubelet, _ := statesinformer.NewKubeletStub("127.0.0.1", 10255, "http", time.Second*5, nil)
	operator, err := nm.NewNodeMangerOperator(conf.CgroupRootDir, conf.YarnContainerCgroupPath, conf.SyncMemoryCgroup, conf.NodeMangerEndpoint, conf.SyncCgroupPeriod, kubelet)
	if err != nil {
		klog.Fatal(err)
	}
	go func() {
		if err := operator.Run(stopCtx.Done()); err != nil {
			klog.Error(err)
		}
	}()
	err = server.NewYarnCopilotServer(operator, conf.ServerEndpoint).Run(stopCtx)
	if err != nil {
		klog.Fatal(err)
	}
}
