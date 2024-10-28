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

package main

import (
	"flag"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	utilclient "github.com/koordinator-sh/koordinator/pkg/util/client"
	"github.com/koordinator-sh/koordinator/pkg/util/fieldindex"
	"github.com/koordinator-sh/yarn-copilot/cmd/yarn-operator/options"
)

var (
	setupLog = ctrl.Log.WithName("setup")

	restConfigQPS   = flag.Int("rest-config-qps", 30, "QPS of rest config.")
	restConfigBurst = flag.Int("rest-config-burst", 50, "Burst of rest config.")
)

func main() {
	var metricsAddr, pprofAddr string
	var healthProbeAddr string
	var enableLeaderElection, enablePprof bool
	var leaderElectionNamespace string
	var namespace string
	var syncPeriodStr string
	var cacheSelectorKey, cacheSelectorValue, cacheSelectorNamespace string
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&healthProbeAddr, "health-probe-addr", ":8000", "The address the healthz/readyz endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", true, "Whether you need to enable leader election.")
	flag.StringVar(&leaderElectionNamespace, "leader-election-namespace", "koordinator-system",
		"This determines the namespace in which the leader election configmap will be created, it will use in-cluster namespace if empty.")
	flag.StringVar(&namespace, "namespace", "",
		"Namespace if specified restricts the manager's cache to watch objects in the desired namespace. Defaults to all namespaces.")
	flag.BoolVar(&enablePprof, "enable-pprof", true, "Enable pprof for controller manager.")
	flag.StringVar(&pprofAddr, "pprof-addr", ":8090", "The address the pprof binds to.")
	flag.StringVar(&syncPeriodStr, "sync-period", "", "Determines the minimum frequency at which watched resources are reconciled.")
	// By setting the following three parameters, the size of the field index cache can be effectively reduced,
	// thereby reducing the memory occupied by the koord-yarn-operator.
	// Warning: three parameters must be set at the same time to take effect.
	flag.StringVar(&cacheSelectorKey, "cache-selector-key", "app", "The cacheSelectorLabel should be the key that marks the label of the NMpod.")
	flag.StringVar(&cacheSelectorValue, "cache-selector-value", "hadoop-yarn", "The cacheSelectorValue should be the value that marks the label of the NMpod.")
	flag.StringVar(&cacheSelectorNamespace, "cache-selector-namespace", "yarn-k8s-elastic", "The cacheSelectorNamespace should be the namespace of NMpod.")

	opts := options.NewOptions()
	opts.InitFlags(flag.CommandLine)
	//sloconfig.InitFlags(flag.CommandLine)
	//utilfeature.DefaultMutableFeatureGate.DefaultMutableFeatureGateAddFlag(pflag.CommandLine)
	klog.InitFlags(nil)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	rand.Seed(time.Now().UnixNano())
	ctrl.SetLogger(klogr.New())
	// features.SetDefaultFeatureGates()

	if enablePprof {
		go func() {
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				setupLog.Error(err, "unable to start pprof")
			}
		}()
	}

	cfg := ctrl.GetConfigOrDie()
	setRestConfig(cfg)
	cfg.UserAgent = "koordinator-yarn-operator"

	setupLog.Info("new clientset registry")
	//err := extclient.NewRegistry(cfg)
	//if err != nil {
	//	setupLog.Error(err, "unable to init koordinator clientset and informer")
	//	os.Exit(1)
	//}

	var syncPeriod *time.Duration
	if syncPeriodStr != "" {
		d, err := time.ParseDuration(syncPeriodStr)
		if err != nil {
			setupLog.Error(err, "invalid sync period flag")
		} else {
			syncPeriod = &d
		}
	}

	// To reduce the memory size of the field index, we created a new cache that filters pods
	// see: https://github.com/kubernetes-sigs/controller-runtime/pull/1419/files
	var newCache cache.NewCacheFunc
	if cacheSelectorKey != "" && cacheSelectorValue != "" && cacheSelectorNamespace != "" {
		newCache = cache.BuilderWithOptions(cache.Options{
			SelectorsByObject: cache.SelectorsByObject{
				&corev1.Pod{}: {
					Label: labels.SelectorFromSet(labels.Set{cacheSelectorKey: cacheSelectorValue}),
				},
			},
			Namespace: cacheSelectorNamespace,
		})
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                     options.Scheme,
		MetricsBindAddress:         metricsAddr,
		HealthProbeBindAddress:     healthProbeAddr,
		LeaderElection:             enableLeaderElection,
		LeaderElectionID:           "koordinator-yarn-operator",
		LeaderElectionNamespace:    leaderElectionNamespace,
		LeaderElectionResourceLock: resourcelock.ConfigMapsLeasesResourceLock,
		Namespace:                  namespace,
		NewCache:                   newCache,
		SyncPeriod:                 syncPeriod,
		NewClient:                  utilclient.NewClient,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	setupLog.Info("register field index")
	if err := fieldindex.RegisterFieldIndexes(mgr.GetCache()); err != nil {
		setupLog.Error(err, "failed to register field index")
		os.Exit(1)
	}

	if err := opts.ApplyTo(mgr); err != nil {
		setupLog.Error(err, "unable to setup controllers")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	ctx := ctrl.SetupSignalHandler()

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func setRestConfig(c *rest.Config) {
	if *restConfigQPS > 0 {
		c.QPS = float32(*restConfigQPS)
	}
	if *restConfigBurst > 0 {
		c.Burst = *restConfigBurst
	}
}
