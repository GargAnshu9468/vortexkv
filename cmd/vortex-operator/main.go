package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/GargAnshu9468/vortexkv/internal/operator"
)

const version = "1.0.0"

func main() {
	namespace := flag.String("namespace", "default", "Kubernetes namespace to watch")
	metricsPort := flag.Int("metrics-port", 8080, "Metrics and health port")
	showVer := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVer {
		fmt.Printf("VortexKV Kubernetes Operator v%s\n", version)
		os.Exit(0)
	}

	log.Printf("[VortexKV Operator] Starting controller manager v%s in namespace: %s", version, *namespace)
	log.Printf("[VortexKV Operator] Monitoring CRD 'vortexclusters.vortexkv.io/v1alpha1' on :%d", *metricsPort)

	_ = operator.NewController(*namespace)

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[VortexKV Operator] Shutting down gracefully...")
}
