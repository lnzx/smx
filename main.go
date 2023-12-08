package main

import (
	"flag"
	"fmt"
	. "github.com/lnzx/smx/handler"
	. "github.com/lnzx/smx/logger"
	. "github.com/lnzx/smx/types"
	"net/http"
	"os"
	"strings"
)

var (
	perDiskNodeCount = 1
	workerCount      = 1
)

var version = "0.0.1"

func init() {
	flag.Usage = func() {
		if _, err := fmt.Fprintf(os.Stderr, "Version: %s\nUsage: %s [args]\n", version, os.Args[0]); err != nil {
			Log.Fatal().Msg(err.Error())
		}
		flag.PrintDefaults()
	}
}

func main() {
	disksStr := flag.String("disks", "", "Comma-separated list of disk paths")
	flag.IntVar(&perDiskNodeCount, "node", 1, "Number of nodes per disk")
	flag.IntVar(&workerCount, "workers", 1, "Number of workers")
	flag.Parse()

	if *disksStr == "" {
		Log.Fatal().Msg("Error: disks parameter is required")
	}
	// 将逗号分隔的字符串转换为切片
	diskPaths := strings.Split(*disksStr, ",")
	Log.Info().Msgf("Disks: %v", diskPaths)
	Log.Info().Msgf("Workers: %v", workerCount)
	Log.Info().Msgf("PerNodeCount: %v\n", perDiskNodeCount)

	manager := NewTaskManager()
	manager.InitializeTasksWithWorkerCount(diskPaths, workerCount, perDiskNodeCount)

	fmt.Println("Tasks:")
	for i, task := range manager.Tasks {
		fmt.Println(i, fmt.Sprintf("%+v", task))
	}

	http.HandleFunc("/task", MakeHandler(manager, TaskHandler))
	http.HandleFunc("/complete", MakeHandler(manager, CompleteHandler))
	http.HandleFunc("/metrics", MakeHandler(manager, MetricsHandler))
	http.HandleFunc("/heartbeat", MakeHandler(manager, HeartbeatHandler))

	Log.Fatal().Err(http.ListenAndServe(":2727", nil))
}
