package handler

import (
	"encoding/json"
	"fmt"
	. "github.com/lnzx/smx/logger"
	. "github.com/lnzx/smx/types"
	"net/http"
	"strconv"
)

func MakeHandler(manager *TaskManager, handlerFunc func(http.ResponseWriter, *http.Request, *TaskManager)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handlerFunc(w, r, manager)
	}
}

func TaskHandler(w http.ResponseWriter, r *http.Request, manager *TaskManager) {
	workerID, err := getWorkerId(r) // workerId 从1开始
	if err != nil {
		Log.Err(err).Msgf("Invalid workId %d ip %s", workerID, r.RemoteAddr)
		http.Error(w, "Invalid workId", http.StatusBadRequest)
		return
	}

	if folder, id, files, ok := manager.AssignTask(workerID); ok {
		task := Task{
			Folder:  folder,
			ID:      id,
			Subsets: [][2]int{files},
		}
		rsp, err := json.Marshal(task)
		if err != nil {
			Log.Error().Err(err).Msg("task json marshal error")
			http.Error(w, "json marshal error", http.StatusInternalServerError)
			return
		}
		Log.Info().Msgf("task %s workId %d", string(rsp), workerID)
		w.Header().Set("Content-Type", "application/json")
		if _, err = w.Write(rsp); err != nil {
			Log.Error().Err(err).Msg("task write response error")
		}
		return
	}

	Log.Warn().Msgf("No tasks available workId %d ip %s", workerID, r.RemoteAddr)
	http.Error(w, "No tasks available", http.StatusServiceUnavailable)
}

func CompleteHandler(w http.ResponseWriter, r *http.Request, manager *TaskManager) {
	workerID, err := getWorkerId(r) // workerId 从1开始
	if err != nil {
		http.Error(w, "Invalid workerId", http.StatusBadRequest)
		return
	}
	// 在这里处理任务完成的逻辑，例如更新任务状态等
	if err = manager.TaskComplete(workerID); err == nil {
		// 返回确认消息
		if _, err = w.Write([]byte("OK")); err != nil {
			Log.Error().Err(err).Msgf("TaskComplete write response error workerID %d", workerID)
		}
	}
}

func MetricsHandler(w http.ResponseWriter, _ *http.Request, manager *TaskManager) {
	data := map[string]interface{}{
		"tasks":    manager.Tasks,
		"workings": manager.TimedSet.GetData(),
	}

	result, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "metrics json marshal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(result); err != nil {
		Log.Error().Err(err).Msg("write metrics response error")
	}
}

func HeartbeatHandler(w http.ResponseWriter, r *http.Request, manager *TaskManager) {
	workerID, err := getWorkerId(r) // workerId 从1开始
	if err != nil {
		http.Error(w, "Invalid workerId", http.StatusBadRequest)
		return
	}
	manager.TimedSet.Add(workerID)
	// 返回确认消息
	if _, err = w.Write([]byte("OK")); err != nil {
		Log.Error().Err(err).Msg("write heartbeat response error")
	}
}

func getWorkerId(r *http.Request) (int, error) {
	workerStr := r.URL.Query().Get("workerId")
	workerId, err := strconv.Atoi(workerStr)
	if workerId < 1 || err != nil {
		Log.Error().Msgf("invalid workerId %s", workerStr)
		return 0, fmt.Errorf("invalid workerId %s", workerStr)
	}
	return workerId, err
}
