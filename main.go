package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	edKeyFileName  = "key.bin"
	PostDataPrefix = "postdata_"
)

var (
	ErrKeyFileExists = errors.New("key file already exists")
)

type TaskStatus int

const (
	TaskIdle TaskStatus = iota
	TaskInProgress
	TaskCompleted
)

var perDiskNodeCount = 1

const (
	NodeFileCount = 64
	PerFileSize   = 4 * 1024 * 1024 * 1024
)

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func main() {
	// 定义命令行参数
	flag.IntVar(&perDiskNodeCount, "node", 1, "Number of nodes per disk")
	disksStr := flag.String("disks", "", "Comma-separated list of disk paths")
	workerCount := flag.Int("workers", 1, "Number of workers")

	flag.Parse()
	if *disksStr == "" {
		log.Fatal().Msg("Error: disks parameter is required")
	}
	// 将逗号分隔的字符串转换为切片
	diskPaths := strings.Split(*disksStr, ",")
	log.Info().Msgf("Disk Paths: %v", diskPaths)
	log.Info().Msgf("Worker Count: %v", *workerCount)

	manager := NewTaskManager()
	manager.InitializeTasksWithWorkerCount(diskPaths, *workerCount)

	fmt.Println("Tasks:")
	for i, task := range manager.Tasks {
		fmt.Println(i, fmt.Sprintf("%+v", task))
	}

	http.HandleFunc("/task", func(w http.ResponseWriter, r *http.Request) {
		workIdStr := r.URL.Query().Get("workId")
		workId, err := strconv.Atoi(workIdStr)
		if err != nil {
			log.Err(err).Msgf("Invalid workId ip %s", r.RemoteAddr)
			http.Error(w, "Invalid workId", http.StatusBadRequest)
			return
		}

		folder, id, files, ok := manager.AssignTask(workId)
		if ok {
			task := Task{
				Folder: folder,
				ID:     id,
				Files:  [][2]int{files},
			}
			rsp, err := json.Marshal(task)
			if err != nil {
				http.Error(w, "json marshal error", http.StatusInternalServerError)
				return
			}
			log.Info().Msgf("task %s workId %s", string(rsp), workId)
			w.Header().Set("Content-Type", "application/json")
			_, err = w.Write(rsp)
			if err != nil {
				log.Error().Err(err).Msg("write response error")
				return
			}
			return
		}
		log.Warn().Msgf("No tasks available ip %s", r.RemoteAddr)
		http.Error(w, "No tasks available", http.StatusServiceUnavailable)
	})

	log.Fatal().Err(http.ListenAndServe(":2727", nil))
}

// Task 代表一个任务
type Task struct {
	Folder string   `json:"folder"`
	ID     string   `json:"id"`
	Files  [][2]int `json:"files"`
	Status TaskStatus
}

// TaskManager 管理任务分配
type TaskManager struct {
	Tasks         []*Task
	WorkerCurrent map[int]int // 工作机当前正在处理的任务索引
	Mutex         sync.Mutex
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		Tasks:         make([]*Task, 0),
		WorkerCurrent: make(map[int]int),
	}
}

func (m *TaskManager) InitializeTasksWithWorkerCount(diskPaths []string, workerCount int) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	for _, path := range diskPaths {
		for i := 0; i < perDiskNodeCount; i++ {
			folder := filepath.Join(path, fmt.Sprintf("post-%d", i))
			log.Info().Msgf("init folder %s", folder)
			// 检查文件夹是否存在
			if _, err := os.Stat(folder); os.IsNotExist(err) {
				log.Info().Msgf("folder is not exist %s, creating now", folder)
				// 文件夹不存在，直接生成key.bin,会自动创建文件夹
				id, err := genKey(folder)
				if err != nil {
					log.Warn().Err(err).Msgf("failed to create folder and key.bin %s", folder)
					continue // 如果创建失败，跳过这个文件夹
				}
				log.Info().Msgf("created folder %s and key.bin ID %s", folder, *id)
			} else {
				filename := filepath.Join(folder, edKeyFileName)
				if _, err := os.Stat(filename); os.IsNotExist(err) {
					id, err := genKey(folder)
					if err != nil {
						log.Warn().Err(err).Msgf("failed to gen key.bin %s", filename)
						continue // 如果创建失败，跳过这个文件夹
					}
					log.Info().Msgf("gen key.bin ID %s", filename, *id)
				}
			}

			// 判断文件夹是否已经P完了
			if isPostComplete(folder) {
				log.Info().Msgf("is post complete: %s", folder)
			} else {
				m.addTaskWithWorkerCount(folder, workerCount)
			}
		}
	}
}

func (m *TaskManager) addTaskWithWorkerCount(folder string, workerCount int) {
	fileRanges := make([][2]int, workerCount)
	filesPerWorker := 64 / workerCount
	remainder := 64 % workerCount

	start := 0
	for i := 0; i < workerCount; i++ {
		end := start + filesPerWorker - 1
		if i < remainder {
			end++
		}
		fileRanges[i] = [2]int{start, end}
		start = end + 1
	}

	id, err := readPubKey(folder)
	if err != nil {
		log.Warn().Msgf("error: read pubKey %s skip task", folder)
		return
	}

	m.Tasks = append(m.Tasks, &Task{
		Folder: folder,
		Files:  fileRanges,
		ID:     id,
		Status: TaskIdle,
	})
}

// AssignTask 分配子任务
func (m *TaskManager) AssignTask(workerID int) (string, string, [2]int, bool) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	// 获取工作机当前任务
	currentTaskIndex, ok := m.WorkerCurrent[workerID]
	if ok {
		// 移动到下一个任务
		currentTaskIndex++
	} else {
		// 如果工作机还没有任务分配一个任务
		currentTaskIndex = 0
	}

	if currentTaskIndex < len(m.Tasks) {
		task := m.Tasks[currentTaskIndex]
		// 更新工作机当前任务
		m.WorkerCurrent[workerID] = currentTaskIndex
		if workerID < len(task.Files) {
			return task.Folder, task.ID, task.Files[workerID], true
		}
	}

	return "", "", [2]int{}, false
}

// isPostComplete 判断1个节点是否已P完
func isPostComplete(folder string) bool {
	// 检查是否存在post.bin文件
	postBinPath := filepath.Join(folder, "post.bin")
	if _, err := os.Stat(postBinPath); err == nil {
		// post.bin文件存在
		return true
	}

	fileCount := 0
	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		base := filepath.Base(path)
		if strings.HasPrefix(base, PostDataPrefix) && info.Size() == PerFileSize {
			fileCount++
		}
		return nil
	})

	if err != nil {
		return false
	}

	return fileCount == NodeFileCount
}

// genKey 生成post key.bin和id
func genKey(dataDir string) (*string, error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("error: failed to generate identity: %w", err)
	}
	log.Info().Msgf("cli: generated id %x\n", pub)
	if err = saveKey(priv, dataDir); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(pub)
	return &id, nil
}

func saveKey(key ed25519.PrivateKey, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("error: mkdir error: %w", err)
	}

	filename := filepath.Join(dataDir, edKeyFileName)
	if _, err := os.Stat(filename); err == nil {
		return ErrKeyFileExists
	}

	if err := os.WriteFile(filename, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		return fmt.Errorf("key write to disk error: %w", err)
	}
	return nil
}

func readPubKey(folder string) (string, error) {
	// 读取 key.bin 文件
	keyPath := filepath.Join(folder, "key.bin")
	keyContent, err := os.ReadFile(keyPath)
	if err != nil {
		log.Printf("Error reading key.bin for folder %s: %v", folder, err)
		return "", err
	}

	// 截取第65到第128个字符作为公钥
	if len(keyContent) < 128 {
		log.Err(err).Msgf("key.bin content is too short in folder %s", folder)
		return "", fmt.Errorf("key.bin content is too short in folder %s", folder)
	}
	publicKey := string(keyContent[64:128]) // 在 Go 中，字符串索引是从0开始的
	return publicKey, nil
}
