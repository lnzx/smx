package types

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	. "github.com/lnzx/smx/logger"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	edKeyFileName  = "key.bin"
	PostDataPrefix = "postdata_"
	skipFileName   = "skip"
	NodeFileCount  = 64
	PerFileSize    = 4 * 1024 * 1024 * 1024
)

var ErrKeyFileExists = errors.New("key file already exists")

// Task 代表一个任务
type Task struct {
	Folder  string   `json:"folder"`
	ID      string   `json:"id"`
	Subsets [][2]int `json:"subsets"`
	Status  []int    `json:"status"`
}

// TaskManager 管理任务分配
type TaskManager struct {
	Tasks         []*Task
	WorkerCurrent map[int]int // 工作机当前正在处理的任务索引
	TimedSet      *TimedSet
	Mutex         sync.Mutex
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		Tasks:         make([]*Task, 0),
		WorkerCurrent: make(map[int]int),
		TimedSet:      NewTimedSet(120 * time.Second), // 创建一个新的NewTimedSet，超时时间为70s
	}
}

func (m *TaskManager) InitializeTasksWithWorkerCount(diskPaths []string, workerCount int, perDiskNodeCount int) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	for _, path := range diskPaths {
		for i := 1; i <= perDiskNodeCount; i++ {
			folder := filepath.Join(path, fmt.Sprintf("post-%d", i))
			// 检查文件夹是否存在
			if _, err := os.Stat(folder); os.IsNotExist(err) {
				Log.Info().Msgf("%s folder is not exist creating now", folder)
				// 文件夹不存在，直接生成key.bin,会自动创建文件夹
				_, err = genKey(folder)
				if err != nil {
					Log.Warn().Err(err).Msgf("failed to create folder and key.bin %s", folder)
					continue // 如果创建失败，跳过这个文件夹
				}
			} else {
				// 检查是否为要跳过的目录
				skipFile := filepath.Join(folder, skipFileName)
				if _, err = os.Stat(skipFile); err == nil {
					Log.Info().Msgf("%s folder skipped", folder)
					continue
				}

				filename := filepath.Join(folder, edKeyFileName)
				if _, err = os.Stat(filename); os.IsNotExist(err) {
					var id *string
					id, err = genKey(folder)
					if err != nil {
						Log.Warn().Err(err).Msgf("failed to gen key.bin %s", filename)
						continue // 如果创建失败，跳过这个文件夹
					}
					Log.Info().Msgf("%s folder gen key.bin ID %s", folder, *id)
				}

				// 判断文件夹是否已经P完了
				if isPostComplete(folder) {
					Log.Info().Msgf("%s folder is post complete", folder)
					continue
				}

				Log.Info().Msgf("%s folder initialization", folder)
			}

			m.addTaskWithWorkerCount(folder, workerCount)
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
		Log.Warn().Msgf("error: read pubKey %s skip task", folder)
		return
	}

	m.Tasks = append(m.Tasks, &Task{
		Folder:  folder,
		Subsets: fileRanges,
		ID:      id,
		Status:  make([]int, workerCount),
	})
}

// AssignTask 分配子任务
func (m *TaskManager) AssignTask(workerID int) (string, string, [2]int, bool) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	index := workerID - 1
	// 获取工作机当前任务
	currentTaskIndex, ok := m.WorkerCurrent[workerID]
	if ok {
		task := m.Tasks[currentTaskIndex]
		// 检查当前工作机处理的文件范围是否已完成
		if index < len(task.Subsets) && task.Status[index] != -workerID {
			return task.Folder, task.ID, task.Subsets[index], true
		}
		// 如果当前任务已完成，移动到下一个任务,+1后有可能比所有任务的索引都大，在下面会判断
		currentTaskIndex++
	} else {
		// 如果工作机还没有任务分配一个任务
		currentTaskIndex = 0
	}

	if currentTaskIndex < len(m.Tasks) {
		task := m.Tasks[currentTaskIndex]
		// 更新工作机当前任务
		m.WorkerCurrent[workerID] = currentTaskIndex
		// 检查当前工作机处理的文件范围是否已完成
		if index < len(task.Subsets) && task.Status[index] != -workerID {
			task.Status[index] = workerID //更新当前任务
			return task.Folder, task.ID, task.Subsets[index], true
		}
	}

	return "", "", [2]int{}, false
}

// TaskComplete 表示当前任务为完成状态
func (m *TaskManager) TaskComplete(workerID int) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	index := workerID - 1
	// 获取工作机当前任务
	currentTaskIndex, ok := m.WorkerCurrent[workerID]
	if ok {
		task := m.Tasks[currentTaskIndex]
		if index < len(task.Subsets) {
			task.Status[index] = -workerID
			return nil
		}
	}
	return errors.New("no task found")
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
		return true
	}

	return fileCount == NodeFileCount
}

// genKey 生成post key.bin和id
func genKey(dataDir string) (*string, error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("error: failed to generate identity: %w", err)
	}
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
		Log.Printf("Error reading key.bin for folder %s: %v", folder, err)
		return "", err
	}

	// 截取第65到第128个字符作为公钥
	if len(keyContent) < 128 {
		Log.Err(err).Msgf("key.bin content is too short in folder %s", folder)
		return "", fmt.Errorf("key.bin content is too short in folder %s", folder)
	}
	publicKey := string(keyContent[64:128]) // 在 Go 中，字符串索引是从0开始的
	return publicKey, nil
}
