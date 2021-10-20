package raft

import (
	"fmt"
	"log"
	"sync"
)

type Log struct {
	Term    int //接受到该状态时的任期
	Command interface{}
}

type Logs struct {
	data []Log
	mu   sync.Mutex
}

func NewLogs() (logs *Logs) {
	logs = &Logs{
		data: make([]Log, 1, 10),
	}
	logs.data[0] = Log{
		Term:    0, //所有raft预存一条启动命令
		Command: "init",
	}
	return
}

func (logs *Logs) GetRangeLog(left, right int) []Log {
	logs.mu.Lock()
	defer logs.mu.Unlock()

	var newData = make([]Log, right-left+1)
	copy(newData, logs.data[left:right+1])
	return newData
}

func (logs *Logs) GetLog(index int) Log {
	logs.mu.Lock()
	defer logs.mu.Unlock()

	if index < 0 || index >= len(logs.data) {
		log.Panicln("Logs访问越界错误!")
	}
	return logs.data[index]
}

func (logs *Logs) GetLastLog() Log {
	logs.mu.Lock()
	defer logs.mu.Unlock()

	if len(logs.data) <= 0 {
		log.Panicln("Logs访问越界错误!")
	}
	return logs.data[len(logs.data)-1]
}

func (logs *Logs) Len() int {
	logs.mu.Lock()
	defer logs.mu.Unlock()

	return len(logs.data)
}

func (logs *Logs) Add(log Log) {
	logs.mu.Lock()
	defer logs.mu.Unlock()

	logs.data = append(logs.data, log)
}

// Rewrite 从index位置开始将所有日志覆盖为entries
func (logs *Logs) Rewrite(index int, entries []Log) {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	if index < 0 || index > len(logs.data) {
		log.Panicln("Logs访问越界错误!")
	}
	logs.data = append(logs.data[:index], entries...)
}

func (logs *Logs) ToString() string {
	logs.mu.Lock()
	defer logs.mu.Unlock()

	return fmt.Sprintf("%v", logs.data)
}
