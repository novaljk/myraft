package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestLogs_Rewrite(t *testing.T) {
	var logs = NewLogs()
	logs.Add(Log{0, 1})
	logs.Add(Log{1, 4})
	logs.Add(Log{2, 3})
	logs.Rewrite(4, []Log{{0, 1}, {1, 2}, {4, 8}})
	fmt.Println(logs.ToString())
}

func TestLogs_Add(t *testing.T) {
	var logs = NewLogs()
	for i := 0; i < 100; i++ {
		go add(logs)
	}
	time.Sleep(time.Second)
	fmt.Println(logs.Len())
}

func TestLogs_Rewrite2(t *testing.T) {
	var logs = NewLogs()
	logs.Add(Log{0, 1})
	logs.Rewrite(1, nil)
	fmt.Println(logs.ToString())
}

func add(logs *Logs) {
	logs.Add(Log{
		Term:    0,
		Command: 0,
	})
}
