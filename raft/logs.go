package raft

type Log struct {
	Term    int //接受到该状态时的任期
	Command interface{}
}

type Logs struct {
	Data []Log
}

func (logs *Logs) GetLastLogIndex() int {
	return len(logs.Data) - 1
}

func (logs *Logs) GetLastLogTerm() int {
	var length = len(logs.Data)
	if length <= 0 {
		return -1
	}
	return logs.Data[length-1].Term
}

func (logs *Logs) Add(log Log) {
	logs.Data = append(logs.Data, log)
}
