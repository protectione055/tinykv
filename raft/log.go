// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, _ := storage.LastIndex()
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Debug("newLog: the first log entry is not available")
	}

	// 有点怪，storage接口没有记录applied的值，测试传入的是MemoryStorage.
	hardState, _, _ := storage.InitialState()
	raftLog := new(RaftLog)
	raftLog.storage = storage
	raftLog.committed = hardState.Commit
	raftLog.applied, _ = storage.FirstIndex()
	raftLog.stabled = lastIndex
	raftLog.entries, _ = storage.Entries(firstIndex, lastIndex)

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	firstIndex, _ := l.storage.FirstIndex()
	firstUnstableIndex := l.stabled - firstIndex + 1
	res := l.entries[firstUnstableIndex:]
	return res
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firstIndex, _ := l.storage.FirstIndex()
	appliedIndex := l.applied - firstIndex
	ents = l.entries[appliedIndex+1:]
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	firstIndex, _ := l.storage.FirstIndex()
	lastIndex := firstIndex + uint64(len(l.entries))
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	firstIndex, _ := l.storage.FirstIndex()
	// i must be greater or equal than the first index
	if i < firstIndex {
		log.Debug("specified index is less than the first index of entries")
		return 0, errors.Errorf("specified index: %v, first index: %v", i, firstIndex)
	}

	term, err := l.storage.Term(i)
	return term, err
}
