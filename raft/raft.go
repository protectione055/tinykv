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
	"errors"
	"log"
	"math"

	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {

		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

// 记录当前选举的票情况，Accept大于节点数一半成为leader，Reject大于一半退选
type Quorum struct {
	Accept, Reject uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// 记录得票情况
	quorum Quorum
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	r := &Raft{
		State:            StateFollower,
		id:               c.ID,
		RaftLog:          raftLog,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Term:             0,
		Prs:              map[uint64]*Progress{},
		votes:            map[uint64]bool{},
		quorum:           Quorum{0, 0}}
	for _, p := range c.peers {
		r.Prs[uint64(p)] = &Progress{
			Match: 0,
			Next:  0}
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		log.Panicf("node %v is not leader %v", r.id, r.Lead)
	}
	// TODO: 向follower发送append信号，如果被拒绝，则找到正确的match
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		LogTerm: logTerm,
		Index:   r.Prs[to].Next,
		Entries: []*pb.Entry{&r.RaftLog.entries[r.RaftLog.LastIndex()-r.RaftLog.entries[0].Index]},
	})
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	log.Printf("Leader %v send Heartbeat message to %v", r.id, to)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed += 1
	} else {
		r.electionElapsed += 1
	}

	// Leader向所有follower发送心跳包
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		for node := range r.Prs {
			if node == r.id {
				continue
			}
			logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, To: node, Term: 0, LogTerm: logTerm})
		}
	}

	// Candidate和follower传递本地消息MessageType_MsgHup
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, Term: 0})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.resetQuorum()
	r.State = StateFollower
	r.Term = term
	r.Lead = lead

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	log.Printf("Node %v become candidate", r.id)
	// Your Code Here (2A).
	r.resetQuorum()
	r.State = StateCandidate

	// 给自己投票
	r.Vote = r.id
	r.votes[r.id] = true

	// 设置 [10, 20) 随机超时
	r.electionTimeout = r.randTimeout(10, 20)

	// 发起下一轮选举
	r.Term++
	for node := range r.Prs {
		if node == r.id {
			continue
		}
		// Message: 大家好，我是练习时长r.Term的偶像练习生___，请大家多多支持
		logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      node,
			Term:    r.Term,
			LogTerm: logTerm})
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Printf("Node %v became Leader", r.id)
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// 告知所有Peer，当前节点成为leader
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{})
	for node := range r.Prs {
		// Follower的状态信息初始化为与Leader一样，后面根据返回的response调整。
		r.Prs[node] = &Progress{Match: r.RaftLog.LastIndex(), Next: r.RaftLog.LastIndex() + 1}
		if node == r.id {
			continue
		}
		//向所有follower发送空Entry()，在handleAppendResponse中更新progress
		for node := range r.Prs {
			r.sendAppend(node)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// TODO: 检查信息是否过期，过期reject，没过期交给对应的step处理
	if m.Term > 0 && m.Term < r.Term {
		//返回拒绝信息
		log.Printf("Rejected staled %v from %v to %v", pb.MessageType_name[int32(m.MsgType)], m.From, m.To)
		r.msgs = append(r.msgs, pb.Message{})
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.sendHeartbeat(m.To)
	case pb.MessageType_MsgPropose:
		//TODO
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
	case pb.MessageType_MsgRequestVote:
		r.voting(false, m.To)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 拒绝过时信息
	if m.Term < r.Term {
		log.Printf("Stale message from %v with term %v, current term %v", m.From, m.Term, r.Term)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			LogTerm: m.LogTerm,
			Index:   m.Index,
			Commit:  r.RaftLog.committed,
			Reject:  true,
		})
	}

	//检查本地commit的位置，看能不能append

	r.Term = m.Term

}

// 处理返回的投票结果
func (r *Raft) handleVoteResponse(m pb.Message) {
	// 法定节点数
	quorum := uint64(math.Ceil(float64(len(r.Prs)) / 2))

	if m.Reject {
		r.quorum.Reject++
		if r.quorum.Reject >= quorum {
			// 退选
			r.becomeFollower(r.Term, None)
		} else {
			r.quorum.Accept++
			if r.quorum.Accept >= quorum {
				// 胜选
				r.becomeLeader()
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		if r.State == StateCandidate {
			r.becomeFollower(m.Term, m.From)
		}
	case pb.MessageType_MsgHeartbeatResponse:
		// Leader更新follower的元信息
		return
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// util functions
// 重置节点选举状态信息
func (r *Raft) resetQuorum() {
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Vote = None
	r.votes = map[uint64]bool{}
	r.quorum = Quorum{0, 0}
}

func (r *Raft) randTimeout(l int, h int) int {
	if l <= h {
		log.Panicf("Invalid timeout interval [%v, %v)", l, h)
	}
	rand.Seed(time.Now().Unix())
	return rand.Intn(h-l) + l
}

// 投票
func (r *Raft) voting(vote bool, to uint64) {
	r.votes[to] = vote
	r.Vote = to
	if to != r.id {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      to,
			Reject:  !vote,
		})
	}
}
