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
	"fmt"
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

	// 选举时，候选人记录得票情况
	quorum Quorum

	// Leader记录提议的append得到了多少回应
	commitQuorum map[uint64]uint64
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
	for _, node := range c.peers {
		r.Prs[node] = &Progress{0, 1}
	}
	// if len(r.Prs) == 1 {
	// 	r.becomeCandidate()
	// 	r.becomeLeader()
	// }
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		log.Printf("node %v is not leader %v", r.id, r.Lead)
		return false
	}
	//log.Printf("[INFO] node %v append entry %v to %v", r.id, r.RaftLog.LastIndex()-r.RaftLog.entries[0].Index, to)
	offset := r.RaftLog.entries[0].Index

	//构造append信息
	if _, ok := r.Prs[to]; !ok {
		//log.Printf("[INFO] sendAppend to %v, but no progress info", to)
	}

	// r.PrintLog()
	// r.PrintProgress()

	logTerm, _ := r.RaftLog.Term(r.Prs[to].Match) // 将leader观察到的match发送给follower，被拒绝了再重新匹配
	r.Prs[to].Next = r.RaftLog.LastIndex() + 1
	ents := r.RaftLog.entries[r.Prs[to].Match-offset+1 : r.Prs[to].Next-offset] //将follower落后的log一次性补全
	appendRequest := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		LogTerm: logTerm, //match位置上的term
		Index:   r.Prs[to].Match + 1}
	for _, ent := range ents {
		appendRequest.Entries = append(appendRequest.Entries, &ent)
	}
	r.msgs = append(r.msgs, appendRequest)

	// 如果日志i是第一次propose，将同意提交i的节点数初始化为0
	for i := r.RaftLog.committed + 1; i < r.Prs[to].Next; i++ {
		if _, ok := r.commitQuorum[i]; !ok {
			r.commitQuorum[i] = 0
		}
	}

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	log.Printf("Leader %v send Heartbeat message to %v", r.id, to)
	// 心跳信息中告知follower最新的Index和对应的LogTerm，follower返回自己的Index信息，
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
	// //log.Printf("[INFO] tick: %v, timeout: %v, %v", r.heartbeatElapsed, r.heartbeatTimeout, len(r.Prs))

	// Leader向所有follower发送心跳包
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})
	}

	// Candidate和follower传递本地消息MessageType_MsgHup
	if r.electionElapsed >= r.electionTimeout {
		// //log.Printf("[INFO] %v election timeout at tick", r.id)
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id})
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
	// Your Code Here (2A).
	r.resetQuorum()
	r.State = StateCandidate

	// 发起下一轮选举
	r.Term++
	// 给自己投票
	myLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	r.Vote = r.id
	// 设置 [10, 20) 随机超时
	r.electionTimeout = r.randTimeout(10, 20)
	// //log.Printf("[INFO]set electionTimeout %v", r.electionTimeout)
	//log.Printf("[INFO] Node %v become candidate with term %v", r.id, r.Term)
	r.Step(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: r.id, Term: r.Term, Index: r.RaftLog.LastIndex(), LogTerm: myLogTerm, Reject: false})
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	//log.Printf("[INFO] Node %v became Leader with term %v", r.id, r.Term)
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// 通过空entry告知所有Peer，当前节点成为leader
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{EntryType: pb.EntryType_EntryNormal, Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: []byte("I become Leader")})
	r.Prs[r.id] = &Progress{Match: r.RaftLog.LastIndex(), Next: r.RaftLog.LastIndex() + 1}
	r.commitQuorum[r.RaftLog.LastIndex()] = 1

	for node := range r.Prs {
		// Follower的状态信息初始化为与Leader一样，后面根据返回的response调整。
		if node == r.id {
			continue
		}
		r.Prs[node] = &Progress{Match: r.RaftLog.LastIndex(), Next: r.RaftLog.LastIndex() + 1}
		//向所有follower发送空Entry()，在handleAppendResponse中更新progress
		r.sendAppend(node)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// TODO: 检查信息是否过期，过期reject，没过期交给对应的step处理
	// if m.Term > 0 && m.Term < r.Term {
	// 	//返回拒绝信息
	// 	log.Printf("Rejected staled %v from %v to %v", pb.MessageType_name[int32(m.MsgType)], m.From, m.To)
	// 	r.msgs = append(r.msgs, pb.Message{})
	// }

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	default:

	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for node := range r.Prs {
			if node == r.id {
				continue
			}
			r.sendHeartbeat(node)
			//log.Printf("[INFO] send MsgBeat of %v", node)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgPropose:
		// 将entry加入到log
		for _, ent_ptr := range m.Entries {
			var ent pb.Entry
			if ent_ptr == nil {
				ent = pb.Entry{EntryType: pb.EntryType_EntryNormal, Term: m.Term, Data: []byte{}, Index: r.RaftLog.LastIndex() + 1}
			} else {
				ent = *ent_ptr
				ent.Term = r.Term
				ent.Index = r.RaftLog.LastIndex() + 1
			}
			r.RaftLog.entries = append(r.RaftLog.entries, ent)
		}
		for peer := range r.Prs {
			r.sendAppend(peer)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	default:
		break
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for node := range r.Prs {
			if node == r.id {
				continue
			}
			// Message: 大家好，我是练习时长logTerm的偶像练习生Candidate，请大家多多为我投票
			logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      node,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: logTerm})
		}
	case pb.MessageType_MsgPropose:
		return errors.New("candidate can't issue a propose")
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	default:

	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for node := range r.Prs {
			if node == r.id {
				continue
			}
			// Message: 大家好，我是练习时长logTerm的偶像练习生Candidate，请大家多多为我投票
			logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      node,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: logTerm})
		}
	case pb.MessageType_MsgPropose:
		return errors.New("follower can't issue a propose")
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//更新自己的term
	if m.Term >= r.Term {
		//log.Printf("[INFO] node %v update term %v with leader %v term as %v", r.id, r.Term, m.From, m.Term)
		r.becomeFollower(m.Term, m.From)
	}
	// 对比Entries[match]的Term与Leader传过来的LogTerm是否一致
	match := m.Index - 1
	if match > r.RaftLog.LastIndex() {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Index:   r.RaftLog.LastIndex(), //Leader将match更新为Index
			Reject:  true,
		})
		return
	}
	// localLogTerm < 0时说明所有log都要被覆盖
	localLogTerm, _ := r.RaftLog.Term(match)
	if localLogTerm > 0 && m.GetLogTerm() != localLogTerm {
		log.Printf("Unmatched message from %v with term %v to %v", m.From, m.Term, r.id)
		// 这里做个原论文里提到的优化，直接找到下一个Term的Index
		preMatch := match - 2
		for preLogTerm, _ := r.RaftLog.Term(preMatch); preLogTerm == localLogTerm && preMatch >= r.RaftLog.entries[0].Index; preMatch-- {
			preLogTerm, _ = r.RaftLog.Term(preMatch)
		}
		//Index告诉Leader下一次append的match位置
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Index:   preMatch,
			Reject:  true,
		})
		return
	}

	// 接收append
	offset := r.RaftLog.entries[0].Index
	index := m.Index - offset
	r.RaftLog.entries = r.RaftLog.entries[0:index]
	for _, ent := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		//log.Printf("[INFO]node %v append entries %v with term %vs", r.id, ent.Index, ent.Term)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Index:   r.RaftLog.LastIndex(), //match=Index
		Reject:  false,
	})

	// r.PrintLog()
}

// 处理follower返回的AppendResponse，如果接受更新quorum信息，拒绝则重设match并重新append
func (r *Raft) handleAppendResponse(m pb.Message) {
	// offset := r.RaftLog.entries[0].Index
	//log.Printf("[INFO] %v handling append response from %v", m.To, m.From)
	if !m.GetReject() {
		// 更新返回数量统计结果，超过大多数则将committed向前移动
		for i := r.RaftLog.committed + 1; i <= m.Index; i++ {
			if i > r.Prs[m.From].Match && i < r.Prs[m.From].Next {
				r.commitQuorum[i]++
			}
			if count := r.commitQuorum[i]; count >= uint64((len(r.Prs)-1)/2+1) {
				r.RaftLog.committed = i
			}
		}
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		//log.Printf("[INFO] %v's proposal was accepted by  %v", m.To, m.From)
	} else {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = r.RaftLog.LastIndex() + 1

		//log.Printf("[INFO] %v's proposal was rejected by  %v with new progress {Match: %v, Next: %v}, ", m.To, m.From, r.Prs[m.From].Match, r.Prs[m.From].Next)

		if !r.sendAppend(m.From) {
			log.Panicf("failed to send append message to %v", m.From)
		}
	}
	// r.PrintLog()
	// r.PrintProgress()
}

// 处理返回的投票结果
func (r *Raft) handleVoteResponse(m pb.Message) {
	// 法定节点数
	if m.MsgType != pb.MessageType_MsgRequestVoteResponse {
		log.Printf("wrong message for handleVoteResponse")
	}
	if _, ok := r.votes[m.From]; ok {
		return
	} else {
		r.votes[m.From] = !m.Reject
	}
	//log.Printf("[INFO] Candidate %v handling vote response from %v, result is %v", r.id, m.From, !m.Reject)
	quorum := uint64(math.Floor(float64(len(r.Prs))/2)) + 1
	if m.Reject {
		r.quorum.Reject++
		if r.quorum.Reject >= quorum {
			// 退选
			r.becomeFollower(r.Term, None)
		}
	} else {
		r.quorum.Accept++
		if r.quorum.Accept >= quorum {
			// 胜选
			r.becomeLeader()
		}
	}
	//log.Printf("[INFO] Voting - reject: %v, accept: %v, quorum: %v, prs: %v", r.quorum.Reject, r.quorum.Accept, quorum, len(r.Prs))
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: logTerm})
}

// TODO: 如果response中出现不一致，发起append
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	//更新Progress信息
	r.Prs[m.From].Next = r.RaftLog.LastIndex() + 1
	r.Prs[m.From].Match = m.Index

	// 解决不一致: 1.Match与Leader不一致; 2. LogTerm与Leader的LogTerm不一致
	leaderLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if r.Prs[m.From].Match != r.Prs[m.From].Next-1 || m.LogTerm != leaderLogTerm {
		// follower的log数超过leader，将超过的部分切断
		if r.Prs[m.From].Match > r.Prs[m.From].Next {
			r.Prs[m.From].Match = r.RaftLog.LastIndex()
		} else if m.LogTerm != leaderLogTerm {
			// LogTerm不一致，向前寻找
			r.Prs[m.From].Match--
		}
		r.sendAppend(m.From)
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
	r.commitQuorum = map[uint64]uint64{}
}

func (r *Raft) randTimeout(l int, h int) int {
	if l >= h {
		log.Panicf("Invalid timeout interval [%v, %v)", l, h)
	}
	rand.Seed(time.Now().UnixNano())
	num := rand.Intn(h-l) + l
	return num
}

// 投票
func (r *Raft) handleVoteRequest(m pb.Message) {
	// 检查Candidate的Log是否比自己更新
	myLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	upToDate := m.Index >= r.RaftLog.LastIndex() && m.LogTerm >= myLogTerm
	if m.Term > r.Term && upToDate {
		//Term比自己大，而且有最新log，则退选
		//log.Printf("[INFO] %v changed vote from %v to %v", r.id, r.Vote, m.From)
		r.Vote = None
	}
	if r.Term > m.Term || r.Vote != None && r.Vote != m.From || !upToDate {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		//log.Printf("[INFO] Node %v rejected to vote for candidate %v, m.logTerm: %v, myLogTerm: %v, upToDate: %v", r.id, m.From, m.LogTerm, myLogTerm, upToDate)
		return
	}

	// Leader收到一个比他更新的voting请求，说明网络可能发生分区后恢复，这时应该让出Leader
	if m.From != r.id && (r.State == StateLeader || r.State == StateCandidate) {
		//log.Printf("[INFO] %s %v with term %v degrade to follower, new Term: %v, m.LogTerm: %v, myLogTerm: %v", r.State, r.id, r.Term, m.Term, m.LogTerm, myLogTerm)
	}

	if m.From != r.id {
		r.becomeFollower(m.Term, None)
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
	})
	r.Vote = m.From

	//log.Printf("[INFO] Node %v voted for candidate %v", r.id, r.Vote)
}

// DEBUG工具
// 打印Log
func (r *Raft) PrintLog() {
	output := fmt.Sprintf("node %v's log:", r.id)
	for _, ent := range r.RaftLog.entries {
		output += fmt.Sprintf("(%v, %v, %s)-", ent.Index, ent.Term, ent.Data)
	}
	log.Print("[INFO] " + output)
}

// 打印progress
func (r *Raft) PrintProgress() {
	output := fmt.Sprintf("Leader %v's perspective:\n", r.id)
	for id, pr := range r.Prs {
		output += fmt.Sprintf("%v: {%v, %v}\n", id, pr.Match, pr.Next)
	}
	log.Print("[INFO] " + output)
}
