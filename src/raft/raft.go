package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

const heartBeat = 50 * time.Millisecond

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//persistent state
	currentTerm int
	votedFor    int
	log         []Entry
	//volatile state
	commitIndex int
	lastApplied int
	//volatile state on leaders
	nextIndex    []int
	matchIndex   []int
	electionTime time.Time
	state        State
	applyCh      chan ApplyMsg
	votes        int
	channel      chan int
	// 2D
	lastIncludedIndex int
	lastIncludedTerm  int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

type HeartBeatArgs struct {
	LeaderCommit int
}

type HeartBeatReply struct {
	//Success bool
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludedTerm int
	LastIncludeIndex int
	Data             []byte
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	return term, isLeader
}

func (rf *Raft) GetPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	data := rf.GetPersistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voted int
	var logs []Entry
	var lastTerm int
	var lastIndex int
	if d.Decode(&term) != nil || d.Decode(&voted) != nil || d.Decode(&logs) != nil || d.Decode(&lastTerm) != nil || d.Decode(&lastIndex) != nil {
		fmt.Errorf("decode error")
	} else {
		rf.currentTerm = term
		rf.votedFor = voted
		rf.log = logs
		rf.lastIncludedTerm = lastTerm
		rf.lastIncludedIndex = lastIndex
	}
}

func (rf *Raft) startInstallSnapshot() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := InstallSnapshotArgs{
			Term:             rf.currentTerm,
			LeaderId:         rf.me,
			LastIncludedTerm: rf.lastIncludedTerm,
			LastIncludeIndex: rf.lastIncludedIndex,
			Data:             rf.persister.ReadSnapshot(),
			Done:             true,
		}
		go func(severID int, args *InstallSnapshotArgs) {
			reply := InstallSnapshotReply{}
			rf.SendInstallSnapshot(severID, args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.setNewTerm(reply.Term)
			}
		}(i, &args)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]: installing snapshot, args %v", rf.me, args)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		reply.Term = rf.currentTerm
	}
	rf.resetElectionTimer()
	if args.LastIncludeIndex < rf.lastIncludedIndex || (rf.lastIncludedIndex == args.LastIncludeIndex && rf.lastIncludedTerm == args.LastIncludedTerm) || args.LastIncludeIndex < rf.lastApplied {
		return
	}

	trune := false
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Index == args.LastIncludeIndex && rf.log[i].Term == args.LastIncludedTerm {
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.lastIncludedIndex = args.LastIncludeIndex
			rf.log = rf.log[i:]
			rf.lastApplied = args.LastIncludeIndex
			rf.commitIndex = args.LastIncludeIndex
			trune = true
			break
		}
	}

	if !trune {
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastIncludedIndex = args.LastIncludeIndex
		rf.lastApplied = args.LastIncludeIndex
		rf.commitIndex = args.LastIncludeIndex
		rf.log = []Entry{
			Entry{
				Term:  rf.lastIncludedTerm,
				Index: rf.lastIncludedIndex,
			},
		}
	}
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
	//rf.mu.Unlock()
	rf.applyCh <- msg
	//rf.mu.Lock()
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex < rf.commitIndex {
		return false
	}
	if lastIncludedIndex > rf.log[len(rf.log)-1].Index {
		rf.log = make([]Entry, 1)
	} else {
		base := rf.log[0].Index
		rf.log = rf.log[(lastIncludedIndex - base):]
	}
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Index = lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	data := rf.GetPersistData()
	rf.persister.SaveStateAndSnapshot(data, snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	base := rf.log[0].Index

	if index <= base || (base+len(rf.log)) <= index || index <= rf.lastIncludedIndex {
		DPrintf("[%v]: snapshot Error", rf.me)
		return
	}

	stateData := rf.GetPersistData()
	rf.persister.SaveStateAndSnapshot(stateData, snapshot)
	rf.lastIncludedIndex = index
	rf.commitIndex = index
	rf.lastApplied = index
	rf.log = append([]Entry{
		Entry{
			Term:  rf.log[index-base].Term,
			Index: index,
		},
	},
		rf.log[index-base+1:]...)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("[%v]: term %v not voted to %v", rf.me, rf.currentTerm, args.CandidateID)
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm < args.Term {
		rf.setNewTerm(args.Term)
	}
	upToDate := args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		reply.VoteGranted = true
		//rf.setNewTerm(args.Term)
		rf.votedFor = args.CandidateID
		rf.persist()
		DPrintf("[%v]: term %v voted to %v", rf.me, rf.currentTerm, rf.votedFor)
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.log[len(rf.log)-1].Index + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// Your code here (2B).
	if isLeader {
		var entry Entry

		entry = Entry{
			Term:    rf.currentTerm,
			Index:   index,
			Command: command,
		}

		rf.log = append(rf.log, entry)
		rf.persist()
		DPrintf("[%v]: term %v Start append entires %v", rf.me, rf.currentTerm, rf.log)
		rf.startAppendEntries(false)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {

	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.persist()
	votes := 0
	DPrintf("[%v]: start leader election, term %d\n", rf.me, rf.currentTerm)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for i, peer := range rf.peers {
		if i == args.CandidateID {
			continue
		}

		go func(i int, peer *labrpc.ClientEnd, votes *int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("[%d]: term %v send vote request to %d\n", args.CandidateID, args.Term, i)

			if reply.Term > rf.currentTerm {
				rf.setNewTerm(reply.Term)
			}
			if args.Term != rf.currentTerm {
				return
			}

			if ok && reply.VoteGranted {
				DPrintf("[%d]:Term %v, get voted from peer %v, candidate %v", rf.me, rf.currentTerm, i, args.CandidateID)
				*votes++
				if *votes >= len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == CANDIDATE {
					DPrintf("[%d]:Term %v selected as leader, candidate %v", rf.me, rf.currentTerm, rf.me)
					rf.state = LEADER
					rf.persist()
					for i, _ := range rf.peers {
						nextIndex := len(rf.log)

						rf.nextIndex[i] = rf.log[nextIndex-1].Index + 1
						rf.matchIndex[i] = 0
					}
					DPrintf("[%d]: leader - nextIndex %d", rf.me, len(rf.log)-1)
					go rf.startInstallSnapshot()
				}
			}
		}(i, peer, &votes, args)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d]: current term %d, get information from Leader %v, appendEntries %v, prevIndex %v, prevTerm %v, log length %v, LeaderCommit %v, LastApplied %v", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), args.LeaderCommit, rf.lastApplied)
	DPrintf("[%d]: current log %v, commitIndex %v, get Entry %v", rf.me, rf.log, rf.commitIndex, args.Entries)
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.XTerm = -1

	if args.Term < rf.currentTerm {
		DPrintf("[%d]: term is smaller than current term, from leader %v, get %v, mine %v,", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		reply.Term = rf.currentTerm
		return
	}

	rf.resetElectionTimer()
	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
		rf.persist()
	}
	base := rf.log[0].Index

	lastLogIndex := len(rf.log) - 1
	if rf.log[lastLogIndex].Index < args.PrevLogIndex {
		DPrintf("[%d]: Leader %v, last log index is not equal, args %v, myLog %v", rf.me, args.LeaderId, args, rf.log)
		reply.XLen = len(rf.log) + base
		reply.Conflict = true
		return
	}

	if args.PrevLogIndex-base < 0 && args.PrevLogTerm != rf.lastIncludedTerm {
		reply.XTerm = rf.lastIncludedTerm
		reply.Conflict = true
		reply.XIndex = rf.lastIncludedIndex + 1
		reply.XLen = len(rf.log) + base
		DPrintf("[%d]: log term is not equal , from leader %v, get %v, mine %v, current term %v", rf.me, args.LeaderId, args.PrevLogTerm, rf.log[lastLogIndex].Term, rf.currentTerm)
		return
	} else if args.PrevLogTerm != rf.log[args.PrevLogIndex-base].Term {
		reply.XTerm = rf.log[args.PrevLogIndex-base].Term
		reply.Conflict = true
		index := args.PrevLogIndex - 1
		for ; index >= 0 && index >= base; index-- {
			if rf.log[args.PrevLogIndex-base].Term != rf.log[index-base].Term {
				break
			}
		}
		reply.XIndex = index + 1
		reply.XLen = len(rf.log) + base
		DPrintf("[%d]: log term is not equal , from leader %v, get %v, mine %v, current term %v", rf.me, args.LeaderId, args.PrevLogTerm, rf.log[lastLogIndex].Term, rf.currentTerm)
		return
	}

	reply.Success = true
	for i, entry := range args.Entries {
		if entry.Index <= rf.log[len(rf.log)-1].Index && rf.log[entry.Index-base].Term != entry.Term {
			rf.log = rf.log[:(entry.Index - base)]
			if entry.Index-base == 0 {
				rf.log = append(rf.log, entry)
			}

			rf.persist()
		}
		if entry.Index > rf.log[len(rf.log)-1].Index {
			rf.log = append(rf.log, args.Entries[i:]...)
			reply.Term = rf.currentTerm
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		DPrintf("*****[%v]: term %v commit index renew, log %v *******", rf.me, rf.currentTerm, rf.log)
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > rf.log[len(rf.log)-1].Index {
			rf.commitIndex = rf.log[len(rf.log)-1].Index
		}
		rf.channel <- 1
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(id, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		DPrintf("||| [%v]: sendAppendEntries Fail1 to %v", rf.me, id)
		return
	}

	if rf.state != LEADER || rf.currentTerm > reply.Term || rf.currentTerm != args.Term {
		DPrintf("||| [%v]: sendAppendEntries Fail2 to %v, currentTerm %v, replyTerm %v, argsTerm %v", rf.me, id, rf.currentTerm, reply.Term, args.Term)
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("||| [%v]: sendAppendEntries Fail3 to %v", rf.me, id)

		rf.setNewTerm(reply.Term)
		return
	}

	if reply.Success {
		match := args.PrevLogIndex
		if len(args.Entries) > 0 {
			match = args.Entries[len(args.Entries)-1].Index
		}
		rf.nextIndex[id] = max(match+1, rf.nextIndex[id])
		rf.matchIndex[id] = max(match, rf.matchIndex[id])
		DPrintf("[%v]: %v append success next %v, match %v, rf.matchIndex %v, rf.nextIndex %v, log %v", rf.me, id, rf.nextIndex[id], rf.matchIndex[id], rf.matchIndex, rf.nextIndex, rf.log)
		base := rf.log[0].Index
		for n := rf.commitIndex + 1; n <= rf.log[len(rf.log)-1].Index; n++ {
			DPrintf("###[%v]: term %v, rf.matchIndex %v, counting for index %v  ####", rf.me, rf.currentTerm, rf.matchIndex, n)
			//
			if rf.log[n-base].Term != rf.currentTerm {
				DPrintf("[%v]:### Term not match, %v, %v ####", rf.me, rf.log[n-base].Term, rf.currentTerm)
				continue
			}
			counter := 1
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me && rf.matchIndex[j] >= n {
					counter++
				}

				if counter > len(rf.peers)/2 {
					rf.commitIndex = n
					rf.channel <- 1
					DPrintf("==== [%v]: leader start to commit index %v ====", rf.me, rf.commitIndex)
					break
				}
			}
		}
	} else if reply.Conflict {
		DPrintf("[%v]: appendEntry not success from peer %v, reply %v", rf.me, id, reply)
		base := rf.log[0].Index
		if reply.XTerm != -1 {
			if reply.XIndex < base || rf.log[reply.XIndex-base].Term != reply.XTerm {
				rf.nextIndex[id] = reply.XIndex
			} else {
				index2 := len(rf.log) - 1
				for ; index2 >= 0; index2-- {
					if rf.log[index2].Term == reply.XTerm {
						break
					}
				}
				rf.nextIndex[id] = index2 + 1 + base
			}
		} else {
			rf.nextIndex[id] = reply.XLen
		}
	} else if rf.nextIndex[id] > 1 {
		rf.nextIndex[id]--
	}
}

func (rf *Raft) startAppendEntries(heartBeat bool) {
	if heartBeat {
		DPrintf("[%v]: heartbeat", rf.me)
		//go rf.startInstallSnapshot()
	} else {
		DPrintf("[%v]:term %v leader %v start appendEntries", rf.me, rf.currentTerm, rf.me)
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(id int, isHeartBeat bool) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			next := rf.nextIndex[id]
			base := rf.log[0].Index
			if next <= 0 {
				next = 1
			}

			if isHeartBeat && rf.nextIndex[id] <= rf.lastIncludedIndex {
				args := InstallSnapshotArgs{
					Term:             rf.currentTerm,
					LeaderId:         rf.me,
					LastIncludedTerm: rf.lastIncludedTerm,
					LastIncludeIndex: rf.lastIncludedIndex,
					Data:             rf.persister.ReadSnapshot(),
					Done:             true,
				}
				rf.mu.Unlock()
				reply := InstallSnapshotReply{}
				rf.SendInstallSnapshot(id, &args, &reply)
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.setNewTerm(reply.Term)
				}
			} else {
				var args AppendEntriesArgs
				DPrintf("[%v]: appendEntry, base %v, next %v, log %v", rf.me, base, next, rf.log)
				prevIndex := rf.lastIncludedIndex
				prevTerm := rf.lastIncludedTerm
				if next-1-base >= 0 {
					prevIndex = rf.log[next-1-base].Index
					prevTerm = rf.log[next-1-base].Term
				}

				var newEntries []Entry
				newEntries = nil
				if next-base >= 0 {
					newEntries = rf.log[(next - base):]
				}

				args = AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      newEntries,
					LeaderCommit: rf.lastApplied,
				}

				reply := AppendEntriesReply{}
				DPrintf("||| [%v]: appendEntries %v to %v, next is %v, nextIndex %v |||", rf.me, args, id, next, rf.nextIndex)

				go rf.leaderAppendEntries(id, &args, &reply)
			}
		}(i, heartBeat)
	}
}

func (rf *Raft) HeartBeats(args *HeartBeatArgs, reply *HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()

	DPrintf("[%v]: heartbeat", rf.me)
}

func (rf *Raft) sendHeartBeats(server int, args *HeartBeatArgs, reply *HeartBeatReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeats", args, reply)
	return ok
}

func (rf *Raft) setNewTerm(term int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) startHeartBeats() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		args := HeartBeatArgs{}
		go func(i int, beatArgs HeartBeatArgs) {
			reply := HeartBeatReply{}
			rf.sendHeartBeats(i, &beatArgs, &reply)
		}(i, args)
	}
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if time.Now().After(rf.electionTime) {
			rf.startElection()
		}
		if rf.state == LEADER {
			rf.resetElectionTimer()
			rf.startAppendEntries(true)
			//rf.startHeartBeats()
		}
		DPrintf("*** [%v]: state OK, log %v ***", rf.me, rf.log)
		rf.mu.Unlock()

		time.Sleep(heartBeat)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		select {
		case <-rf.channel:
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied && rf.log[len(rf.log)-1].Index > rf.lastApplied {
				rf.ApplyMsg(rf.commitIndex, rf.lastApplied)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) ApplyMsg(index int, lastIndex int) {

	DPrintf("*****[%v]: COMMITING: %v to %v, current log %v******", rf.me, lastIndex, index, rf.log)
	base := rf.log[0].Index
	for i := lastIndex + 1; i <= index; i++ {
		DPrintf("*****[%v]: COMMIT msg %d: %v, lastapplied %v******", rf.me, i, rf.commits(i), rf.lastApplied)
		msg := ApplyMsg{
			Command:      rf.log[i-base].Command,
			CommandValid: true,
			CommandIndex: i,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.lastApplied = i
		base = rf.log[0].Index
	}
}

func (rf *Raft) commits(index int) string {
	nums := []string{}
	base := rf.log[0].Index
	for i := 0; i <= index-base; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.log[i].Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{-1, 0, 0})

	rf.me = me
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.dead = 0
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.channel = make(chan int, 100)

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 0
	}
	rf.resetElectionTimer()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
