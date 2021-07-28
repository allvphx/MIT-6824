package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "C"
import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type LogEntry struct {
	Term  int
	Index int
	Cmd   interface{}
}

type Role int

const (
	Leader            Role = 1
	Candidate         Role = 2
	Follower          Role = 3
	HeartBeatInterval      = 100 * time.Millisecond
	ApplyBeatInterval      = 10 * time.Millisecond
	VoteTimeOut            = 3 * HeartBeatInterval
	LogLength              = 0
)

func role2String(num Role) string {
	switch num {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return "Error"
	}
}

func Min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

func getRandElectTimeout() time.Duration {
	return time.Duration(5+rand.Intn(3)) * HeartBeatInterval
}

func (a *LogEntry) CheckSame(b *LogEntry) bool {
	return a.Term == b.Term && a.Index == b.Index
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// common states used for leader election
	currentTerm int
	role        Role
	leaderID    int
	peerNum     int
	// voteFor : -1 for null
	voteFor       int
	electionTimer *time.Timer
	applyCh       chan ApplyMsg

	// common states used for log appending
	log               []*LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	commitIndex int
	lastApplied int
	// states for leader to replicate the logs
	nextIndex  []int
	matchIndex []int
	pingTimer  *time.Timer
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[%s:%d;Term:%d;VotedFor:%d;LD:%d;logLen:%v;Commit:%v;Apply:%v]",
		role2String(rf.role), rf.me, rf.currentTerm, rf.voteFor, rf.leaderID, len(rf.log), rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) getLog(index int) *LogEntry {
	index -= rf.lastIncludedIndex
	return rf.log[index-1]
}

func (rf *Raft) getLogLen() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) indexDecSnapshot(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) indexADDSnapshot(index int) int {
	return index + rf.lastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	/// we assume that there are more than 0 reps
	isleader = rf.role == Leader
	return term, isleader
}

func (rf *Raft) apply(entry *LogEntry) {
	//DPrintf("%v [APP] the %d applied", rf, entry.Index)
	rf.applyCh <- ApplyMsg{
		CommandValid: true,
		CommandIndex: entry.Index,
		Command:      entry.Cmd,
	}
}

func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.raftStateForPersist()
	rf.persister.SaveRaftState(data)
	DPrintf("%v [APK] Persist saved with term:%d;voteFor%d;len(log):%d", rf, rf.currentTerm, rf.voteFor, len(rf.log))
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("%v [APK] Bootstrap without any state", rf)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	rf.voteFor = -1
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.voteFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.lastIncludedIndex) != nil || d.Decode(&rf.lastIncludedTerm) != nil {
		DPrintf("%v [APK] Decoding not found", rf)
	} else {
		DPrintf("%v [APK] Data loaded with term:%d;voteFor%d;len(log):%d;lastIncluded:%d", rf, rf.currentTerm, rf.voteFor,
			len(rf.log), rf.lastIncludedIndex)
	}
	rf.mu.Unlock()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	//	if
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/*
		if rf.lastIncludedIndex > lastIncludedIndex || rf.lastIncludedTerm > lastIncludedTerm {
			return false
		}

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex

		rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)
	*/
	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/*
		entry := rf.log[index-1]
		DPrintf("[%v] [SNAP] Snapshot with index index[%v]  rf.Log[%v]", rf, index, rf.log)

		rf.log = rf.log[index:]
		rf.lastIncludedIndex = entry.Index
		rf.lastIncludedTerm = entry.Term
		rf.lastApplied = entry.Index

		rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)
		rf.persist()
	*/
}

// (Thread unsafe) If the received Term is bigger, transform into the Follower
func (rf *Raft) checkTerm(term int) {
	if term > rf.currentTerm {
		DPrintf("%s: [TermChange] The term is updated into %d", rf, term)
		rf.TranState(Follower, term)
	}
}

// (Thread unsafe) Check if (Index, Term) is at least up to date with rf.
func (rf *Raft) isUpToDate(index int, term int) bool {
	if rf.getLogLen() == 0 {
		return true
	}
	tailLog := *rf.getLog(rf.getLogLen())
	if term > tailLog.Term {
		return true
	}
	return term == tailLog.Term && index >= rf.getLogLen()
}

// (Thread unsafe) To transform the state into next, with new Term
func (rf *Raft) TranState(next Role, term int) bool {
	rf.currentTerm = term
	switch next {
	case Leader:
		DPrintf("%s [StateChange] Begin changing to leader", rf)
		if rf.role != Candidate {
			return false
		}
		rf.role = Leader
		rf.leaderID = rf.me
		rf.persist()

		for i := 0; i < rf.peerNum; i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = rf.getLogLen() + 1
		}
		rf.pingTimer.Reset(HeartBeatInterval)
		go rf.pingLoop()
		DPrintf("%s [StateChange] Changed to leader", rf)

	case Candidate:
		if rf.role != Follower {
			return false
		}
		//DPrintf("%s [StateChange] Begin changing to candidate", rf)
		rf.role = Candidate
		rf.voteFor = rf.me
		rf.persist()

		rf.leaderID = -1
		DPrintf("%s [StateChange] Changed to candidate", rf)
	case Follower:
		//DPrintf("%s [StateChange] Begin changing to follower with Term %d", rf, term)
		rf.role = Follower
		rf.voteFor = -1
		rf.persist()

		rf.leaderID = -1
		rand.Seed(time.Now().UnixNano())
		rf.electionTimer.Reset(getRandElectTimeout())
		DPrintf("%s [StateChange] Changed to follower with Term %d", rf, term)

	default:
		return false
	}
	return true
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastlogIndex int
	LastlogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rv *RequestVoteReply) String() string {
	return fmt.Sprintf("[Term:%d;VotedGranted:%v]", rv.Term, rv.VoteGranted)
}

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry // empty for heartbeat
	LeaderCommit int
}

type AppendEntryReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rv *AppendEntryReply) String() string {
	return fmt.Sprintf("[Term:%d;Success:%v]", rv.Term, rv.Success)
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rv *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("[Term:%d;LeaderId:%v;LastInclude:%v-%v;]", rv.Term, rv.LeaderId, rv.LastIncludeTerm, rv.LastIncludeIndex)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkTerm(args.Term)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if rf.isUpToDate(args.LastlogIndex, args.LastlogTerm) && (rf.voteFor == -1 || rf.voteFor == args.CandidateID) {
		reply.VoteGranted = true
		rf.electionTimer.Reset(getRandElectTimeout())
		rf.voteFor = args.CandidateID
		rf.persist()
	}
}

// snapshot supported
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkTerm(args.Term)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if rf.role == Candidate {
		rf.TranState(Follower, args.Term)
	}

	rf.electionTimer.Reset(getRandElectTimeout())
	rf.leaderID = args.LeaderID

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		if args.PrevLogTerm != rf.lastIncludedTerm {
			reply.ConflictIndex = 1
			return
		}
	} else {
		if rf.getLogLen() < args.PrevLogIndex {
			reply.ConflictIndex = rf.getLogLen() + 1
			return
		}
		if args.PrevLogIndex > 0 && rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.getLog(args.PrevLogIndex).Term
			for id := rf.lastIncludedIndex + 1; id <= args.PrevLogIndex; id++ {
				if rf.getLog(id).Term == reply.ConflictTerm {
					reply.ConflictIndex = id
					break
				}
			}
			return
		}
	}

	if len(args.Entries) > 0 {
		DPrintf("%s [APP] %d Entries applied from %d with index %d", rf, len(args.Entries), rf.leaderID, args.PrevLogIndex)
	}

	rf.log = rf.log[:rf.indexDecSnapshot(args.PrevLogIndex)]
	rf.log = append(rf.log, args.Entries...)

	if Debug && len(args.Entries) > 0 {
		fmt.Printf("%v the log -> ", rf)
		for _, x := range rf.log {
			fmt.Printf("[%d,%d: %v]", x.Term, x.Index, x.Cmd)
		}
		fmt.Printf("\n")
	}

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLogLen())
		DPrintf("%s [APP] CommitIndex updated", rf)
	}
	reply.ConflictIndex = 0
	rf.persist()
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < reply.Term {
		rf.mu.Unlock()
		return
	}

	if rf.lastIncludedIndex >= args.LastIncludeIndex || rf.lastIncludedTerm >= args.LastIncludeTerm {
		DPrintf("%v [SNAP] InstallSnapshot from %d without rebuild", rf, args.LastIncludeIndex)
		return
	}

	if args.LastIncludeIndex >= rf.getLogLen() || rf.getLog(args.LastIncludeIndex).Term != args.LastIncludeTerm {
		rf.log = rf.log[:0]
	} else {
		rf.log = rf.log[rf.indexDecSnapshot(args.LastIncludeIndex):]
	}

	DPrintf("%v [SNAP] InstallSnapshot from %d with rebuild", rf, args.LastIncludeIndex)

	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.Snapshot,
		SnapshotTerm: args.LastIncludeTerm, SnapshotIndex: args.LastIncludeIndex}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, &reply)
	if ok {
		rf.mu.Lock()
		rf.checkTerm(reply.Term)
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	//	DPrintf("%s: [RPC] %d asks vote from %d", rf, rf.me, server)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		rf.mu.Lock()
		rf.checkTerm(reply.Term)
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	if reply.VoteGranted {
		//		DPrintf("%s: [RPC] %d get vote from %d", rf, rf.me, server)
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if ok {
		rf.mu.Lock()
		rf.checkTerm(reply.Term)
		rf.mu.Unlock()
	}
	return ok
}

// Send the vote request messages concurrently
func (rf *Raft) broadcastVoteRequest() int {
	voteCount := 1
	remainPost := rf.peerNum - 1

	for i := 0; i < rf.peerNum; i++ {
		if i != rf.me {
			go func(server int) {
				reps := RequestVoteReply{}
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastlogIndex: rf.getLogLen(),
				}
				if rf.getLogLen() > 0 {
					args.LastlogTerm = rf.getLog(rf.getLogLen()).Term
				}

				if remainPost == 0 {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				ok := rf.sendRequestVote(server, &args, &reps)
				rf.mu.Lock()

				if ok && reps.VoteGranted && rf.role == Candidate && rf.currentTerm == args.Term {
					voteCount++
				}
				remainPost = Max(0, remainPost-1)
				if voteCount*2 > rf.peerNum || (voteCount+remainPost)*2 < rf.peerNum || rf.currentTerm != args.Term {
					remainPost = 0
				}
				rf.mu.Unlock()
			}(i)
		}
	}

	for i := time.Duration(0); i < VoteTimeOut; i += HeartBeatInterval {
		time.Sleep(HeartBeatInterval)
		rf.mu.Lock()
		if remainPost == 0 {
			rf.mu.Unlock()
			return voteCount
		}
		rf.mu.Unlock()
	}
	// Time out , get no vote
	return 0
}

// Send the append entry messages concurrently
func (rf *Raft) broadcastAppendEntry() {
	voteCount := 1
	Cnd := sync.NewCond(&rf.mu)
	remainPost := rf.peerNum - 1
	rf.mu.Lock()
	Term := rf.currentTerm
	rf.mu.Unlock()

	for i := 0; i < rf.peerNum; i++ {
		if i != rf.me {
			go rf.tryAppendEntries(i, Cnd, &remainPost, &voteCount, Term)
		}
	}

	for {
		rf.mu.Lock()
		Cnd.Wait()
		if remainPost == 0 {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	if 2*voteCount > rf.peerNum && rf.role == Leader && Term == rf.currentTerm {
		rf.updateCommitIndex()
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateCommitIndex() {
	for N := rf.getLogLen(); N > rf.commitIndex; N-- {
		matchCount := 1
		for i := 0; i < rf.peerNum; i++ {
			if i != rf.me && N <= rf.matchIndex[i] {
				matchCount++
			}
		}

		if matchCount*2 > rf.peerNum {
			rf.commitIndex = N
			DPrintf("%s [APP] Inc commitIndex to %d with match count %d", rf, N, matchCount)
			break
		}
	}
}

func (rf *Raft) tryAppendEntries(server int, Cnd *sync.Cond, remainPost *int, voteCount *int, Term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reps := AppendEntryReply{}
	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	for {
		if *remainPost == 0 {
			return
		}

		if rf.nextIndex[server] <= rf.lastIncludedIndex {
			args.Entries = rf.log[:0]
		} else {
			args.Entries = rf.log[rf.indexDecSnapshot(rf.nextIndex[server])-1:]
			if rf.indexDecSnapshot(rf.nextIndex[server]) > 1 {
				args.PrevLogTerm = rf.getLog(rf.nextIndex[server] - 1).Term
				args.PrevLogIndex = rf.getLog(rf.nextIndex[server] - 1).Index
			} else {
				args.PrevLogTerm = 0
				args.PrevLogIndex = 0
			}
		}

		if Term != rf.currentTerm {
			*remainPost = 0
			DPrintf("%s [APP] invalid sent to %d for index %d - %d, for overdated leader", rf, server, args.PrevLogIndex, args.PrevLogTerm)
			Cnd.Broadcast()
			return
		}

		rf.mu.Unlock()
		ok := rf.sendAppendEntry(server, &args, &reps)
		rf.mu.Lock()

		if Term != rf.currentTerm {
			*remainPost = 0
			DPrintf("%s [APP] invalid sent to %d for index %d - %d, for overdated leader", rf, server, args.PrevLogIndex, args.PrevLogTerm)
			Cnd.Broadcast()
			return
		}

		if (!reps.Success && reps.ConflictIndex == -1) || !ok {
			// the later two will not happen
			*remainPost = Max(*remainPost-1, 0)
			DPrintf("%s [APP] invalid sent to %d for index %d - %d for network failure", rf, server, args.PrevLogIndex, args.PrevLogTerm)
			Cnd.Broadcast()
			return
		}

		if reps.Success {
			*voteCount++
			*remainPost = Max(*remainPost-1, 0)
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			if *voteCount*2 > rf.peerNum || (*voteCount+*remainPost)*2 < rf.peerNum {
				*remainPost = 0
			}

			Cnd.Broadcast()
			return
		}

		rf.nextIndex[server] = reps.ConflictIndex

		if reps.ConflictTerm != -1 {
			conflictId := -1
			for id := args.PrevLogIndex; id > rf.lastIncludedIndex; id-- {
				if rf.getLog(id).Term == reps.ConflictTerm {
					conflictId = id
					break
				}
			}
			if conflictId != -1 {
				rf.nextIndex[server] = conflictId
			}
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.role == Leader

	if !isLeader {
		return index, term, isLeader
	}

	// Your code here (2B).
	term = rf.currentTerm
	index = rf.getLogLen() + 1 // it begins from where?
	DPrintf("%s [APP] client request received, with index %d", rf, index)
	newLog := &LogEntry{Term: term, Index: index, Cmd: command}
	rf.log = append(rf.log, newLog)
	rf.persist()
	go rf.broadcastAppendEntry()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.electionTimer.Stop()
	rf.pingTimer.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The pingLoop go routine sends the heartbeats periodically for leader
func (rf *Raft) pingLoop() {
	//	TDPrintf(&rf.mu, "%v [Ping] Start ping", rf)
	//	defer TDPrintf(&rf.mu, "%v [Ping] End ping", rf)
	rf.mu.Lock()
	Term := rf.currentTerm
	rf.mu.Unlock()

	for !rf.killed() {
		<-rf.pingTimer.C
		rf.pingTimer.Reset(HeartBeatInterval)

		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != Term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := 0; i < rf.peerNum; i++ {
			if i != rf.me {
				go func(server int) {
					Cnd := sync.NewCond(&rf.mu)
					voteCount := 1
					remainPost := 1
					rf.mu.Lock()
					if rf.role != Leader || rf.currentTerm != Term {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					rf.tryAppendEntries(server, Cnd, &remainPost, &voteCount, Term)
					rf.mu.Lock()
					//					DPrintf("%s [Ping] Sends ping to %d, done", rf, server)
					rf.mu.Unlock()
				}(i)
			}
		}
	}
}

// The electionLoop go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		<-rf.electionTimer.C // the follower with a valid leader would be blocked here

		rf.mu.Lock()
		rf.electionTimer.Reset(getRandElectTimeout())
		if rf.role == Leader {
			// the timer may be triggered before the new leader reset it
			// also if it has a valid leader, then
			rf.mu.Unlock()
			continue
		}
		//		DPrintf("%s [Election] Election triggered at %v", rf, timeNow)
		rf.TranState(Candidate, rf.currentTerm+1)
		TermNow := rf.currentTerm
		rf.mu.Unlock()

		voteCount := rf.broadcastVoteRequest()

		rf.mu.Lock()
		//		DPrintf("%s: [Election] Try to become leader, with %d vote(s)", rf, voteCount)
		if rf.role == Candidate && rf.currentTerm == TermNow {
			// if the candidate has not been transformed into a follower by one leader,
			//it is elected as the new leader
			if voteCount*2 > rf.peerNum {
				rf.TranState(Leader, rf.currentTerm)
			} else {
				rf.currentTerm--
				rf.persist()
			}
		}
		rf.mu.Unlock()
	}
}

// The applyLoop go routine continuously try to commit rf.log for leader
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		time.Sleep(ApplyBeatInterval)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.apply(rf.getLog(rf.lastApplied + 1))
			rf.lastApplied++
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		leaderID:          -1,
		role:              Follower,
		peerNum:           len(peers),
		voteFor:           -1,
		log:               make([]*LogEntry, LogLength),
		commitIndex:       0,
		lastApplied:       0,
		currentTerm:       0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		electionTimer:     time.NewTimer(getRandElectTimeout()),
		pingTimer:         time.NewTimer(HeartBeatInterval),
		applyCh:           applyCh,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start electionLoop goroutine to start elections
	go rf.applyLoop()
	go rf.electionLoop()

	return rf
}
