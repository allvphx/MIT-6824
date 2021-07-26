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

func (a *LogEntry) CheckSame(b *LogEntry) bool {
	return a.Term == b.Term && a.Index == b.Index
}

type Role int

const (
	Leader            Role = 1
	Candidate         Role = 2
	Follower          Role = 3
	HeartBeatInterval      = 110 * time.Millisecond
	ApplyBeatInterval      = 10 * time.Millisecond
	VoteTimeOut            = 3 * HeartBeatInterval
	//	AppendEntryRPCInterval = 50 * time.Millisecond
	//	EntryAppendTimeOut	   = 3 * AppendEntryRPCInterval
	LogLength       = 0
	DecIncreaseRate = 1.5
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
	// common states used for log appending
	log         []*LogEntry
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

func (rf *Raft) apply(entry *LogEntry, applychan chan ApplyMsg) {
	applychan <- ApplyMsg{
		CommandValid: true,
		CommandIndex: entry.Index,
		Command:      entry.Cmd,
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// RequestVote RPC arguments structure.
//
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

func (rv *AppendEntryReply) String() string {
	return fmt.Sprintf("[Term:%d;Success:%v]", rv.Term, rv.Success)
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
	var tailLog LogEntry = LogEntry{}
	if len(rf.log) > 0 {
		tailLog = *rf.log[len(rf.log)-1]
	}
	if term > tailLog.Term {
		return true
	}
	return term == tailLog.Term && index >= tailLog.Index
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
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && rf.isUpToDate(args.LastlogIndex, args.LastlogTerm) {
		reply.VoteGranted = true
		//		DPrintf("%s [Election] Election timer reseted", rf)
		rf.electionTimer.Reset(getRandElectTimeout())
		rf.voteFor = args.CandidateID
	}
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

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry // empty for heartbeat
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	Dec     bool // how to decrease NextIndex
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkTerm(args.Term)

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.Dec = false

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	//	DPrintf("%s [Election] Election timer reseted", rf)
	rf.electionTimer.Reset(getRandElectTimeout())
	if rf.leaderID != args.LeaderID {
		rf.TranState(Follower, args.Term)
		rf.leaderID = args.LeaderID
	}

	if len(rf.log) < args.PrevLogIndex {
		reply.Success = false
		reply.Dec = true
		return
	}

	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// is 0 for all logs to be replicated
		reply.Success = false
		reply.Dec = true
		return
	}

	if len(args.Entries) > 0 {
		DPrintf("%s [APP] %d Entries applied from %d with index %d", rf, len(args.Entries), rf.leaderID, args.PrevLogIndex)
	}

	// remove the conflict ones
	for i := 0; i < len(args.Entries); i++ {
		index := i + args.PrevLogIndex
		//		DPrintf("%s [APP] the index is %d, the log is %d", rf, index, len(rf.log))
		if index < len(rf.log) && !rf.log[index].CheckSame(args.Entries[i]) { // remove the conflict entry and the following ones
			rf.log = rf.log[:index]
		}
		if len(rf.log) <= index { // append the different entries behind
			DPrintf("%s [APP] Entries attached with separate point %d", rf, i)
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	//	TDPrintf(&rf.mu, "%s: [RPC] %d append entry to %d", rf, rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntry", args, &reply)
	if ok {
		rf.mu.Lock()
		rf.checkTerm(reply.Term)
		rf.mu.Unlock()
	}
	//	TDPrintf(&rf.mu, "%s: [RPC] %d append entry to %d done, rep: %s", rf, rf.me, server, reply)
	return ok
}

func (rf *Raft) tryAppendEntries(server int, mtx *sync.Mutex, Cnd *sync.Cond, remainPost *int, voteCount *int) {
	decLen := 1
	reps := AppendEntryReply{Dec: true}

	for {
		rf.mu.Lock()
		entries := rf.log[rf.nextIndex[server]-1:]
		tailIndex := len(rf.log)
		args := AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			Entries:      entries,
			PrevLogIndex: rf.nextIndex[server] - 1,
			LeaderCommit: rf.commitIndex,
		}
		if rf.nextIndex[server] > 1 { // default 0 Term 0 Index
			args.PrevLogTerm = rf.log[rf.nextIndex[server]-2].Term
			args.PrevLogIndex = rf.log[rf.nextIndex[server]-2].Index
		}
		rf.mu.Unlock()

		mtx.Lock()
		if *remainPost == 0 {
			mtx.Unlock()
			return
		}
		mtx.Unlock()

		ok := rf.sendAppendEntry(server, &args, &reps)

		rf.mu.Lock()
		if args.Term != rf.currentTerm {
			DPrintf("%s [APP] The term is changed from %d to %d", rf, server, args.Term, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		if rf.role != Leader || (!reps.Success && !reps.Dec) || !ok {
			// the later two will not happen
			rf.mu.Unlock()
			mtx.Lock()
			Cnd.Broadcast()
			mtx.Unlock()
			return
		}

		mtx.Lock()
		if reps.Success {
			*voteCount++
			*remainPost--
			rf.nextIndex[server] = tailIndex + 1
			rf.matchIndex[server] = tailIndex
			if *voteCount*2 > rf.peerNum || (*voteCount+*remainPost)*2 < rf.peerNum {
				*remainPost = 0
			}

			Cnd.Broadcast()
			mtx.Unlock()
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[server] = Max(1, rf.nextIndex[server]-decLen)
		decLen = int(float32(decLen)*DecIncreaseRate + 1)
		mtx.Unlock()
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex() {
	for N := len(rf.log); N > rf.commitIndex; N-- { //rf.log[N-1].Term == rf.currentTerm && ??
		matchCount := 1
		for i := 0; i < rf.peerNum; i++ {
			if i != rf.me && N <= rf.matchIndex[i] {
				matchCount++
			}
		}

		DPrintf("%s [APP] Try to inc commitIndex to %d with match count %d", rf, N, matchCount)

		if matchCount*2 > rf.peerNum { //!!!!
			rf.commitIndex = N
			break
		}
	}
}

// Send the append entry messages concurrently
func (rf *Raft) broadcastAppendEntry() {
	voteCount := 1
	mtx := sync.Mutex{}
	Cnd := sync.NewCond(&mtx)
	remainPost := rf.peerNum - 1

	for i := 0; i < rf.peerNum; i++ {
		if i != rf.me {
			go rf.tryAppendEntries(i, &mtx, Cnd, &remainPost, &voteCount)
		}
	}

	for {
		mtx.Lock()
		Cnd.Wait()
		if remainPost == 0 {
			mtx.Unlock()
			break
		}
		mtx.Unlock()
	}

	rf.mu.Lock()
	if 2*voteCount > rf.peerNum && rf.role == Leader {
		rf.updateCommitIndex()
	}
	rf.mu.Unlock()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
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
	index = len(rf.log) + 1 // it begins from where?
	DPrintf("%s [APP] client request received, with index %d", rf, index)
	newLog := &LogEntry{Term: term, Index: index, Cmd: command}
	rf.log = append(rf.log, newLog)
	go rf.broadcastAppendEntry()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
		for i := 0; i < rf.peerNum; i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = len(rf.log) + 1
		}
		rf.pingTimer.Reset(HeartBeatInterval)
		go rf.pingLoop()
		DPrintf("%s [StateChange] Changed to leader", rf)

	case Candidate:
		if rf.role != Follower {
			return false
		}
		DPrintf("%s [StateChange] Begin changing to candidate", rf)
		rf.role = Candidate
		rf.voteFor = rf.me
		rf.leaderID = -1
		DPrintf("%s [StateChange] Changed to candidate", rf)
	case Follower:
		DPrintf("%s [StateChange] Begin changing to follower with Term %d", rf, term)
		rf.role = Follower
		rf.voteFor = -1
		rf.leaderID = -1
		rand.Seed(time.Now().UnixNano())
		rf.electionTimer.Reset(getRandElectTimeout())
		DPrintf("%s [StateChange] Changed to follower with Term %d", rf, term)

	default:
		return false
	}
	return true
}

// The pingLoop go routine sends the heartbeats periodically for leader
func (rf *Raft) pingLoop() {
	TDPrintf(&rf.mu, "%v [Ping] Start ping", rf)
	defer TDPrintf(&rf.mu, "%v [Ping] End ping", rf)

	for !rf.killed() {
		<-rf.pingTimer.C
		rf.pingTimer.Reset(HeartBeatInterval)

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		//		DPrintf("%v [Ping] Start next ping round", rf)
		rf.mu.Unlock()

		for i := 0; i < rf.peerNum; i++ {
			if i != rf.me {
				go func(server int) {
					reps := AppendEntryReply{}
					rf.mu.Lock()
					if rf.role != Leader {
						rf.mu.Unlock()
						return
					}
					args := AppendEntryArgs{
						Term:         rf.currentTerm,
						Entries:      make([]*LogEntry, 0),
						LeaderID:     rf.me,
						LeaderCommit: rf.commitIndex,
					}
					if len(rf.log) > 0 {
						args.PrevLogIndex = rf.log[len(rf.log)-1].Index
						args.PrevLogTerm = rf.log[len(rf.log)-1].Term
					}
					rf.mu.Unlock()
					rf.sendAppendEntry(server, &args, &reps)
					rf.mu.Lock()
					//					DPrintf("%s [Ping] Sends ping to %d, done", rf, server)
					rf.mu.Unlock()
				}(i)
			}
		}
	}
}

// Send the vote request messages concurrently
func (rf *Raft) broadcastVoteRequest() int {
	voteCount := 1
	mtx := sync.Mutex{}
	remainPost := rf.peerNum - 1

	for i := 0; i < rf.peerNum; i++ {
		if i != rf.me {
			go func(server int) {
				reps := RequestVoteReply{}
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateID: rf.me,
				}
				if len(rf.log) > 0 {
					args.LastlogTerm = rf.log[len(rf.log)-1].Term
					args.LastlogIndex = rf.log[len(rf.log)-1].Index
				}

				mtx.Lock()
				if remainPost == 0 {
					rf.mu.Unlock()
					mtx.Unlock()
					return
				}
				mtx.Unlock()
				rf.mu.Unlock()

				ok := rf.sendRequestVote(server, &args, &reps)
				rf.mu.Lock()
				mtx.Lock()

				if ok && reps.VoteGranted && rf.role == Candidate {
					voteCount++
				}
				remainPost--
				if voteCount*2 > rf.peerNum || (voteCount+remainPost)*2 < rf.peerNum {
					remainPost = 0
				}
				mtx.Unlock()
				rf.mu.Unlock()
			}(i)
		}
	}

	for i := time.Duration(0); i < VoteTimeOut; i += HeartBeatInterval {
		time.Sleep(HeartBeatInterval)
		mtx.Lock()
		if remainPost == 0 {
			mtx.Unlock()
			return voteCount
		}
		mtx.Unlock()
	}
	// Time out , get no vote
	return 0
}

// The electionLoop go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		timeNow := <-rf.electionTimer.C // the follower with a valid leader would be blocked here

		rf.mu.Lock()
		rf.electionTimer.Reset(getRandElectTimeout())
		if rf.role == Leader {
			// the timer may be triggered before the new leader reset it
			// also if it has a valid leader, then
			rf.mu.Unlock()
			continue
		}
		DPrintf("%s [Election] Election triggered at %v", rf, timeNow)
		rf.TranState(Candidate, rf.currentTerm+1)
		rf.mu.Unlock()

		voteCount := rf.broadcastVoteRequest()

		rf.mu.Lock()
		DPrintf("%s: [Election] Try to become leader, with %d vote(s)", rf, voteCount)
		if rf.role == Candidate {
			// if the candidate has not been transformed into a follower by one leader,
			//it is elected as the new leader
			if voteCount*2 > rf.peerNum {
				rf.TranState(Leader, rf.currentTerm)
			} else {
				rf.currentTerm--
			}
		}
		rf.mu.Unlock()
	}
}

// The applyLoop go routine continuously try to commit rf.log for leader
func (rf *Raft) applyLoop(applychan chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(ApplyBeatInterval)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.apply(rf.log[rf.lastApplied-1], applychan)
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
		peers:         peers,
		persister:     persister,
		me:            me,
		leaderID:      -1,
		role:          Follower,
		peerNum:       len(peers),
		voteFor:       -1,
		log:           make([]*LogEntry, LogLength),
		commitIndex:   0,
		lastApplied:   0,
		currentTerm:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		electionTimer: time.NewTimer(getRandElectTimeout()),
		pingTimer:     time.NewTimer(HeartBeatInterval),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start electionLoop goroutine to start elections
	go rf.applyLoop(applyCh)
	go rf.electionLoop()

	return rf
}
