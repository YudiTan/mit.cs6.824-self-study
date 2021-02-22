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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int        // latest term server has seen
	votedFor        string     // candidateId that received vote in current term
	log             []LogEntry // log entries
	commitIndex     int        // index of highest log entry known to be commited
	lastApplied     int        // index of highest log entry applied to state machine
	nextIndex       []int      // for each peer server, index of the next log entry to send to that server
	matchIndex      []int      // for each peer server, index of highest log entry known to be replicated on that server
	peerState       int        //  using the server state enum below
	electionTimeout int        // number of milliseconds before triggering re-election
	votesReceived   int        // number of votes received

	leaderChan      chan bool
	peerChan        chan bool
	electionWonChan chan bool
}

// LogEntry represents each log entry in the raft node's log.
type LogEntry struct {
	Command      string // command for state machine
	TermReceived int    // term when entry was received by leader
	Index        int    // index of this logentry
}

// ENUM of the raft server's different states.
const (
	FOLLOWER_STATE = iota
	CANDIDATE_STATE
	LEADER_STATE
)

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.peerState == LEADER_STATE
	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int    // candidate's term
	CandidateID  string // candidate requesting vote
	LastLogIndex int    // index of candidate's last log entry
	LastLogTerm  int    // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntriesArgs defines the args for AppendEntries RPC, following Figure 2
// of raft paper.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     string     // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceeding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply defines the reply for AppendEntries RPC, following Figure
// 2 of raft paper.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// the receiver is more updated  than the candidate
	if args.Term < rf.currentTerm {
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.peerState = FOLLOWER_STATE
		rf.votedFor = ""
	}

	if (rf.votedFor == "" || rf.votedFor == args.CandidateID) && CandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex, rf.log) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.peerChan <- true
		return
	}
}

// CandidateLogUpToDate returns a bool
// representing whether the candidate's logs are at least as up-to-date as
// ReceiverLogs.
// Ref: Raft paper section 5.4.1 last paragraph.
func CandidateLogUpToDate(LastLogTerm, LastLogIndex int, ReceiverLogs []LogEntry) bool {
	if len(ReceiverLogs) == 0 {
		return true
	}
	lastEntry := ReceiverLogs[len(ReceiverLogs)-1]
	// if terms equal, then the receiver's log must be shorter than or equal
	// to candidate's log
	if lastEntry.TermReceived == LastLogTerm {
		return len(ReceiverLogs)-1 <= LastLogIndex
	}
	// else, candidate must have higher term
	return lastEntry.TermReceived < LastLogTerm
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.peerState = FOLLOWER_STATE
		rf.votedFor = ""
	}
	rf.leaderChan <- true
	// see if log contains a matching entry
	logMatches := func() bool {
		if len(rf.log) < args.PrevLogIndex+1 {
			return false
		}
		entryAtIndex := rf.log[args.PrevLogIndex]
		if entryAtIndex.TermReceived != args.PrevLogTerm {
			// since there is conflicting entry, delete all log entries
			// starting from the conflicting one (at args.PrevLogIndex) onwards.
			rf.log = rf.log[:args.PrevLogIndex]
			return false
		}
		return true
	}()

	if logMatches {
		rf.log = append(rf.log, args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		}
		reply.Success = true
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// validate that the request is valid
		if rf.peerState != CANDIDATE_STATE || rf.currentTerm != args.Term {
			return ok
		}
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		if rf.currentTerm < reply.Term {
			rf.currentTerm = args.Term
			rf.peerState = FOLLOWER_STATE
			rf.votedFor = ""
			return ok
		}
		if reply.VoteGranted {
			rf.votesReceived++
			// if majority vote received, this node wins election.
			if rf.votesReceived > len(rf.peers)/2 {
				rf.peerState = LEADER_STATE
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
			}
		}
	}
	return ok
}

// sendAppendEntries calls the AppendEntries rpc
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// validate the request
	if !ok || rf.peerState != LEADER_STATE || args.Term != rf.currentTerm {
		return ok
	}
	// convert to follower state
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.peerState = FOLLOWER_STATE
		rf.votedFor = ""
		return ok
	}
	if reply.Success {
		if len(args.Entries) > 0 {
			// actual appendentry command
			ni := args.Entries[len(args.Entries)-1].Index
			rf.nextIndex[server] = ni
			rf.matchIndex[server] = ni - 1
		}
	}
	return ok
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
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.peerState == LEADER_STATE {
			go rf.sendAppendEntries(server, &AppendEntriesArgs{}, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) sendRequestVotes() {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  fmt.Sprintf("%v", rf.me),
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].TermReceived,
	}
	rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me && rf.peerState == CANDIDATE_STATE {
			go rf.sendRequestVote(server, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) Run() {
	for {
		switch rf.peerState {
		case FOLLOWER_STATE:
			// this is a common go pattern to enforce timeouts when waiting to hear back from channels
			select {
			// this node can either hear from leaders (heartbeat / appendEntry)
			// or from peers (which requested its vote for election)
			case <-rf.leaderChan:
			case <-rf.peerChan:
			// start a leader election if we do not hear back from peers
			case <-time.After(time.Millisecond * time.Duration(rf.electionTimeout)):
				rf.peerState = CANDIDATE_STATE
			}
		case CANDIDATE_STATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = fmt.Sprintf("%v", rf.me)
			rf.votesReceived = 1
			rf.mu.Unlock()
			go rf.sendRequestVotes()

			// again we set a timeout for this election
			select {
			// if during election we heard back from a leader, we immediately revert back to follower state
			case <-rf.leaderChan:
				rf.peerState = FOLLOWER_STATE
				// if timeout, we remain in candidate state and start a new election phase
			case <-time.After(time.Millisecond * time.Duration(rf.electionTimeout)):
				// if we won election, we get notified and immediately transition to leader_state
			case <-rf.electionWonChan:
			}
		case LEADER_STATE:
			// send heartbeats no more than 10 times per second
			go rf.sendHeartbeats()
			time.Sleep(time.Millisecond * 110)
		}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// set election timeout to be between 200 and 350 milliseconds
	rf.electionTimeout = rand.Intn(150) + 200
	rf.peerState = FOLLOWER_STATE
	rf.currentTerm = 0
	rf.votedFor = ""
	rf.log = []LogEntry{LogEntry{TermReceived: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesReceived = 0
	rf.leaderChan = make(chan bool, 100)
	rf.peerChan = make(chan bool, 100)
	rf.electionWonChan = make(chan bool, 100)
	// start a goroutine to kickoff leader election periodically
	// by sending out RequestVote RPCs when it hasnt heard back
	// from another peer for a while
	go rf.Run()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
