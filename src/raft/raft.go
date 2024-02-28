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
	//	"bytes"

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type logEntry struct {
	Command interface{}
	Term	int
}


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type Identity int

const (
	LEADER = 0
	CANDIDATE = 1
	FOLLOWER = 2
)

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
	// persistent state
	logs 		[]logEntry		  // log entries

	currentTerm int			  	  // latest term server has seen
	votedFor	int				  // candidateId that received vote in current term (or null if none)
	// some metadata
	identity	Identity	
	last_connection	time.Time		  // whether received message from leader or candidates
	// leaderid		int
	// volatile state
	election_timeout int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity == LEADER && !rf.killed() {
		return rf.currentTerm, true
	} else {
		return rf.currentTerm, false
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int	// candidate’s term
	CandidateId 	int	// candidate requesting vote
	// for 2B
	// lastLogIndex		uint32	// index of candidate’s last log entry
	// lastLogTerm		uint32	// term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	Entries			[]logEntry
	// for future
}

type AppendEntriesReply struct {
	Term			int
	Success			bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// a peer is asking for vote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	DPrintf("%v received requestvote from %v, myterm: %v candidate term: %v", rf.me, args.CandidateId, rf.currentTerm, args.Term)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.identity = FOLLOWER
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.last_connection = time.Now()
		DPrintf("%v reply to %v: %v %v", rf.me, args.CandidateId, rf.votedFor, reply.VoteGranted)
		return
	} 

	// now args.Term == rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.last_connection = time.Now()
	} else {
		reply.VoteGranted = false
	}

	DPrintf("%v reply to %v: %v %v", rf.me, args.CandidateId, rf.votedFor, reply.VoteGranted)
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// heartbeat message
	if len(args.Entries) == 0 {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		reply.Term = rf.currentTerm
		if args.Term < rf.currentTerm {
			reply.Success = false
		} else {
			rf.last_connection = time.Now()
			rf.identity = FOLLOWER
			rf.currentTerm = args.Term
			reply.Success = true
		}
	}
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

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
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

func makeRequestVote(term int, candidateId int) (*RequestVoteArgs, *RequestVoteReply) {
	args := &RequestVoteArgs{term, candidateId}
	reply := &RequestVoteReply{}
	return args, reply
}

func makeHeartBeat(term int, leaderid int, entries []logEntry) (*AppendEntriesArgs, *AppendEntriesReply) {
	args := &AppendEntriesArgs{term, leaderid, entries}
	reply := &AppendEntriesReply{}
	return args, reply
}

func (rf *Raft) checkNoConnection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.last_connection).Milliseconds() > rf.election_timeout 
}

/**
 * who need timeout ticker?
 * only follower and candidate
 */
func (rf *Raft) timeoutTicker(c chan<- bool) {
	for !rf.killed() {
		// every 20 millisecond, start ticker
		time.Sleep(time.Duration(rf.election_timeout) * time.Millisecond)
		DPrintf("tick")
		if rf.checkNoConnection() {
			c <- true
			return
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("here")
	time.Sleep(time.Duration(rf.election_timeout) * time.Millisecond)
	rf.mu.Lock()
	if rf.identity != CANDIDATE {
		DPrintf("%v is not candidate anymore", rf.me)
		rf.mu.Unlock()
		return
	}
	rf.last_connection = time.Now()
	rf.votedFor = rf.me
	rf.currentTerm++
	termTmp := rf.currentTerm
	rf.election_timeout = randTimeout(250, 300)
	DPrintf("candidate %v start election with term %v", rf.me, termTmp)
	rf.mu.Unlock()

	received_votes := 1
	finished := 1

	var mu sync.Mutex
	//cond := sync.NewCond(&mu)

	DPrintf("start send vote to peers")

	success := make(chan bool)

	for server := range(rf.peers) {
		if server == rf.me {
			continue
		}
		args, reply := makeRequestVote(termTmp, rf.me)
		go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
			DPrintf("%v send request vote to %v", rf.me, server)
			rf.sendRequestVote(server, args, reply)
			DPrintf("%v received reply from %v", rf.me, server)
			mu.Lock()
			defer mu.Unlock()
			finished++
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if termTmp != rf.currentTerm {
				DPrintf("args.Term != rf.currentTerm")
				return
			}
			if reply.Term > rf.currentTerm {
				DPrintf("convert to follower!")
				rf.identity = FOLLOWER
				rf.currentTerm = reply.Term
				rf.last_connection = time.Now()
				return
			}
			if reply.VoteGranted {
				DPrintf("get a vote!")
				received_votes++
			}
			if received_votes >= int(math.Ceil((float64(len(rf.peers)) / 2))) {
				if rf.identity != LEADER {
					success <- true
					rf.identity = LEADER
				}
				return
			} else if finished == len(rf.peers) {
				return
			}
			//cond.Broadcast()
		} (server, args, reply)
	}

	for {
		select {
		case <- success:
			return
		case <- time.After(time.Duration(rf.election_timeout) * time.Millisecond):
			return
		}
	}
}

func (rf *Raft) keepAlive() {
	rf.mu.Lock()
	termTmp := rf.currentTerm
	rf.mu.Unlock()

	for server := range(rf.peers) {
		if (server == rf.me) {
			continue
		}
		args, reply := makeHeartBeat(termTmp, rf.me, []logEntry{})
		
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			rf.sendHeartBeat(server, args, reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.identity = FOLLOWER
				rf.currentTerm = reply.Term
			}
			rf.mu.Unlock()
		} (server, args, reply)
	}
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.identity = CANDIDATE
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		rf.mu.Lock()
		switch rf.identity {
		case FOLLOWER:
			rf.mu.Unlock()
			c := make(chan bool)
			go rf.timeoutTicker(c)
			<-c
			rf.becomeCandidate()
		case LEADER:
			// send heartbeat message
			rf.mu.Unlock()
			rf.keepAlive()
			time.Sleep(time.Duration(120) * time.Millisecond)
		case CANDIDATE:
			// only candidate can start election
			rf.mu.Unlock()
			rf.startElection()
		}
	}
}

func randTimeout(lower_time int, higher_time int) int64 {
	rand.Seed(time.Now().UnixNano())
	return int64(lower_time) + (rand.Int63() % int64(higher_time))
}


// func randTimeoutAndSleep(lower_time int, higher_time int) {
// 	ms := randTimeout(lower_time, higher_time)
// 	time.Sleep(time.Duration(ms) * time.Millisecond)
// }

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
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0

	rf.last_connection = time.Now()

	// when server start up, begin as followers (page 5, 5.2)
	rf.identity = FOLLOWER

	// initial election_timeout
	rf.election_timeout = randTimeout(250, 300)

	// voted for nobody
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// DPrintf("len of peers: %v", len(peers))
	// DPrintf("round: %v", int(math.Ceil((float64(len(rf.peers)) / 2))))

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}