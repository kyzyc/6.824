package raft

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

func randTimeout(lower_time int, higher_time int) int64 {
	rand.Seed(time.Now().UnixNano())
	return int64(lower_time) + (rand.Int63() % int64(higher_time))
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
		if rf.checkNoConnection() {
			c <- true
			return
		}
	}
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.identity = CANDIDATE
}

func (rf *Raft) keepAlive() {
	rf.mu.Lock()
	termTmp := rf.currentTerm
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
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
		}(server, args, reply)
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
				return
			}
			if reply.Term > rf.currentTerm {
				rf.identity = FOLLOWER
				rf.currentTerm = reply.Term
				rf.last_connection = time.Now()
				return
			}
			if reply.VoteGranted {
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
		} (server, args, reply)
	}

	select {
	case <- success:
		return
	case <- time.After(time.Duration(rf.election_timeout) * time.Millisecond):
		return
	}
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