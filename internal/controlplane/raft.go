package controlplane

import (
	"os"
	"time"
)


const ElectionTimeout  = 5 * time.Second

type Node struct {
	id          uint
	currentTerm uint
	votedFor    uint
	isDead      bool
	log         []*RaftLog
	commitIndex uint
	lastApplied uint
}

type RaftModule struct { 
	leader Leader
	followers []Node
}

func(node *Node) ReplyVote() {

}

func (leader *Leader) AppendEntries(term, leaderID, prevLogIndex, prevLogTerm, leaderCommit uint, entries []string) (uint, bool) {
	return 0, true
}

type Leader struct {
	nextIndex  []uint
	node       Node
	matchIndex []uint
}

type RaftLog struct {
	logFile os.File
	entry   string
	Termindex   uint
}

type Candidate struct {
	node *Node
}

func (cand *Candidate) RequestVote(term, candidateID, lastLogIndex, lastLogTerm uint) (uint, bool) {
	///while waiting for votes, the previous leader may be partioned and if it recovers
	/// it may send out a heartbeat to all followers if the leaders current Term is == or larger than the candidates 
	/// it should step down and demote itself to a follower 
	return 0, true
}

func (leader *Leader) heartbeat() {

}


func (node *Node) Vote() {
	//should vote on a first come first bases which ever candidates request hits first 
	// each candidate checks if they are the new leader by comparing votes to majority if they are the majority 
	// then they should issue the heartbeat
}


func (cand *Candidate) beginElection() {
	cand.node.votedFor = cand.node.id
	cand.RequestVote(cand.node.currentTerm,  cand.node.id, lastLogIndex uint, lastLogTerm uint)
}

func (node *Node) newCandidate () Candidate { 
	candidate := Candidate{node}

	candidate.node.currentTerm += 1

	return candidate
}
