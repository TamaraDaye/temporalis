package controlplane

import (
	"os"
	"time"
)

type Request uint 


const ( 
	AppendEntry Request = iota
	ReplyEntry
	ReplyVote
	Elect
)


type RPC struct {
	request Request
}

type Node struct {
	id          uint //server id
	currentTerm uint //which term of the module is in place
	votedFor    uint // leader server voted for
	isDead      bool // server status
	log         []*RaftLog // list of logs 
	commitIndex uint // last log committed
	lastApplied uint // last log applied to state machine
}



type RaftModule struct { 
	leader Leader
	followers []Node
}

func(node *Node) Vote() {

}

func (leader *Leader) AppendEntries(term, leaderID, prevLogIndex, prevLogTerm, leaderCommit uint, entries []string) (uint, bool) {
	/// log entries should committed only one when a quorum of majority have replicated the entrty
	return 0, true
}

type Leader struct {
	nextIndex  []uint //list of logs for each follower to be sent
	node       Node // leader's details =
	matchIndex []uint 
}

type RaftLog struct {
	logFile os.File
	entry   string
	TermIndex   uint
}

type Candidate struct {
	node *Node
}

func (cand *Candidate) RequestVote(term, lastLogIndex, lastLogTerm uint) (uint, bool) {
	///while waiting for votes, the previous leader may be partioned and if it recovers
	/// it may send out a heartbeat to all followers if the leaders current Term is == or larger than the candidates 
	/// it should step down and demote itself to a follower 
	return 0, true
}


func (node *Node) replyEntry () {

}


func (node *Node) Vote() {
	//should vote on a first come first bases which ever candidates request hits first 
	// each candidate checks if they are the new leader by comparing votes to majority if they are the majority 
	// then they should issue the heartbeat
}


func (cand *Candidate) beginElection() {
	cand.node.votedFor = cand.node.id
	cand.RequestVote(cand.node.currentTerm,  cand.node.id, lastLogIndex, lastLogTerm) 
}


func(leader *Leader) SendSnapshot (lastIncludedIndex, lastIncludedTerm, uint, data []byte){

}

func (node *Node) newCandidate () Candidate { 
	candidate := Candidate{node}

	candidate.node.currentTerm += 1

	return candidate
}
