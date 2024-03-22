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

import "sync"
import "sync/atomic"
import "raft/labrpc"
import "time"
import "math/rand"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//

const HeartbeatInterval = 100 * time.Millisecond

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// Server states
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.

	currentTerm       int
	votedFor          int
	log               []LogEntry
	commitIndex       int
	state             ServerState
	votesReceived     map[int]bool
	electionTimeout   float64
	electionCh        ElectionChan
	lastHeartBeatTime time.Time
}

type ElectionChan struct {
	ch     chan struct{}
	closed uint32
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.lastHeartBeatTime = time.Now()
}

func (rf *Raft) leaderLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C
		rf.sendHeartbeats()
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, isLeader := rf.GetState()
	if !isLeader {
		return
	}

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendHeartbeat(i)
		}
	}
}

func (rf *Raft) sendHeartbeat(server int) {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex,

		// other fields...
	}
	reply := &AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {

	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	// Your code here (2A).
	var isleader bool = (rf.state == Leader)
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	From        int
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".
	reply.From = rf.me

	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	voteRequestTerm := args.Term
	votedFor := rf.votedFor
	candidateId := args.CandidateId

	if voteRequestTerm > currentTerm {
		// term is outdated, move forward to new term
		rf.currentTerm = voteRequestTerm
		rf.votedFor = -1 // forget node voted for in prev term
		rf.state = Follower
	}

	if voteRequestTerm == currentTerm && (votedFor == -1 || votedFor == candidateId) {
		// not voted yet OR already voted for that candidate
		reply.VoteGranted = true
		rf.votedFor = candidateId
	} else {
		// already voted for other candidate OR RequestVote is outdated
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	}

}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		// DPrintf("Not a Candidate. Fail to start election!\n")
		return
	}

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesReceived = make(map[int]bool)
	rf.votesReceived[rf.me] = true
	rf.electionCh.ch = make(chan struct{})
	rf.electionCh.closed = 0

	// Send RequestVote RPCs to all other servers
	electionTerm := rf.currentTerm
	for peer := range rf.peers {
		if peer != rf.me {
			// DPrintf("Term %d: Node %d sent RequestVote to node %d\n", rf.currentTerm, rf.me, peer)
			go rf.SendRequestVoteToPeer(peer, electionTerm)
		}
	}

	go rf.StartElectionTimer()
}

func (rf *Raft) StartElectionTimer() {
	for {
		select {
		case <-rf.electionCh.ch:
			// DPrintf("Term %d: New Leader has been elected\n", rf.currentTerm)
			return
		default:
			time.Sleep(time.Duration(rf.electionTimeout) * time.Second)

			// election timeout, start new election
			// DPrintf("Term %d: Election timeout elapsed, start new election\n", rf.currentTerm)
			rf.StartElection()
		}
	}
}

func (rf *Raft) CancelElectionTimer() {
	if atomic.SwapUint32(&rf.electionCh.closed, 1) == 0 {
		// Is there case where Election starts but SendRequestVoteToPeer routine in last term still exists ?
		// Close the channel if it's not already closed
		close(rf.electionCh.ch)
	}
}

func (rf *Raft) SendRequestVoteToPeer(peer int, electionTerm int) {
	args := RequestVoteArgs{Term: electionTerm, CandidateId: rf.me}
	reply := RequestVoteReply{}

	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	if !ok {
		// DPrintf("Term %d: Fail to send RequestVote from node %d to node %d\n", rf.currentTerm, rf.me, peer)
		return
	}

	// Handle reply
	rf.mu.Lock()
	defer rf.mu.Unlock()

	voteReplyTerm := reply.Term
	currentTerm := rf.currentTerm

	/* NEED TO CHECK THIS! */
	// if currentTerm != electionTerm {
	// 	return
	// }

	if rf.state == Candidate && voteReplyTerm == currentTerm && reply.VoteGranted {
		rf.votesReceived[reply.From] = true

		if len(rf.votesReceived) > len(rf.peers)/2 {
			// receive vote from majority of servers -> become Leader
			rf.state = Leader
			rf.CancelElectionTimer()
			// DPrintf("Term %d: Node %d becomes Leader\n", rf.currentTerm, rf.me)
		}
	} else if voteReplyTerm > currentTerm {
		rf.currentTerm = voteReplyTerm
		rf.votedFor = -1 // forget node voted for in prev term
		rf.state = Follower
		rf.CancelElectionTimer()
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

func (rf *Raft) StartServer() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			select {
			// case <- rf.heartbeatCh:
			// 	//DPrintf("Term %d: Received hearbeat from Leader", rf.currentTerm)
			// case <- time.After(rf.heartBeatTimeout):
			// 	rf.StartElection()
			}
		case Candidate:
			select {
			// case <- rf.heartbeatCh:
			// 	//DPrintf("Term %d: Received hearbeat from Leader", rf.currentTerm)
			// 	// rf.mu.Lock()
			// 	// rf.currentTerm = AppendEntriesArgs.Term
			// 	// rf.votedFor = -1 // forget node voted for in prev term
			// 	// rf.state = Follower
			// 	// rf.mu.Unlock()
			}
		}
	}
}

func getRandomTimer() float64 {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	randomTimer := 0.5 + random.Float64()*0.5

	return randomTimer
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:           peers,
		me:              me,
		currentTerm:     0,
		votedFor:        -1,
		log:             make([]LogEntry, 0),
		commitIndex:     0,
		state:           Follower,
		electionTimeout: getRandomTimer(),
	}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (2A, 2B).
	/* choose HeartBeat Timeout and Election Timeout */

	go rf.StartServer()

	go rf.leaderLoop()

	return rf
}
