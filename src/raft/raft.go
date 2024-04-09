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
	"math/rand"
	"raft/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//

const HeartbeatInterval = 100

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index	int
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
	/* Part 2A */
	currentTerm     int
	votedFor        int
	state           ServerState
	votesReceived   map[int]bool
	electionTimeout float64
	heartbeatCh     chan *AppendEntriesArgs
	electionCh      chan bool
	leaderAlive     bool

	/* Part 2B */
	log         []LogEntry
	commitIndex int // index of highest log entry known to be committed
	lastApplied	int
	nextIndex	[]int // index of next log entry
	matchIndex  []int // index of highest log entry known to be replicated
	applyCh     chan ApplyMsg

	logLock   	sync.Mutex

	Pause bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	
	/* Part 2B */
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Conflicted bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("entering AppendEntries")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.heartbeatCh <- args
	reply.Conflicted = false

	/* HANDLE NETWORK FAILURE WHERE OLD LEADER STILL ASSUMES IT IS STILL LEADER */
	if args.Term < rf.currentTerm {
		DPrintf("Term %d: Node %d received HB from old leader %d!\n", rf.currentTerm, rf.me, args.LeaderId)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	leaderPrevLogIndex := args.PrevLogIndex
	leaderPrevLogTerm := args.PrevLogTerm

	// Consistency check
	DPrintf("Consistency check on server %d args PrevIdx %d <= lastIdx %d, args PrevTerm %d\n", rf.me, leaderPrevLogIndex, rf.getLastLogIndex(), args.PrevLogTerm)
	DPrintf("leader commit log length to server %d: %d\n", rf.me, len(args.Entries))

	if (leaderPrevLogIndex <= rf.getLastLogIndex() && leaderPrevLogTerm == rf.log[leaderPrevLogIndex].Term) {
		if (len(args.Entries) > 0) {
			DPrintf("Term %d: Node %d receives log entry from leader %d\n", rf.currentTerm, rf.me, args.LeaderId)
			if leaderPrevLogIndex < rf.getLastLogIndex() {
				rf.log = rf.log[:leaderPrevLogIndex + 1]
				DPrintf("removed all elements up to index %d", leaderPrevLogIndex)
			}
			for _, e := range args.Entries {
				rf.log = append(rf.log, e)
				DPrintf("Follower %d appending entry %d at %d\n", rf.me, e.Command, e.Index)
			}
			rf.Pause = false
			DPrintf("unpausing")
		}
	} else {
		// Log mismatch
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Conflicted = true
		rf.Pause = true
		DPrintf("pausing")
		return
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex && !rf.Pause {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			e := rf.log[i]
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index}
			DPrintf("Follower %d apply command %d at %d\n", rf.me, e.Command, e.Index)
			rf.lastApplied = i
		}
	}

	DPrintf("new commit index on server %d commitIndex %d\n", rf.me, rf.commitIndex)
	
}

func (rf *Raft) LeaderLoop() {
	rf.mu.Lock()

	if (rf.state != Candidate) {
		rf.mu.Unlock()
		return
	}

	rf.state = Leader

	/* Update log info */
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := range rf.peers {
		if peer != rf.me {
			rf.nextIndex[peer] = rf.getLastLogIndex() + 1
			DPrintf("peer %d initalized as %d", peer, rf.getLastLogIndex() + 1)
		}
	}

	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		state := rf.state
		leaderTerm := rf.currentTerm
		lastLogIndex := rf.getLastLogIndex()
		rf.mu.Unlock()

		if state == Leader {
			for peer := range rf.peers {
				if peer != rf.me {
					if (lastLogIndex >= rf.nextIndex[peer]) {
						// send log entries
						rf.mu.Lock()
						args := AppendEntriesArgs{
							Term:         leaderTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.log[rf.nextIndex[peer] - 1].Index,
							PrevLogTerm:  rf.log[rf.nextIndex[peer] - 1].Term,
							Entries:      rf.log[rf.nextIndex[peer]:],
							LeaderCommit: rf.commitIndex,
						}
						rf.mu.Unlock()
						DPrintf("Term %d: Node %d starts sending log entry\n", leaderTerm, rf.me)
						go rf.sendAppendEntries(peer, args)

					} else {
						// send heart beat
						rf.mu.Lock()
						args := AppendEntriesArgs{
							Term:     leaderTerm,
							LeaderId: rf.me,
							PrevLogIndex: rf.log[rf.nextIndex[peer] - 1].Index,
							PrevLogTerm:  rf.log[rf.nextIndex[peer] - 1].Term,
							Entries:  []LogEntry{},
							LeaderCommit: rf.commitIndex,
						}
						rf.mu.Unlock()
						DPrintf("Term %d: Node %d starts sending hb\n", leaderTerm, rf.me)
						go rf.sendAppendEntries(peer, args)
					}
				}
			}
			time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
		} else {
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	isReplicated := false

	if len(args.Entries) != 0 {
		isReplicated = true
	}

	reply := AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	for !ok {
		DPrintf("Term %d: Leader %d fails to send hb to node %d\n", rf.currentTerm, rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	/* HANDLE NETWORK FAILURE WHERE OLD LEADER STILL ASSUMES IT IS STILL LEADER */
	if !reply.Success && !reply.Conflicted {
		rf.currentTerm = reply.Term
		rf.votedFor = -1 // forget node voted for in previous term
		rf.state = Follower
		rf.votesReceived = map[int]bool{}
		go rf.StartServer()
		DPrintf("Term %d: Leader %d downgrades to Follower\n", rf.currentTerm, rf.me)
		return
	}

	if (!reply.Success && reply.Conflicted) {
		rf.nextIndex[server]--;
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		return
	}

	// check majority of replication
	if (isReplicated) {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		DPrintf("server %d's next index incremented to: %d", server, rf.nextIndex[server])
	}

	//if isReplicated && (rf.commitIndex < args.Entries[0].Index) {
		for n := rf.getLastLogIndex(); n >= rf.commitIndex; n-- {
			ack := 1
			if rf.log[n].Term == rf.currentTerm {
				for peer := 0; peer < len(rf.peers); peer++ {
					if peer != rf.me && rf.matchIndex[peer] >= n {
						ack++
					}
				}
			}

			if ack > len(rf.peers) / 2 {
				rf.commitIndex = n
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					e := rf.log[i]
					rf.applyCh <- ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index}
					rf.lastApplied = i
					DPrintf("Term %d: Leader %d apply command %d\n", rf.currentTerm, rf.me, e.Command)
				}
				break
			}
		}
	//}

	DPrintf("leader commit index: %d", rf.commitIndex)
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int = rf.currentTerm
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

	/* Part 2B */
	LastLogIndex int
	LastLogTerm  int
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Term %d: Node %d receive reqVote from node %d (term %d)\n", rf.currentTerm, rf.me, args.CandidateId, args.Term)
	reply.From = rf.me

	voteRequestTerm := args.Term
	candidateId := args.CandidateId
	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm
	receiverLastLogTerm := rf.log[rf.getLastLogIndex()].Term
	receiverLastLogIndex := rf.log[rf.getLastLogIndex()].Index

	if voteRequestTerm > rf.currentTerm {
		// term is outdated, move forward to new term
		rf.currentTerm = voteRequestTerm
		rf.votedFor = -1 // forget node voted for in prev term
		rf.state = Follower
		DPrintf("Term %d: Moving node %d to new term by %d\n", rf.currentTerm, rf.me, candidateId)
	}
	DPrintf("candidateLastLogTerm: %d", candidateLastLogTerm)
	if voteRequestTerm == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == candidateId) {
		if candidateLastLogTerm != -1 {
			if candidateLastLogTerm > receiverLastLogTerm || ((candidateLastLogTerm == receiverLastLogTerm) && (candidateLastLogIndex >= receiverLastLogIndex)) {
				reply.VoteGranted = true
				reply.Term = voteRequestTerm
				rf.votedFor = candidateId
				DPrintf("Term %d: Node %d voted for node %d\n", rf.currentTerm, rf.me, candidateId)
			}
		} else {
			// not voted yet OR already voted for that candidate and candidate is up to date
			reply.VoteGranted = true
			reply.Term = voteRequestTerm
			rf.votedFor = candidateId
			DPrintf("Term %d: Node %d voted for node %d\n", rf.currentTerm, rf.me, candidateId)
		}
	} else {
		// already voted for other candidate OR RequestVote is outdated OR candidate is not up to date
		DPrintf("Term %d: Node %d already voted for node %d/or ReqVote outdated\n", rf.currentTerm, rf.me, rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		DPrintf("Term %d: Node %d did not get voted by node %d\n", rf.currentTerm, candidateId, rf.me)
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		DPrintf("Not a Candidate. Fail to start election!\n")
		return
	}

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesReceived = make(map[int]bool)
	rf.votesReceived[rf.me] = true

	// To restart timer ???
	rf.leaderAlive = true

	// Send RequestVote RPCs to all other servers
	electionTerm := rf.currentTerm
	for peer := range rf.peers {
		if peer != rf.me {
			DPrintf("Term %d: Node %d sent RequestVote to node %d\n", rf.currentTerm, rf.me, peer)
			go rf.SendRequestVoteToPeer(peer, electionTerm, rf.log[rf.getLastLogIndex()].Index, rf.log[rf.getLastLogIndex()].Term)
		}
	}

	go rf.CandidateLoop()
}

func (rf *Raft) SendRequestVoteToPeer(peer int, electionTerm int, lastLogIndex int, lastLogTerm int) {
	args := RequestVoteArgs{Term: electionTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	reply := RequestVoteReply{}

	ok := rf.peers[peer].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		DPrintf("Term %d: Fail to send RequestVote from node %d to node %d\n", rf.currentTerm, rf.me, peer)
		return
	}

	// Handle reply
	rf.mu.Lock()
	defer rf.mu.Unlock()

	voteReplyTerm := reply.Term
	currentTerm := rf.currentTerm

	/* NEED TO CHECK THIS! */
	if currentTerm != electionTerm {
		return
	}

	if rf.state == Candidate && voteReplyTerm == currentTerm && reply.VoteGranted {
		DPrintf("Term %d: Node %d got vote from node %d\n", rf.currentTerm, rf.me, reply.From)
		rf.votesReceived[reply.From] = true
		DPrintf("Term %d: Node %d : received votes = %d, majority = %d\n", rf.currentTerm, rf.me, len(rf.votesReceived), len(rf.peers)/2)

		if len(rf.votesReceived) > len(rf.peers)/2 {
			// receive vote from majority of servers -> become Leader
			rf.leaderAlive = true
			go rf.LeaderLoop()
			DPrintf("Term %d: Node %d becomes Leader\n", rf.currentTerm, rf.me)
			return
		}
	} else if voteReplyTerm > currentTerm {
		rf.currentTerm = voteReplyTerm
		rf.votedFor = -1 // forget node voted for in prev term
		rf.state = Follower

		/* CHECK THIS !!!!!!!!*/
		rf.leaderAlive = true
	}
}

func (rf *Raft) CandidateLoop() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == Candidate {
			select {
			case args := <-rf.heartbeatCh:
				rf.mu.Lock()
				defer rf.mu.Unlock()

				DPrintf("Term %d: Candidate node %d Received heartbeat from Leader, downgrade to Follower\n", rf.currentTerm, rf.me)

				rf.currentTerm = args.Term
				rf.votedFor = -1 // forget node voted for in previous term
				rf.state = Follower

				/* CHECK THIS !!!!!!!!*/
				rf.leaderAlive = true
				go rf.StartServer()
				return
			case electionActive := <-rf.electionCh:
				if !electionActive {
					return
				}
			}
		} else {
			return
		}
	}
}

func (rf *Raft) StartElectionTimer() {
	for {
		time.Sleep(time.Duration((rf.electionTimeout)*1000) * time.Millisecond)

		rf.mu.Lock()

		if rf.state == Leader {
			rf.mu.Unlock()
			//rf.leaderAlive = true
			return
		}

		if rf.leaderAlive {
			rf.leaderAlive = false
			rf.mu.Unlock()
			continue
		}

		// election timeout, start new election
		DPrintf("Term %d: Election timeout elapsed at %f, node %d start new election\n", rf.currentTerm, getCurrentTime(), rf.me)

		if rf.state == Candidate {
			// remove all old candidates
			rf.electionCh <- false
		}

		rf.state = Candidate
		go rf.StartElection()
		rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.state != Leader  {
		return index, rf.currentTerm, false
	}

	index, term = len(rf.log), rf.currentTerm
	rf.log = append(rf.log, LogEntry{Command: command, Term: term, Index: rf.getLastLogIndex() + 1})
	rf.matchIndex[rf.me] += 1

	DPrintf("Term %d: Leader %d received command %d from client\n", term, rf.me, command)

	return index, term, isLeader
}

func (rf *Raft) getLastLogIndex() int {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	return len(rf.log) - 1;
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
	go rf.StartElectionTimer()

	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == Follower {
			select {
			case args := <-rf.heartbeatCh:
				rf.mu.Lock()
				rf.leaderAlive = true
				if args.Entries == nil {
					DPrintf("Term %d: Node %d receives hb from leader %d\n", rf.currentTerm, rf.me, args.LeaderId)
				}
				rf.mu.Unlock()
				DPrintf("Reset timer\n")
			}
		} else {
			return
		}
	}
}

func getRandomTimer() float64 {
	// Generate a random float64 between 0 and 0.5, then add 0.5 to it
	rand.Seed(time.Now().UnixNano())
	randomNum := rand.Float64()*0.5 + 0.5

	return randomNum
}

func getCurrentTime() float64 {
	return float64(time.Now().UnixNano()) / 1_000_000_000.0
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
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
		state:           Follower,
		electionTimeout: getRandomTimer(),
		heartbeatCh:     make(chan *AppendEntriesArgs),
		leaderAlive:     false,
		electionCh:      make(chan bool),
		
		/* Part 2B */
		log:             make([]LogEntry, 0),
		commitIndex:     0,
		lastApplied:     0,
		applyCh: 		 applyCh,
		Pause: false,
	}

	//DPrintf("Node %d, timeout %f\n", rf.me, rf.electionTimeout)
	// Your initialization code here (2A, 2B).

	rf.log = append(rf.log, LogEntry{Command: nil, Term: -1, Index: 0}) // log index starts at 1
	rf.applyCh <- ApplyMsg{CommandValid: true, Command: nil, CommandIndex: 0}

	go rf.StartServer()

	return rf
}
