package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "labgob"

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

type State int

const Follower State = 0
const Candidate State = 1
const Leader State = 2

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	state          State
	elapseTimer    *time.Timer
	regularTimer   *time.Timer
	heartbeatTimer *time.Timer

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastEntryTime int64
}

type LogEntry struct {
	Command []byte
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	DPrintf("%d state %d", rf.me, rf.state)
	return rf.currentTerm, rf.state == Leader
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.commitIndex {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
	rf.mu.Unlock()
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
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader’s Term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.resetTimer()
	if rf.state == Follower {
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	reply.Success = true
	if args.Entries != nil {
		if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
		for idx, entry := range args.Entries {
			if len(rf.log) > args.PrevLogIndex+1+idx {
				if entry.Term != rf.log[args.PrevLogIndex+1+idx].Term {
					rf.log = rf.log[0:args.PrevLogIndex+1+idx]
					rf.log = append(rf.log, entry)
				}
			} else {
				rf.log = append(rf.log, entry)
			}
		}

		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log) {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
		}
	}
	rf.mu.Unlock()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).

	return index, term, rf.state == Leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func NewElectionDuration() time.Duration {
	var electionTimeout = int64(rand.Int()%150+150) * time.Millisecond.Nanoseconds()
	return time.Duration(electionTimeout)
}

const heartBeatPeriod = 120 * time.Millisecond

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
	rf.state = Follower

	rand.Seed(int64(me))

	rf.elapseTimer = time.NewTimer(NewElectionDuration())
	rf.regularTimer = time.NewTimer(100 * time.Microsecond)
	rf.heartbeatTimer = time.NewTimer(heartBeatPeriod)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start backend loop
	go func() {
		// In the beginning, all are followers.
		for {
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied],
					CommandIndex: rf.lastApplied,
				}
			}
			select {
			case <-rf.elapseTimer.C:
				rf.mu.Lock()
				rf.resetTimer()
				if rf.state == Follower {
					rf.state = Candidate
					rf.currentTerm++
					DPrintf("%d self accumulate term to %d at %d", rf.me, rf.currentTerm, time.Now().Nanosecond())
				}
				if rf.state == Candidate {
					rf.startElection()
				}
				rf.mu.Unlock()
			case <-rf.heartbeatTimer.C:
				DPrintf("%d heartbeat timer expired.", rf.me)
				rf.mu.Lock()
				rf.heartbeatTimer.Stop()
				rf.heartbeatTimer.Reset(heartBeatPeriod)
				if rf.state == Leader {
					DPrintf("%d send heartbeat normally.", rf.me)
					rf.sendHeartBeat()
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}

func (rf *Raft) startElection() {
	grantCount := 0
	finishCount := 0
	DPrintf("%d start election", rf.me)
	rf.resetTimer()
	grantChannel := make(chan bool)
	for peer := 0; peer < len(rf.peers); peer++ {
		go func(peer int, grantChannel chan bool) {
			if peer != rf.me {
				lastLogTerm := 0
				lastLogIndex := 0
				if len(rf.log) > 0 {
					lastLogTerm = rf.log[len(rf.log)-1].Term
					lastLogIndex = len(rf.log)
				}
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &args, &reply)
				DPrintf("%d voted for %d:%t.", peer, args.CandidateId, reply.VoteGranted)
				if ok && reply.VoteGranted {
					grantChannel <- true
				} else {
					grantChannel <- false
				}
				return
			}
			grantChannel <- true
		}(peer, grantChannel)
	}
	for finishCount < len(rf.peers) && grantCount < len(rf.peers) / 2 + 1 {
		select {
		case g := <-grantChannel:
			if g {
				grantCount++
			}
			finishCount++
		}
	}
	DPrintf("Number of votes for %d: %d", rf.me, grantCount)
	if grantCount >= len(rf.peers) / 2 + 1 {
		rf.state = Leader
		rf.sendHeartBeat()
	}
}

func (rf *Raft) sendHeartBeat() {
	DPrintf("New heartbeat from leader %d at %d", rf.me, time.Now().Nanosecond())
	heartbeat := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := &AppendEntriesReply{

	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			DPrintf("%d send heartbeat to %d", rf.me, peer)
			for !rf.sendAppendEntries(peer, heartbeat, reply) {

			}
		}
		DPrintf("%d peer %d sent.", rf.me, peer)
	}
	DPrintf("%d finished sending heart beats.", rf.me)
}

func (rf *Raft) resetTimer() {
	var electionTimeout = int64(rand.Int()%150+150) * time.Millisecond.Nanoseconds()
	DPrintf("%d reset election timeout to %d", rf.me, electionTimeout)
	rf.elapseTimer.Stop()
	rf.elapseTimer.Reset(time.Duration(electionTimeout))
}
