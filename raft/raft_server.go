package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	unknownLeader = 0
	noVote        = 0
	leader        = "Leader"
	candidate     = "Candidate"
	follower      = "Follower"
)

var (
	MinimumElectionTimeoutMS int32 = 250
	maximumElectionTimeoutMS       = 2 * MinimumElectionTimeoutMS
)

type requestVote struct {
	Term         uint64 `json:"term"`
	CandidateID  uint64 `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type requestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	reason      string
}

type appendEntries struct {
	Term         uint64     `json:"term"`
	LeaderID     uint64     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []logEntry `json:"entries"`
	CommitIndex  uint64     `json:"commit_index"`
}

type appendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	reason  string
}

type appendEntriesTuple struct {
	Request  appendEntries
	Response chan appendEntriesResponse
}

type requestVoteTuple struct {
	Request  requestVote
	Response chan requestVoteResponse
}

type protectedString struct {
	sync.RWMutex
	value string
}

func (s *protectedString) Get() string {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *protectedString) Set(value string) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

type protectedBool struct {
	sync.RWMutex
	value bool
}

func (s *protectedBool) Get() bool {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *protectedBool) Set(value bool) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

func maximumElectionTimeout() time.Duration {
	return time.Duration(maximumElectionTimeoutMS) * time.Millisecond
}

func electionTimeout() time.Duration {
	n := rand.Intn(int(maximumElectionTimeoutMS - MinimumElectionTimeoutMS))
	d := int(MinimumElectionTimeoutMS) + n
	return time.Duration(d) * time.Millisecond
}

func broadcastInterval() time.Duration {
	d := MinimumElectionTimeoutMS / 10
	return time.Duration(d) * time.Millisecond
}

type Server struct {
	id      uint64
	state   *protectedString
	running *protectedBool
	leader  uint64
	term    uint64
	vote    uint64
	log     *rlog
	config  *configuration

	appendEntriesChan chan appendEntriesTuple
	requestVoteChan   chan requestVoteTuple
	commandChan       chan commandTuple
	configurationChan chan configurationTuple

	electionTick <-chan time.Time
	quit         chan chan struct{}
}

type ApplyFunc func(commitIndex uint64, cmd []byte) []byte

func NewServer(id uint64, store io.ReadWriter, a ApplyFunc) *Server {
	if id <= 0 {
		panic("server id must be > 0")
	}

	rlog := newRaftLog(store, a)
	latestTerm := rlog.lastTerm()

	s := &Server{
		id:      id,
		state:   &protectedString{value: follower}, // "when servers start up they begin as followers"
		running: &protectedBool{value: false},
		leader:  unknownLeader, // unknown at startup
		log:     rlog,
		term:    latestTerm,
		config:  newConfiguration(peerMap{}),

		appendEntriesChan: make(chan appendEntriesTuple),
		requestVoteChan:   make(chan requestVoteTuple),
		commandChan:       make(chan commandTuple),
		configurationChan: make(chan configurationTuple),

		electionTick: nil,
		quit:         make(chan chan struct{}),
	}
	s.resetElectionTimeout()
	return s
}

type configurationTuple struct {
	Peers []Peer
	Err   chan error
}

func (s *Server) SetConfiguration(peers ...Peer) error {
	if !s.running.Get() {
		s.config.directSet(makePeerMap(peers...))
		return nil
	}

	err := make(chan error)
	s.configurationChan <- configurationTuple{peers, err}
	return <-err
}

func (s *Server) Start() {
	go s.loop()
}

func (s *Server) Stop() {
	q := make(chan struct{})
	s.quit <- q
	<-q
	s.logFmt("server stopped")
}

type commandTuple struct {
	Command         []byte
	CommandResponse chan<- []byte
	Err             chan error
}

func (s *Server) Command(cmd []byte, response chan<- []byte) error {
	err := make(chan error)
	s.commandChan <- commandTuple{cmd, response, err}
	return <-err
}

func (s *Server) appendEntries(ae appendEntries) appendEntriesResponse {
	t := appendEntriesTuple{
		Request:  ae,
		Response: make(chan appendEntriesResponse),
	}
	s.appendEntriesChan <- t
	return <-t.Response
}

func (s *Server) requestVote(rv requestVote) requestVoteResponse {
	t := requestVoteTuple{
		Request:  rv,
		Response: make(chan requestVoteResponse),
	}
	s.requestVoteChan <- t
	return <-t.Response
}

func (s *Server) loop() {
	s.running.Set(true)
	for s.running.Get() {
		switch state := s.state.Get(); state {
		case follower:
			s.followerSelect()
		case candidate:
			s.candidateSelect()
		case leader:
			s.leaderSelect()
		default:
			panic(fmt.Sprintf("unknown Server State '%s'", state))
		}
	}
}

func (s *Server) resetElectionTimeout() {
	s.electionTick = time.NewTimer(electionTimeout()).C
}

func (s *Server) logFmt(format string, args ...interface{}) {
	prefix := fmt.Sprintf("state=%s id=%d term=%d: ", s.id, s.term, s.state.Get())
	log.Printf(prefix+format, args...)
}

func (s *Server) logRequestVoteResp(req requestVote, resp requestVoteResponse, stepDown bool) {
	s.logFmt(
		"got RequestVote, candidate=%d: responded with granted=%v (reason='%s') stepDown=%v",
		req.CandidateID,
		resp.VoteGranted,
		resp.reason,
		stepDown,
	)
}

func (s *Server) logAppendEntriesResp(req appendEntries, resp appendEntriesResponse, stepDown bool) {
	s.logFmt(
		"got appendEntries, sz=%d leader=%d prevIndex/Term=%d/%d commitIndex=%d: responded with success=%v (reason='%s') stepDown=%v",
		len(req.Entries),
		req.LeaderID,
		req.PrevLogIndex,
		req.PrevLogTerm,
		req.CommitIndex,
		resp.Success,
		resp.reason,
		stepDown,
	)
}

func (s *Server) handleQuit(q chan struct{}) {
	s.logFmt("got quit signal")
	s.running.Set(false)
	close(q)
}

func (s *Server) forwardCommand(t commandTuple) {
	switch s.leader {
	case unknownLeader:
		s.logFmt("got command, but don't know leader")
		t.Err <- errors.New("unknown leader")

	case s.id:
		panic("impossible state in forwardCommand")

	default:
		leader, ok := s.config.get(s.leader)
		if !ok {
			panic("invalid state in peers")
		}
		s.logFmt("got command, forwarding to leader (%d)", s.leader)
		go func() { t.Err <- leader.callCommand(t.Command, t.CommandResponse) }()
	}
}

func (s *Server) forwardConfiguration(t configurationTuple) {
	switch s.leader {
	case unknownLeader:
		s.logFmt("got configuration, but don't know leader")
		t.Err <- errors.New("unknown leader")
	case s.id:
		panic("impossible state in forwardConfiguration")
	default:
		leader, ok := s.config.get(s.leader)
		if !ok {
			panic("invalid state in peers")
		}
		s.logFmt("got configuration, forwarding to leader (%d)", s.leader)
		go func() { t.Err <- leader.callSetConfiguration(t.Peers...) }()
	}
}

func (s *Server) followerSelect() {
	for {
		select {
		case q := <-s.quit:
			s.handleQuit(q)
			return
		case t := <-s.commandChan:
			s.forwardCommand(t)
		case t := <-s.configurationChan:
			s.forwardConfiguration(t)
		case <-s.electionTick:
			if s.config == nil {
				s.logFmt("election timeout, but no configuration: ignoring")
				s.resetElectionTimeout()
				continue
			}
			s.logFmt("election timeout, becoming candidate")
			s.term++
			s.vote = noVote
			s.leader = unknownLeader
			s.state.Set(candidate)
			s.resetElectionTimeout()
			return

		case t := <-s.appendEntriesChan:
			if s.leader == unknownLeader {
				s.leader = t.Request.LeaderID
				s.logFmt("discovered Leader %d", s.leader)
			}
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResp(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				if s.leader != unknownLeader {
					s.logFmt("abandoning old leader=%d", s.leader)
				}
				s.logFmt("following new leader=%d", t.Request.LeaderID)
				s.leader = t.Request.LeaderID
			}

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResp(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				if s.leader != unknownLeader {
					s.logFmt("abandoning old leader=%d", s.leader)
				}
				s.logFmt("new leader unknown")
				s.leader = unknownLeader
			}
		}
	}
}

func (s *Server) candidateSelect() {
	if s.leader != unknownLeader {
		panic("known leader when entering candidateSelect")
	}
	if s.vote != 0 {
		panic("existing vote when entering candidateSelect")
	}
	requestVoteResponses, canceler := s.config.allPeers().except(s.id).requestVotes(requestVote{
		Term:         s.term,
		CandidateID:  s.id,
		LastLogIndex: s.log.lastIndex(),
		LastLogTerm:  s.log.lastTerm(),
	})
	defer canceler.Cancel()

	votes := map[uint64]bool{s.id: true}
	s.vote = s.id
	s.logFmt("term=%d election started (configuration state %s)", s.term, s.config.state)

	if s.config.pass(votes) {
		s.logFmt("Immediately won the election")
		s.leader = s.id
		s.state.Set(leader)
		s.vote = noVote
		return
	}

	for {
		select {
		case q := <-s.quit:
			s.handleQuit(q)
			return

		case t := <-s.commandChan:
			s.forwardCommand(t)

		case t := <-s.configurationChan:
			s.forwardConfiguration(t)

		case t := <-requestVoteResponses:
			s.logFmt("got vote: id=%d term=%d granted=%v", t.id, t.response.Term, t.response.VoteGranted)
			if t.response.Term > s.term {
				s.logFmt("got vote from future term (%d>%d); abandoning election", t.response.Term, s.term)
				s.leader = unknownLeader
				s.state.Set(follower)
				s.vote = noVote
				return
			}
			if t.response.Term < s.term {
				s.logFmt("got vote from past term (%d<%d); ignoring", t.response.Term, s.term)
				break
			}
			if t.response.VoteGranted {
				s.logFmt("%d voted for me", t.id)
				votes[t.id] = true
			}
			if s.config.pass(votes) {
				s.logFmt("I won the election")
				s.leader = s.id
				s.state.Set(leader)
				s.vote = noVote
				return
			}

		case t := <-s.appendEntriesChan:
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResp(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logFmt("after an appendEntries, stepping down to Follower (leader=%d)", t.Request.LeaderID)
				s.leader = t.Request.LeaderID
				s.state.Set(follower)
				return
			}

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResp(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logFmt("after a requestVote, stepping down to Follower (leader unknown)")
				s.leader = unknownLeader
				s.state.Set(follower)
				return // lose
			}

		case <-s.electionTick:
			s.logFmt("election ended with no winner; incrementing term and trying again")
			s.resetElectionTimeout()
			s.term++
			s.vote = noVote
			return
		}
	}
}

type nextIndex struct {
	sync.RWMutex
	m map[uint64]uint64 // followerId: nextIndex
}

func newNextIndex(pm peerMap, defaultNextIndex uint64) *nextIndex {
	ni := &nextIndex{
		m: map[uint64]uint64{},
	}
	for id := range pm {
		ni.m[id] = defaultNextIndex
	}
	return ni
}

func (ni *nextIndex) bestIndex() uint64 {
	ni.RLock()
	defer ni.RUnlock()

	if len(ni.m) <= 0 {
		return 0
	}

	i := uint64(math.MaxUint64)
	for _, nextIndex := range ni.m {
		if nextIndex < i {
			i = nextIndex
		}
	}
	return i
}

func (ni *nextIndex) prevLogIndex(id uint64) uint64 {
	ni.RLock()
	defer ni.RUnlock()
	if _, ok := ni.m[id]; !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	return ni.m[id]
}

func (ni *nextIndex) decrement(id uint64, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()

	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}

	if i != prev {
		return i, errors.New("out of sync")
	}

	if i > 0 {
		ni.m[id]--
	}
	return ni.m[id], nil
}

func (ni *nextIndex) set(id, index, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()

	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	if i != prev {
		return i, errors.New("out of sync")
	}

	ni.m[id] = index
	return index, nil
}

func (s *Server) flush(peer Peer, ni *nextIndex) error {
	peerID := peer.id()
	currentTerm := s.term
	prevLogIndex := ni.prevLogIndex(peerID)
	entries, prevLogTerm := s.log.entriesAfter(prevLogIndex)
	commitIndex := s.log.getCommitIndex()
	s.logFmt("flush to %d: term=%d leaderId=%d prevLogIndex/Term=%d/%d sz=%d commitIndex=%d", peerID, currentTerm, s.id, prevLogIndex, prevLogTerm, len(entries), commitIndex)
	resp := peer.callAppendEntries(appendEntries{
		Term:         currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		CommitIndex:  commitIndex,
	})

	if resp.Term > currentTerm {
		s.logFmt("flush to %d: responseTerm=%d > currentTerm=%d: deposed", peerID, resp.Term, currentTerm)
		return errors.New("deposed during replication")
	}

	if !resp.Success {
		newPrevLogIndex, err := ni.decrement(peerID, prevLogIndex)
		if err != nil {
			s.logFmt("flush to %d: while decrementing prevLogIndex: %s", peerID, err)
			return err
		}
		s.logFmt("flush to %d: rejected; prevLogIndex(%d) becomes %d", peerID, peerID, newPrevLogIndex)
		return errors.New("appendEntries RPC rejected")
	}

	if len(entries) > 0 {
		newPrevLogIndex, err := ni.set(peer.id(), entries[len(entries)-1].Index, prevLogIndex)
		if err != nil {
			s.logFmt("flush to %d: while moving prevLogIndex forward: %s", peerID, err)
			return err
		}
		s.logFmt("flush to %d: accepted; prevLogIndex(%d) becomes %d", peerID, peerID, newPrevLogIndex)
		return nil
	}

	s.logFmt("flush to %d: accepted; prevLogIndex(%d) remains %d", peerID, peerID, ni.prevLogIndex(peerID))
	return nil
}

func (s *Server) concurrentFlush(pm peerMap, ni *nextIndex, timeout time.Duration) (int, bool) {
	type tuple struct {
		id  uint64
		err error
	}
	responses := make(chan tuple, len(pm))
	for _, peer := range pm {
		go func(peer Peer) {
			errChan := make(chan error, 1)
			go func() { errChan <- s.flush(peer, ni) }()
			go func() { time.Sleep(timeout); errChan <- errTimeout }()
			responses <- tuple{peer.id(), <-errChan} // first responder wins
		}(peer)
	}

	successes, stepDown := 0, false
	for i := 0; i < cap(responses); i++ {
		switch t := <-responses; t.err {
		case nil:
			s.logFmt("concurrentFlush: peer %d: OK (prevLogIndex(%d)=%d)", t.id, t.id, ni.prevLogIndex(t.id))
			successes++
		case errors.New("deposed during replication"):
			s.logFmt("concurrentFlush: peer %d: deposed!", t.id)
			stepDown = true
		default:
			s.logFmt("concurrentFlush: peer %d: %s (prevLogIndex(%d)=%d)", t.id, t.err, t.id, ni.prevLogIndex(t.id))
		}
	}
	return successes, stepDown
}

func (s *Server) leaderSelect() {
	if s.leader != s.id {
		panic(fmt.Sprintf("leader (%d) not me (%d) when entering leaderSelect", s.leader, s.id))
	}
	if s.vote != 0 {
		panic(fmt.Sprintf("vote (%d) not zero when entering leaderSelect", s.leader))
	}

	ni := newNextIndex(s.config.allPeers().except(s.id), s.log.lastIndex()) // +1)

	flush := make(chan struct{})
	heartbeat := time.NewTicker(broadcastInterval())
	defer heartbeat.Stop()
	go func() {
		for _ = range heartbeat.C {
			flush <- struct{}{}
		}
	}()

	for {
		select {
		case q := <-s.quit:
			s.handleQuit(q)
			return

		case t := <-s.commandChan:
			s.logFmt("got command, appending")
			currentTerm := s.term
			entry := logEntry{
				Index:           s.log.lastIndex() + 1,
				Term:            currentTerm,
				Command:         t.Command,
				commandResponse: t.CommandResponse,
			}
			if err := s.log.appendEntry(entry); err != nil {
				t.Err <- err
				continue
			}
			s.logFmt(
				"after append, commitIndex=%d lastIndex=%d lastTerm=%d",
				s.log.getCommitIndex(),
				s.log.lastIndex(),
				s.log.lastTerm(),
			)

			go func() { flush <- struct{}{} }()
			t.Err <- nil

		case t := <-s.configurationChan:
			if err := s.config.changeTo(makePeerMap(t.Peers...)); err != nil {
				t.Err <- err
				continue
			}

			encodedConfiguration, err := s.config.encode()
			if err != nil {
				t.Err <- err
				continue
			}

			entry := logEntry{
				Index:           s.log.lastIndex() + 1,
				Term:            s.term,
				Command:         encodedConfiguration,
				isConfiguration: true,
				committed:       make(chan bool),
			}
			go func() {
				committed := <-entry.committed
				if !committed {
					s.config.changeAborted()
					return
				}
				s.config.changeCommitted()
				if _, ok := s.config.allPeers()[s.id]; !ok {
					s.logFmt("leader expelled; shutting down")
					q := make(chan struct{})
					s.quit <- q
					<-q
				}
			}()
			if err := s.log.appendEntry(entry); err != nil {
				t.Err <- err
				continue
			}

		case <-flush:
			recipients := s.config.allPeers().except(s.id)

			if len(recipients) <= 0 {
				ourLastIndex := s.log.lastIndex()
				if ourLastIndex > 0 {
					if err := s.log.commitTo(ourLastIndex); err != nil {
						s.logFmt("commitTo(%d): %s", ourLastIndex, err)
						continue
					}
					s.logFmt("after commitTo(%d), commitIndex=%d", ourLastIndex, s.log.getCommitIndex())
				}
				continue
			}

			successes, stepDown := s.concurrentFlush(recipients, ni, 2*broadcastInterval())
			if stepDown {
				s.logFmt("deposed during flush")
				s.state.Set(follower)
				s.leader = unknownLeader
				return
			}

			if successes == len(recipients) {
				peersBestIndex := ni.bestIndex()
				ourLastIndex := s.log.lastIndex()
				ourCommitIndex := s.log.getCommitIndex()
				if peersBestIndex > ourLastIndex {

					s.logFmt("peers' best index %d > our lastIndex %d", peersBestIndex, ourLastIndex)
					s.logFmt("this is crazy, I'm gonna become a follower")
					s.leader = unknownLeader
					s.vote = noVote
					s.state.Set(follower)
					return
				}
				if peersBestIndex > ourCommitIndex {
					if err := s.log.commitTo(peersBestIndex); err != nil {
						s.logFmt("commitTo(%d): %s", peersBestIndex, err)
						continue // oh well, next time?
					}
					if s.log.getCommitIndex() > ourCommitIndex {
						s.logFmt("after commitTo(%d), commitIndex=%d -- queueing another flush", peersBestIndex, s.log.getCommitIndex())
						go func() { flush <- struct{}{} }()
					}
				}
			}

		case t := <-s.appendEntriesChan:
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResp(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logFmt("after an appendEntries, deposed to Follower (leader=%d)", t.Request.LeaderID)
				s.leader = t.Request.LeaderID
				s.state.Set(follower)
				return
			}

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResp(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logFmt("after a requestVote, deposed to Follower (leader unknown)")
				s.leader = unknownLeader
				s.state.Set(follower)
				return
			}
		}
	}
}

func (s *Server) handleAppendEntries(r appendEntries) (appendEntriesResponse, bool) {
	if r.Term < s.term {
		return appendEntriesResponse{
			Term:    s.term,
			Success: false,
			reason:  fmt.Sprintf("Term %d < %d", r.Term, s.term),
		}, false
	}

	stepDown := false
	if r.Term > s.term {
		s.term = r.Term
		s.vote = noVote
		stepDown = true
	}

	if s.state.Get() == candidate && r.LeaderID != s.leader && r.Term >= s.term {
		s.term = r.Term
		s.vote = noVote
		stepDown = true
	}

	s.resetElectionTimeout()

	if err := s.log.ensureLastIs(r.PrevLogIndex, r.PrevLogTerm); err != nil {
		return appendEntriesResponse{
			Term:    s.term,
			Success: false,
			reason: fmt.Sprintf(
				"while ensuring last log entry had index=%d term=%d: error: %s",
				r.PrevLogIndex,
				r.PrevLogTerm,
				err,
			),
		}, stepDown
	}

	for i, entry := range r.Entries {
		var pm peerMap
		if entry.isConfiguration {
			commandBuf := bytes.NewBuffer(entry.Command)
			if err := gob.NewDecoder(commandBuf).Decode(&pm); err != nil {
				panic("gob decode of peers failed")
			}

			if s.state.Get() == leader {
				return appendEntriesResponse{
					Term:    s.term,
					Success: false,
					reason: fmt.Sprintf(
						"AppendEntry %d/%d failed (configuration): %s",
						i+1,
						len(r.Entries),
						"Leader shouldn't receive configurations via appendEntries",
					),
				}, stepDown
			}

			if _, ok := pm[s.id]; !ok {
				entry.committed = make(chan bool)
				go func() {
					if <-entry.committed {
						s.logFmt("non-leader expelled; shutting down")
						q := make(chan struct{})
						s.quit <- q
						<-q
					}
				}()
			}
		}

		if err := s.log.appendEntry(entry); err != nil {
			return appendEntriesResponse{
				Term:    s.term,
				Success: false,
				reason: fmt.Sprintf(
					"AppendEntry %d/%d failed: %s",
					i+1,
					len(r.Entries),
					err,
				),
			}, stepDown
		}

		if entry.isConfiguration {
			if err := s.config.directSet(pm); err != nil {
				return appendEntriesResponse{
					Term:    s.term,
					Success: false,
					reason: fmt.Sprintf(
						"AppendEntry %d/%d failed (configuration): %s",
						i+1,
						len(r.Entries),
						err,
					),
				}, stepDown
			}
		}
	}

	if r.CommitIndex > 0 && r.CommitIndex > s.log.getCommitIndex() {
		if err := s.log.commitTo(r.CommitIndex); err != nil {
			return appendEntriesResponse{
				Term:    s.term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.CommitIndex, err),
			}, stepDown
		}
	}

	return appendEntriesResponse{
		Term:    s.term,
		Success: true,
	}, stepDown
}

func (s *Server) handleRequestVote(rv requestVote) (requestVoteResponse, bool) {
	if rv.Term < s.term {
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      fmt.Sprintf("Term %d < %d", rv.Term, s.term),
		}, false
	}

	stepDown := false
	if rv.Term > s.term {
		s.logFmt("requestVote from newer term (%d): we defer", rv.Term)
		s.term = rv.Term
		s.vote = noVote
		s.leader = unknownLeader
		stepDown = true
	}

	if s.state.Get() == leader && !stepDown {
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      "already the leader",
		}, stepDown
	}

	if s.vote != 0 && s.vote != rv.CandidateID {
		if stepDown {
			panic("impossible state in handleRequestVote")
		}
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      fmt.Sprintf("already cast vote for %d", s.vote),
		}, stepDown
	}

	if s.log.lastIndex() > rv.LastLogIndex || s.log.lastTerm() > rv.LastLogTerm {
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason: fmt.Sprintf(
				"our index/term %d/%d > %d/%d",
				s.log.lastIndex(),
				s.log.lastTerm(),
				rv.LastLogIndex,
				rv.LastLogTerm,
			),
		}, stepDown
	}

	s.vote = rv.CandidateID
	s.resetElectionTimeout()
	return requestVoteResponse{
		Term:        s.term,
		VoteGranted: true,
	}, stepDown
}
