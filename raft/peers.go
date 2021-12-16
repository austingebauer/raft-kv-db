package raft

import (
	"errors"
	"time"
)

var (
	errTimeout = errors.New("timeout")
)

type Peer interface {
	id() uint64
	callAppendEntries(appendEntries) appendEntriesResponse
	callRequestVote(requestVote) requestVoteResponse
	callCommand([]byte, chan<- []byte) error
	callSetConfiguration(...Peer) error
}

func requestVoteTimeout(p Peer, rv requestVote, timeout time.Duration) (requestVoteResponse, error) {
	c := make(chan requestVoteResponse, 1)
	go func() { c <- p.callRequestVote(rv) }()

	select {
	case resp := <-c:
		return resp, nil
	case <-time.After(timeout):
		return requestVoteResponse{}, errTimeout
	}
}

type peerMap map[uint64]Peer

func makePeerMap(peers ...Peer) peerMap {
	pm := peerMap{}
	for _, peer := range peers {
		pm[peer.id()] = peer
	}
	return pm
}

func explodePeerMap(pm peerMap) []Peer {
	a := []Peer{}
	for _, peer := range pm {
		a = append(a, peer)
	}
	return a
}

func (pm peerMap) except(id uint64) peerMap {
	except := peerMap{}
	for id0, peer := range pm {
		if id0 == id {
			continue
		}
		except[id0] = peer
	}
	return except
}

func (pm peerMap) count() int { return len(pm) }

func (pm peerMap) quorum() int {
	switch n := len(pm); n {
	case 0, 1:
		return 1
	default:
		return (n / 2) + 1
	}
}

func (pm peerMap) requestVotes(r requestVote) (chan voteResponseTuple, canceler) {
	abortChan := make(chan struct{})
	tupleChan := make(chan voteResponseTuple)

	go func() {
		respondedAlready := peerMap{}
		for {
			notYetResponded := disjoint(pm, respondedAlready)
			if len(notYetResponded) <= 0 {
				return
			}

			tupleChan0 := make(chan voteResponseTuple, len(notYetResponded))
			for id, peer := range notYetResponded {
				go func(id uint64, peer Peer) {
					resp, err := requestVoteTimeout(peer, r, 2*maximumElectionTimeout())
					tupleChan0 <- voteResponseTuple{id, resp, err}
				}(id, peer)
			}

			for i := 0; i < cap(tupleChan0); i++ {
				select {
				case t := <-tupleChan0:
					if t.err != nil {
						continue
					}
					respondedAlready[t.id] = nil
					tupleChan <- t

				case <-abortChan:
					return
				}
			}
		}
	}()

	return tupleChan, cancel(abortChan)
}

type voteResponseTuple struct {
	id       uint64
	response requestVoteResponse
	err      error
}

type canceler interface {
	Cancel()
}

type cancel chan struct{}

func (c cancel) Cancel() { close(c) }

func disjoint(all, except peerMap) peerMap {
	d := peerMap{}
	for id, peer := range all {
		if _, ok := except[id]; ok {
			continue
		}
		d[id] = peer
	}
	return d
}
