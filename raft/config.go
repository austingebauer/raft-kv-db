package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
)

var (
	errConfigChanging = errors.New("configuration already changing")
)

const (
	configOld       = "config_old"
	configOldAndNew = "config_old,new"
)

type configuration struct {
	sync.RWMutex
	state     string
	cOldPeers peerMap
	cNewPeers peerMap
}

func newConfiguration(pm peerMap) *configuration {
	return &configuration{
		state:     configOld,
		cOldPeers: pm,
	}
}

func (c *configuration) directSet(pm peerMap) error {
	c.Lock()
	defer c.Unlock()

	c.cOldPeers = pm
	c.cNewPeers = peerMap{}
	c.state = configOld
	return nil
}

func (c *configuration) get(id uint64) (Peer, bool) {
	c.RLock()
	defer c.RUnlock()

	if peer, ok := c.cOldPeers[id]; ok {
		return peer, true
	}
	if peer, ok := c.cNewPeers[id]; ok {
		return peer, true
	}
	return nil, false
}

func (c *configuration) encode() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(c.allPeers()); err != nil {
		return []byte{}, err
	}
	return buf.Bytes(), nil
}

func (c *configuration) allPeers() peerMap {
	c.RLock()
	defer c.RUnlock()

	union := peerMap{}
	for id, peer := range c.cOldPeers {
		union[id] = peer
	}
	for id, peer := range c.cNewPeers {
		union[id] = peer
	}
	return union
}

func (c *configuration) pass(votes map[uint64]bool) bool {
	c.RLock()
	defer c.RUnlock()

	cOldHave, cOldRequired := 0, c.cOldPeers.quorum()
	for id := range c.cOldPeers {
		if votes[id] {
			cOldHave++
		}
		if cOldHave >= cOldRequired {
			break
		}
	}

	if cOldHave < cOldRequired {
		return false
	}

	if c.state == configOld {
		return true
	}

	if len(c.cNewPeers) <= 0 {
		panic(fmt.Sprintf("configuration state '%s', but no config_new peers", c.state))
	}

	cNewHave, cNewRequired := 0, c.cNewPeers.quorum()
	for id := range c.cNewPeers {
		if votes[id] {
			cNewHave++
		}
		if cNewHave >= cNewRequired {
			break
		}
	}

	return cNewHave >= cNewRequired
}

func (c *configuration) changeTo(pm peerMap) error {
	c.Lock()
	defer c.Unlock()

	if c.state != configOld {
		return errConfigChanging
	}

	if len(c.cNewPeers) > 0 {
		panic(fmt.Sprintf("configuration change to in state '%s', but have config_new peers already", c.state))
	}

	c.cNewPeers = pm
	c.state = configOldAndNew
	return nil
}

func (c *configuration) changeCommitted() {
	c.Lock()
	defer c.Unlock()

	if c.state != configOldAndNew {
		panic("configuration change committed, but not in config_old,new")
	}

	if len(c.cNewPeers) <= 0 {
		panic("configuration change committed, but config_new peers are empty")
	}

	c.cOldPeers = c.cNewPeers
	c.cNewPeers = peerMap{}
	c.state = configOld
}

func (c *configuration) changeAborted() {
	c.Lock()
	defer c.Unlock()

	if c.state != configOldAndNew {
		panic("configuration change aborted, but not in config_old,new")
	}

	c.cNewPeers = peerMap{}
	c.state = configOld
}
