package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"time"
)

type rlog struct {
	sync.RWMutex
	store     io.Writer
	entries   []logEntry
	commitPos int
	apply     func(uint64, []byte) []byte
}

func newRaftLog(store io.ReadWriter, apply func(uint64, []byte) []byte) *rlog {
	l := &rlog{
		store:     store,
		entries:   []logEntry{},
		commitPos: -1,
		apply:     apply,
	}
	l.recover(store)
	return l
}

func (l *rlog) recover(r io.Reader) error {
	for {
		var entry logEntry
		switch err := entry.decode(r); err {
		case io.EOF:
			return nil // successful completion
		case nil:
			if err := l.appendEntry(entry); err != nil {
				return err
			}
			l.commitPos++
			l.apply(entry.Index, entry.Command)
		default:
			return err // unsuccessful completion
		}
	}
}

func (l *rlog) entriesAfter(index uint64) ([]logEntry, uint64) {
	l.RLock()
	defer l.RUnlock()

	pos := 0
	lastTerm := uint64(0)
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index > index {
			break
		}
		lastTerm = l.entries[pos].Term
	}

	a := l.entries[pos:]
	if len(a) == 0 {
		return []logEntry{}, lastTerm
	}

	return stripResponseChannels(a), lastTerm
}

func stripResponseChannels(a []logEntry) []logEntry {
	stripped := make([]logEntry, len(a))
	for i, entry := range a {
		stripped[i] = logEntry{
			Index:           entry.Index,
			Term:            entry.Term,
			Command:         entry.Command,
			commandResponse: nil,
		}
	}
	return stripped
}

func (l *rlog) contains(index, term uint64) bool {
	l.RLock()
	defer l.RUnlock()

	// It's not necessarily true that l.entries[i] has index == i.
	for _, entry := range l.entries {
		if entry.Index == index && entry.Term == term {
			return true
		}
		if entry.Index > index || entry.Term > term {
			break
		}
	}
	return false
}

func (l *rlog) ensureLastIs(index, term uint64) error {
	l.Lock()
	defer l.Unlock()

	if index < l.getCommitIndexWithLock() {
		return errors.New("index too small")
	}

	if index > l.lastIndexWithLock() {
		return errors.New("commit index too big")
	}

	if index == 0 {
		for pos := 0; pos < len(l.entries); pos++ {
			if l.entries[pos].commandResponse != nil {
				close(l.entries[pos].commandResponse)
				l.entries[pos].commandResponse = nil
			}
			if l.entries[pos].committed != nil {
				l.entries[pos].committed <- false
				close(l.entries[pos].committed)
				l.entries[pos].committed = nil
			}
		}
		l.entries = []logEntry{}
		return nil
	}

	pos := 0
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index < index {
			continue
		}
		if l.entries[pos].Index > index {
			return errors.New("bad index")
		}
		if l.entries[pos].Index != index {
			panic("not <, not >, but somehow !=")
		}
		if l.entries[pos].Term != term {
			return errors.New("bad term")
		}
		break
	}

	if pos < l.commitPos {
		panic("index >= commitIndex, but pos < commitPos")
	}

	truncateFrom := pos + 1
	if truncateFrom >= len(l.entries) {
		return nil
	}

	for pos = truncateFrom; pos < len(l.entries); pos++ {
		if l.entries[pos].commandResponse != nil {
			close(l.entries[pos].commandResponse)
			l.entries[pos].commandResponse = nil
		}
		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- false
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}
	}

	l.entries = l.entries[:truncateFrom]
	return nil
}

func (l *rlog) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.getCommitIndexWithLock()
}

func (l *rlog) getCommitIndexWithLock() uint64 {
	if l.commitPos < 0 {
		return 0
	}
	if l.commitPos >= len(l.entries) {
		panic(fmt.Sprintf("commitPos %d > len(l.entries) %d; bad bookkeeping in log", l.commitPos, len(l.entries)))
	}
	return l.entries[l.commitPos].Index
}

func (l *rlog) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndexWithLock()
}

func (l *rlog) lastIndexWithLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *rlog) lastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastTermWithLock()
}

func (l *rlog) lastTermWithLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *rlog) appendEntry(entry logEntry) error {
	l.Lock()
	defer l.Unlock()

	if len(l.entries) > 0 {
		lastTerm := l.lastTermWithLock()
		if entry.Term < lastTerm {
			return errors.New("term too small")
		}
		lastIndex := l.lastIndexWithLock()
		if entry.Term == lastTerm && entry.Index <= lastIndex {
			return errors.New("index too small")
		}
	}

	l.entries = append(l.entries, entry)
	return nil
}

func (l *rlog) commitTo(commitIndex uint64) error {
	if commitIndex == 0 {
		panic("commitTo(0)")
	}

	l.Lock()
	defer l.Unlock()

	if commitIndex < l.getCommitIndexWithLock() {
		return errors.New("index too small")
	}

	if commitIndex > l.lastIndexWithLock() {
		return errors.New("commit index too big")
	}

	if commitIndex == l.getCommitIndexWithLock() {
		return nil
	}

	pos := l.commitPos + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}

	for {
		if pos >= len(l.entries) {
			panic(fmt.Sprintf("commitTo pos=%d advanced past all log entries (%d)", pos, len(l.entries)))
		}
		if l.entries[pos].Index > commitIndex {
			panic("commitTo advanced past the desired commitIndex")
		}

		if err := l.entries[pos].encode(l.store); err != nil {
			return err
		}

		if !l.entries[pos].isConfiguration {
			resp := l.apply(l.entries[pos].Index, l.entries[pos].Command)
			if l.entries[pos].commandResponse != nil {
				select {
				case l.entries[pos].commandResponse <- resp:
					break
				case <-time.After(maximumElectionTimeout()):
					panic("uncoöperative command response receiver")
				}
				close(l.entries[pos].commandResponse)
				l.entries[pos].commandResponse = nil
			}
		}

		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- true
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}

		l.commitPos = pos
		if l.entries[pos].Index == commitIndex {
			break
		}
		if l.entries[pos].Index > commitIndex {
			panic(fmt.Sprintf(
				"current entry Index %d is beyond our desired commitIndex %d",
				l.entries[pos].Index,
				commitIndex,
			))
		}

		pos++
	}

	return nil
}

type logEntry struct {
	Index           uint64        `json:"index"`
	Term            uint64        `json:"term"`
	Command         []byte        `json:"command,omitempty"`
	committed       chan bool     `json:"-"`
	commandResponse chan<- []byte `json:"-"`
	isConfiguration bool          `json:"-"`
}

func (e *logEntry) encode(w io.Writer) error {
	if len(e.Command) <= 0 {
		return errors.New("no command")
	}
	if e.Index <= 0 {
		return errors.New("bad index")
	}
	if e.Term <= 0 {
		return errors.New("bad term")
	}

	commandSize := len(e.Command)
	buf := make([]byte, 24+commandSize)

	binary.LittleEndian.PutUint64(buf[4:12], e.Term)
	binary.LittleEndian.PutUint64(buf[12:20], e.Index)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(commandSize))

	copy(buf[24:], e.Command)

	binary.LittleEndian.PutUint32(
		buf[0:4],
		crc32.ChecksumIEEE(buf[4:]),
	)

	_, err := w.Write(buf)
	return err
}

func (e *logEntry) decode(r io.Reader) error {
	header := make([]byte, 24)

	if _, err := r.Read(header); err != nil {
		return err
	}

	command := make([]byte, binary.LittleEndian.Uint32(header[20:24]))

	if _, err := r.Read(command); err != nil {
		return err
	}

	crc := binary.LittleEndian.Uint32(header[:4])

	check := crc32.NewIEEE()
	check.Write(header[4:])
	check.Write(command)

	if crc != check.Sum32() {
		return errors.New("invalid checksum")
	}

	e.Term = binary.LittleEndian.Uint64(header[4:12])
	e.Index = binary.LittleEndian.Uint64(header[12:20])
	e.Command = command

	return nil
}
