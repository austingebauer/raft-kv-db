package raft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

const (
	PeerSetConfigurationPath = "/raft/setconfiguration"
	PeerAppendEntriesPath    = "/raft/appendentries"
	PeerIDPath               = "/raft/id"
	PeerCommandPath          = "/raft/command"
	PeerRequestVotePath      = "/raft/requestvote"
)

var (
	emptyAppendEntriesResponse bytes.Buffer
	emptyRequestVoteResponse   bytes.Buffer
)

func init() {
	json.NewEncoder(&emptyAppendEntriesResponse).Encode(appendEntriesResponse{})
	json.NewEncoder(&emptyRequestVoteResponse).Encode(requestVoteResponse{})
	gob.Register(&HTTPPeer{})
}

func idHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprint(s.id)))
	}
}

func appendEntriesHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var ae appendEntries
		if err := json.NewDecoder(r.Body).Decode(&ae); err != nil {
			http.Error(w, emptyAppendEntriesResponse.String(), http.StatusBadRequest)
			return
		}

		aer := s.appendEntries(ae)
		if err := json.NewEncoder(w).Encode(aer); err != nil {
			http.Error(w, emptyAppendEntriesResponse.String(), http.StatusInternalServerError)
			return
		}
	}
}

func requestVoteHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var rv requestVote
		if err := json.NewDecoder(r.Body).Decode(&rv); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusBadRequest)
			return
		}

		rvr := s.requestVote(rv)
		if err := json.NewEncoder(w).Encode(rvr); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
			return
		}
	}
}

func commandHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		cmd, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		response := make(chan []byte, 1)
		if err := s.Command(cmd, response); err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		resp, ok := <-response
		if !ok {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		w.Write(resp)
	}
}

func setConfigurationHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var pm peerMap
		if err := gob.NewDecoder(r.Body).Decode(&pm); err != nil {
			errBuf, _ := json.Marshal(commaError{err.Error(), false})
			http.Error(w, string(errBuf), http.StatusBadRequest)
			return
		}

		if err := s.SetConfiguration(explodePeerMap(pm)...); err != nil {
			errBuf, _ := json.Marshal(commaError{err.Error(), false})
			http.Error(w, string(errBuf), http.StatusInternalServerError)
			return
		}

		respBuf, _ := json.Marshal(commaError{"", true})
		w.Write(respBuf)
	}
}

type commaError struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success,omitempty"`
}

type HTTPPeer struct {
	RemoteID uint64
	URL      *url.URL
}

func (p *HTTPPeer) id() uint64 { return p.RemoteID }

func (p *HTTPPeer) callAppendEntries(ae appendEntries) appendEntriesResponse {
	var aer appendEntriesResponse

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(ae); err != nil {
		log.Printf("Raft: HTTP Peer: AppendEntries: encode request: %s", err)
		return aer
	}

	var resp bytes.Buffer
	if err := p.rpc(&body, PeerAppendEntriesPath, &resp); err != nil {
		log.Printf("Raft: HTTP Peer: AppendEntries: during RPC: %s", err)
		return aer
	}

	if err := json.Unmarshal(resp.Bytes(), &aer); err != nil {
		log.Printf("Raft: HTTP Peer: AppendEntries: decode response: %s", err)
		return aer
	}

	return aer
}

func (p *HTTPPeer) callRequestVote(rv requestVote) requestVoteResponse {
	var rvr requestVoteResponse

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(rv); err != nil {
		log.Printf("Raft: HTTP Peer: RequestVote: encode request: %s", err)
		return rvr
	}

	var resp bytes.Buffer
	if err := p.rpc(&body, PeerRequestVotePath, &resp); err != nil {
		log.Printf("Raft: HTTP Peer: RequestVote: during RPC: %s", err)
		return rvr
	}

	if err := json.Unmarshal(resp.Bytes(), &rvr); err != nil {
		log.Printf("Raft: HTTP Peer: RequestVote: decode response: %s", err)
		return rvr
	}

	return rvr
}

func (p *HTTPPeer) callCommand(cmd []byte, response chan<- []byte) error {
	errChan := make(chan error)
	go func() {
		var responseBuf bytes.Buffer
		err := p.rpc(bytes.NewBuffer(cmd), PeerCommandPath, &responseBuf)
		errChan <- err
		if err != nil {
			return
		}
		response <- responseBuf.Bytes()
	}()
	return <-errChan // TODO timeout?
}

func (p *HTTPPeer) callSetConfiguration(peers ...Peer) error {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(&peers); err != nil {
		log.Printf("Raft: HTTP Peer: SetConfiguration: encode request: %s", err)
		return err
	}

	var resp bytes.Buffer
	if err := p.rpc(buf, PeerSetConfigurationPath, &resp); err != nil {
		log.Printf("Raft: HTTP Peer: SetConfiguration: during RPC: %s", err)
		return err
	}

	var commaErr commaError
	if err := json.Unmarshal(resp.Bytes(), &commaErr); err != nil {
		log.Printf("Raft: HTTP Peer: SetConfiguration: decode response: %s", err)
		return err
	}

	if !commaErr.Success {
		return fmt.Errorf(commaErr.Error)
	}
	return nil
}

func SetupServeMux(mux *http.ServeMux, s *Server) {
	mux.HandleFunc(PeerIDPath, idHandler(s))
	mux.HandleFunc(PeerAppendEntriesPath, appendEntriesHandler(s))
	mux.HandleFunc(PeerRequestVotePath, requestVoteHandler(s))
	mux.HandleFunc(PeerCommandPath, commandHandler(s))
	mux.HandleFunc(PeerSetConfigurationPath, setConfigurationHandler(s))
}

func (p *HTTPPeer) rpc(request *bytes.Buffer, path string, response *bytes.Buffer) error {
	url := *p.URL
	url.Path = path
	resp, err := http.Post(url.String(), "application/json", request)
	if err != nil {
		log.Printf("Raft: HTTP Peer: rpc POST: %s", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	n, err := io.Copy(response, resp.Body)
	if err != nil {
		return err
	}
	if l := response.Len(); n < int64(l) {
		return fmt.Errorf("short read (%d < %d)", n, l)
	}

	return nil
}
