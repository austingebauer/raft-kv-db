package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/austingebauer/raft-kv-db/command"
	"github.com/austingebauer/raft-kv-db/raft"
	"github.com/austingebauer/raft-kv-db/storage"
)

var id uint64
var apiAddr string
var raftAddr string
var storageFile string
var peerList string

func init() {
	flag.Uint64Var(&id, "id", 1, "ID to assign to this peer")
	flag.StringVar(&apiAddr, "api-addr", "127.0.0.1:8501", "Listen address for key/value API requests")
	flag.StringVar(&raftAddr, "raft-addr", "127.0.0.1:8601", "Listen address for Raft peer connections")
	flag.StringVar(&storageFile, "storage-file", "", "File to store the Raft log in. Will be created if it doesn't exist")
	flag.StringVar(&peerList, "peer-list", "1=http://127.0.0.1:8601,2=http://127.0.0.1:8602", "List of raft peers in the form <peer_1_id>=<peer_1_url>,<peer_2_id>=<peer_2_url>")
}

func main() {
	// Parse and print command line arguments
	flag.Parse()
	log.Printf("id: %d", id)
	log.Printf("api-addr: %s", apiAddr)
	log.Printf("raft-addr: %s", raftAddr)
	log.Printf("storage-file: %s", storageFile)
	log.Printf("peer-list: %s", peerList)

	// Parse the raft address into a URL
	raftURL, err := url.Parse("http://" + raftAddr)
	if err != nil {
		log.Fatal(err)
	}

	// Parse the peer list
	parsedPeerMap, err := parsePeerList(peerList)
	if err != nil {
		log.Fatal(err)
	}
	parsedPeerMap[id] = raftURL

	// Check if raft log storage file exists and create it if not
	var store io.ReadWriter = &bytes.Buffer{}
	if storageFile != "" {
		e, err := exists(storageFile)
		if err != nil {
			log.Fatal(err)
		}
		if !e {
			store, err = os.Create(storageFile)
		} else {
			store, err = os.Open(storageFile)
		}
	}

	// Create an in-memory DB for the key/value FSM
	fsm := storage.NewMemDB()
	cb := func(_ uint64, cmd []byte) []byte {
		cmdParsed, err := command.Deserialize(cmd)
		if err != nil {
			log.Fatal(err)
		}

		switch cmdParsed.Action {
		case command.Put:
			fsm.Put(cmdParsed.Key, string(cmdParsed.Value))
		case command.Delete:
			fsm.Delete(cmdParsed.Key)
		default:
			log.Fatal("unknown command action")
		}
		return nil
	}

	// Start the raft server
	server := raft.NewServer(id, store, cb)
	raft.SetupServeMux(http.DefaultServeMux, server)

	// Set the list of raft peers
	peers := make([]raft.Peer, 0, len(parsedPeerMap))
	for id, peerURL := range parsedPeerMap {
		peers = append(peers, &raft.HTTPPeer{
			RemoteID: id,
			URL:      peerURL,
		})
	}
	if err := server.SetConfiguration(peers...); err != nil {
		log.Fatal(err)
	}

	// Start the raft server
	go http.ListenAndServe(raftAddr, nil)
	server.Start()

	// Create the key/value DB API handler
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/kvdb/") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Bad Request\n"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			kvPath := strings.Split(r.URL.Path, "/kvdb/")[1]
			w.Header().Set("Content-Type", "application/json")
			v, ok := fsm.Get(kvPath)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("Not Found\n"))
				return
			}

			w.Write([]byte(fmt.Sprintf("%s\n", v)))

		case http.MethodDelete:
			kvPath := strings.Split(r.URL.Path, "/kvdb/")[1]
			serialized, err := command.Serialize(&command.Command{
				Action: command.Delete,
				Key:    kvPath,
			})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Internal Server Error\n"))
				return
			}

			if err := server.Command(serialized, nil); err != nil {
				log.Printf("error submitting command: %v", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Internal Server Error\n"))
				return
			}

			w.WriteHeader(http.StatusNoContent)
			w.Write([]byte("\n"))

		case http.MethodPut:
			kvPath := strings.Split(r.URL.Path, "/kvdb/")[1]
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Internal Server Error\n"))
				return
			}
			serialized, err := command.Serialize(&command.Command{
				Action: command.Put,
				Key:    kvPath,
				Value:  body,
			})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Internal Server Error\n"))
				return
			}

			if err := server.Command(serialized, nil); err != nil {
				log.Printf("error submitting command: %v", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Internal Server Error\n"))
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("\n"))

		default:
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Unsupported request method\n"))
		}
	})

	// Serve up the key/value DB API
	log.Fatal(http.ListenAndServe(apiAddr, nil))
}

// Parses a peer list, which has the form:
//   <peer_1_id>=<peer_1_url>,<peer_2_id>=<peer_2_url>
//
// For example:
//   1=10.0.1.11:2380,2=10.0.1.12:2380
//
// Returns a map from peer ID to peer URL.
func parsePeerList(l string) (map[uint64]*url.URL, error) {
	peerMap := make(map[uint64]*url.URL)
	p1 := strings.Split(l, ",")

	for _, p := range p1 {
		p2 := strings.Split(p, "=")
		if len(p2) != 2 {
			return nil, errors.New("invalid peer list format")
		}
		id, err := strconv.Atoi(p2[0])
		if err != nil {
			return nil, errors.New("invalid peer list format")
		}
		u, err := url.Parse(p2[1])
		if err != nil {
			return nil, errors.New("failed to parse peer list URL")
		}
		peerMap[uint64(id)] = u
	}

	return peerMap, nil
}

// Returns true if the given file at path exists.
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}
