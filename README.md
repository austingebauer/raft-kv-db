# raft-kv-db

A distributed key-value store that uses the [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)
for data replication.

## Build

Prerequisites:
- [Go](https://go.dev/) 1.15.0+
- Make

Running the following command to build a binary for your platform. The `raft-kv-db` binary will 
be placed in the `bin` directory.

```
make build
```

## Configuration

### CLI Usage

```
$ raft-kv-db -h
    Usage of ./bin/raft-kv-db:
      -api-addr string
            Listen address for key/value API requests (default "127.0.0.1:8501")
      -id uint
            ID to assign to this peer (default 1)
      -peer-list string
            List of raft peers in the form <peer_1_id>=<peer_1_url>,<peer_2_id>=<peer_2_url> (default "1=http://127.0.0.1:8601,2=http://127.0.0.1:8602")
      -raft-addr string
            Listen address for Raft peer connections (default "127.0.0.1:8601")
      -storage-file string
            File to store the Raft log in. Will be created if it doesn't exist.
```

### Example

```
./bin/raft-kv-db \
        -id 1 \
        -api-addr "127.0.0.1:8501" \
        -raft-addr "127.0.0.1:8601" \
        -peer-list "2=http://127.0.0.1:8602,3=http://127.0.0.1:8603"

./bin/raft-kv-db \
        -id 2 \
        -api-addr "127.0.0.1:8502" \
        -raft-addr "127.0.0.1:8602" \
        -peer-list "1=http://127.0.0.1:8601,3=http://127.0.0.1:8603"

./bin/raft-kv-db \
        -id 3 \
        -api-addr "127.0.0.1:8503" \
        -raft-addr "127.0.0.1:8603" \
        -peer-list "1=http://127.0.0.1:8601,2=http://127.0.0.1:8602"
```

## Usage

### Key-Value API

#### Put

- **Path**: `/kvdb/{key_name}`
- **Method**: `PUT`
- **Body**: `JSON string`

#### Get

- **Path**: `/kvdb/{key_name}`
- **Method**: `GET`

#### Delete

- **Path**: `/kvdb/{key_name}`
- **Method**: `DELETE`

### Example

```
# Put k/v pairs into the distributed store
$ curl-X PUT -d '{"name": "uw"}' http://127.0.0.1:8501/kvdb/university
$ curl-X PUT -d '{"course": "cse550"}' http://127.0.0.1:8501/kvdb/cse

# Get k/v pairs from each peer
$ curlhttp://127.0.0.1:8501/kvdb/university
$ curlhttp://127.0.0.1:8502/kvdb/university
$ curlhttp://127.0.0.1:8503/kvdb/university
$ curlhttp://127.0.0.1:8501/kvdb/cse
$ curlhttp://127.0.0.1:8502/kvdb/cse
$ curlhttp://127.0.0.1:8503/kvdb/cse

# Delete k/v pairs from peers
$ curl -X DELETE http://127.0.0.1:8502/kvdb/cse
$ curl -X DELETE http://127.0.0.1:8501/kvdb/university

# Get k/v pairs from each peer to show deleted
$ curl http://127.0.0.1:8501/kvdb/university
$ curl http://127.0.0.1:8502/kvdb/university
$ curl http://127.0.0.1:8503/kvdb/university
$ curl -s http://127.0.0.1:8501/kvdb/cse
$ curl -s http://127.0.0.1:8502/kvdb/cse
$ curl -s http://127.0.0.1:8503/kvdb/cse
```
