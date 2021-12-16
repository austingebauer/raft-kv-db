#!/usr/bin/env bash

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

# Put k/v pairs into the distributed store
curl -X PUT -d '{"name": "uw"}' http://127.0.0.1:8501/kvdb/university
curl -X PUT -d '{"course": "cse550"}' http://127.0.0.1:8501/kvdb/cse

# Get k/v pairs from each peer
curl http://127.0.0.1:8501/kvdb/university
curl http://127.0.0.1:8502/kvdb/university
curl http://127.0.0.1:8503/kvdb/university
curl http://127.0.0.1:8501/kvdb/cse
curl http://127.0.0.1:8502/kvdb/cse
curl http://127.0.0.1:8503/kvdb/cse

# Delete k/v pairs from peers
curl -X DELETE http://127.0.0.1:8502/kvdb/cse
curl -X DELETE http://127.0.0.1:8501/kvdb/university

# Get k/v pairs from each peer to show deleted
curl http://127.0.0.1:8501/kvdb/university
curl http://127.0.0.1:8502/kvdb/university
curl http://127.0.0.1:8503/kvdb/university
curl http://127.0.0.1:8501/kvdb/cse
curl http://127.0.0.1:8502/kvdb/cse
curl http://127.0.0.1:8503/kvdb/cse
