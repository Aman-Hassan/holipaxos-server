HoliPaxos protocol implementation in Go with grpc communication. (with Hints)

## Build
1. Install protocol buffer compiler, rocksdb
- Installing protocol buffer compiler (protoc)
```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"
protoc-gen-go-grpc --version
```
- Installing rocksbd (Currently disabled, will be enabled in future commits)
```
TODO
```

2. Update gRPC and Protocol Buffer libraries
```
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf@latest
go mod tidy
```

3. Compile protocol buffer
```
rmdir holipaxos/comm
mkdir -p holipaxos/comm

protoc --go_out=holipaxos/comm --go_opt=paths=source_relative \
    --go-grpc_out=holipaxos/comm --go-grpc_opt=paths=source_relative \
    ./holipaxosrpc.proto 
```
4. Build replicant
- Use the following CGO flags only if compiling with rocksdb (ignore if in-memeory store)
- NOTE: Currently the rocksdb functionailty has been disabled, will be enabled in future commits.
```
CGO_CFLAGS="-I/path/to/rocksdb/include" \
CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
go build -o bin/replicant main/main.go
```
or if prerequisites are in linker paths \
```
go build -o bin/replicant main/main.go
```

## Run
1. Run the initial replicant with leader_config file (i.e. only 1 node, self elects as leader), Optionally run with `-d` flag for debug logs (NOTE: the basic logs by authors are not very descriptive, so it's preferred to run with `-d` for better insights)
```
./bin/replicant -id 0 -c config/leader_config.json [-d]
```
2. Run other replicants with the follower_config file (NOTE: ensure that the leader node is already running before starting followers). Note: the `-j` flag should be added for "joining" state as well as `-l` flag for current leader address (in <IP>:<PORT> format)
```
./bin/replicant -id 1 -c config/follower_config.json -j -l <LEADER-IP>:<LEADER-PORT> [-d]
./bin/replicant -id 2 -c config/follower_config.json -j -l <LEADER-IP>:<LEADER-PORT> [-d]
```
3. Current follower_config files support for 3 nodes, can be modified for more nodes as needed.
4. Refer the (holipaxos-client)[] repository for running ycsb client against the running replicants.

## Pending
- Fix rocksdb integration
- Testing docker setup