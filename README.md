## Introduction
A distributed wallet with two operation `DEPOSIT` and `WITHDRAW`
## Prerequisites
Go 1.13+
## Quickstarts
### Build
```shell script
go build main.go
```
### Bootstrap new cluster
Start first node (create a new cluster)
```shell script
./main -http 127.0.0.1:10000 -consensus 127.0.0.1:20000 -id NODE1 -bootstrap ./NODE1
```

Wait for first node started then start second & third nodes
```shell script
./main -http 127.0.0.1:10001 -consensus 127.0.0.1:20001 -id NODE2 -join 127.0.0.1:10000 ./NODE2
./main -http 127.0.0.1:10002 -consensus 127.0.0.1:20002 -id NODE3 -join 127.0.0.1:10001 ./NODE3
```
### Live bootstrap
Create a new node
```shell script
./main -http 127.0.0.1:10003 -consensus 127.0.0.1:20003 -id NODE4 ./NODE4
```
Bootstrap the newly created node into existing cluster
```shell script
curl -i -X POST 127.0.0.1:10001/join -H 'Content-Type:application/json' -d '{"httpAddr":"127.0.0.1:10003", "consensusAddr": "127.0.0.1:20003", "id": "NODE4"}'
```
### Test
Check the balance 
```shell script
curl -X GET 127.0.0.1:10000
curl -X GET 127.0.0.1:10001
curl -X GET 127.0.0.1:10002
```
```shell script
5000
5000
5000
```
deposit money into wallet
```shell script
curl -i -X POST 127.0.0.1:10002/dw -H 'Content-Type:application/json' -d '{"op":"DEPOSIT", "amount": "525"}'
```
Then the balance is change on all nodes too
```shell script
curl -X GET 127.0.0.1:10000
curl -X GET 127.0.0.1:10001
curl -X GET 127.0.0.1:10002
```
```shell script
5525
5525
5525
```
withdraw money from wallet
```shell script
curl -i -X POST 127.0.0.1:10002/dw -H 'Content-Type:application/json' -d '{"op":"WITHDRAW", "amount": "1725"}'
```
Get new balance
```shell script
curl -X GET 127.0.0.1:10000
curl -X GET 127.0.0.1:10001
curl -X GET 127.0.0.1:10002
```
```shell script
3800
3800
3800
```
## Usage
### Build
```shell script
go build main.go
```
### Run
```shell script
./main -http $HTTP_ADDR -consensus $CONSENSUS_ADDR -id $NODE_ID -bootstrap -join $OTHER_ADDR /path/to/store/dir
```
while 

`-http $HTTP_ADDR` 
declares service http endpoint (e.g 127.0.0.1:10000)

`-consensus $CONSENSUS_ADDR` 
declares consensus service endpoint (e.g 127.0.0.1:20000)

`-id $NODE_ID` 
node id (ids in the same cluster must not be duplicated)

`-bootstrap` 
to bootstrap new cluster. 
* if `bootstrap` is set then ignore `join` option.
* if `bootstrap` and `join` is not set then create a new node (it will not functional)

`-join $OTHER_ADDR` 
used to join this newly created node to an existed cluster.
(this could be http binding of any node inside cluster) 

`/path/to/store/dir` 
path to log store used to snapshot & restore if node is up
