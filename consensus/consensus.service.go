package consensus

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"rdm/service"
	"strconv"
	"sync"
	"time"
)

type ConsensusService struct {
	mu sync.Mutex
	LocalStoreDir string
	Address string
	HttpAddress string
	raft *raft.Raft
	servers map[string]service.ServerInfo
	// transaction logs
	logs []string
	// wallet balance
	balance int
}

type Command struct {
	Op string `json:"op,omitempty"`
	OpCode string `json:"op_code,omitempty"`
	Value int `json:"value"`
	AdditionalInfo interface{} `json:"additional_info"`
}

const (
	retainSnapshotCount = 2
	raftCmdTimeout = 10 * time.Second
)

func New(balance int) *ConsensusService {
	return &ConsensusService{
		logs: make([]string, 0, 10),
		balance: balance,
		servers: make(map[string]service.ServerInfo),
	}
}

func (s *ConsensusService) Start(id string, initNewCluster bool) error {
	log.Printf("Starting Consensus service")
	// default config
	config := &raft.Config{
		ProtocolVersion: raft.ProtocolVersionMax,
		HeartbeatTimeout: 1000 * time.Millisecond,
		ElectionTimeout: 1000 * time.Millisecond,
		CommitTimeout: 50 * time.Millisecond,
		MaxAppendEntries: 64,
		ShutdownOnRemove: true,
		TrailingLogs: 10240,
		SnapshotInterval: 120 * time.Second,
		SnapshotThreshold: 8192,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		LogLevel: "DEBUG",
		LocalID: raft.ServerID(id),
	}
	log.Printf("Resolving TCP address %s", s.Address)
	address, err := net.ResolveTCPAddr("tcp", s.Address)
	if err != nil {
		log.Fatalf("Failed to resolve TCP address %s", s.Address)
		return err
	}
	log.Printf("Creating TCP transport %s", s.Address)
	transport, err := raft.NewTCPTransport(s.Address, address, 10, 30 * time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("Failed to create TCP transport %s", s.Address)
		return err
	}
	log.Printf("Creating snapshot store %s with retain = %d", s.LocalStoreDir, retainSnapshotCount)
	snapshots, err := raft.NewFileSnapshotStore(s.LocalStoreDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		log.Fatalf("Failed to create snapshot store %s", s.LocalStoreDir)
		return err
	}
	log.Printf("Creating log store & stable store")
	// stores
	//logStore := raft.NewInmemStore()
	//stableStore := raft.NewInmemStore()
	logStoreDB, err := raftboltdb.NewBoltStore(filepath.Join(s.LocalStoreDir, "logStore.dat"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	stableStoreDB, err := raftboltdb.NewBoltStore(filepath.Join(s.LocalStoreDir, "stableStore.dat"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	logStore := logStoreDB
	stableStore := stableStoreDB
	//
	log.Printf("Creating Raft instance")
	raftService, err := raft.NewRaft(config, s, logStore, stableStore, snapshots, transport)
	if err != nil {
		log.Fatalf("Failed to create Raft instance")
		return err
	}
	s.raft = raftService
	if initNewCluster {
		log.Printf("Bootstraping new cluster")
		clusterConfig := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:       config.LocalID,
					Address:  transport.LocalAddr(),
				},
			},
		}
		raftService.BootstrapCluster(clusterConfig)
		for s.raft.State() != raft.Leader {
			time.Sleep(200 * time.Millisecond)
		}
		sv := service.ServerInfo{
			HttpAddr:      s.HttpAddress,
			ConsensusAddr: s.Address,
			Id:            id,
		}
		s.servers[s.Address] = sv
		cmd := &Command{
			Op:     "join",
			OpCode: s.Address,
			Value:  0,
			AdditionalInfo: sv,
		}
		bo, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		f := s.raft.Apply(bo, raftCmdTimeout)
		if err := f.Error(); err != nil {
			return err
		}
	}
	log.Printf("Consensus service started")
	return nil
}

// service.Consensus interface method
func (s *ConsensusService) Append(op string, value int) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	cmd := &Command{
		Op:     "append",
		OpCode: op,
		Value:  value,
		AdditionalInfo: nil,
	}
	bo, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(bo, raftCmdTimeout)
	return f.Error()
}
// service.Consensus interface method
func (s *ConsensusService) Leader() (service.ServerInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	leader := s.raft.Leader()
	server, ok := s.servers[string(leader)]
	if !ok {
		return service.ServerInfo{}, fmt.Errorf("could not get leader")
	}
	return server, nil
}

// service.Consensus interface method
func (s *ConsensusService) Get() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.balance, nil
}

// service.Consensus interface method
func (s *ConsensusService) Join(id string, httpAddr string, consensusAddr string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	log.Printf("Join request received from %s with id = %s and httpAddr = %s", consensusAddr, id, httpAddr)
	configurationF := s.raft.GetConfiguration()
	err := configurationF.Error()
	if err != nil {
		return err
	}
	for _, server := range configurationF.Configuration().Servers {
		// if server existed under different id or same id with different address, which need to remove
		if server.ID == raft.ServerID(id) || server.Address == raft.ServerAddress(consensusAddr) {
			if server.ID == raft.ServerID(id) && server.Address == raft.ServerAddress(consensusAddr) {
				log.Printf("Server %s(%s) is already a member of this cluster, ignoring joining request", id, consensusAddr)
				return nil
			}
			return fmt.Errorf("node id or address already binded")
		}
	}
	f := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(consensusAddr), 0, 0)
	err = f.Error()
	if err != nil {
		return err
	}
	log.Printf("Server %s(%s) joined cluster", id, consensusAddr)
	sv := service.ServerInfo{
		HttpAddr:      httpAddr,
		ConsensusAddr: consensusAddr,
		Id:            id,
	}
	cmd := &Command{
		Op:     "join",
		OpCode: consensusAddr,
		Value:  0,
		AdditionalInfo: sv,
	}
	bo, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f = s.raft.Apply(bo, raftCmdTimeout)
	return f.Error()
}

// raft.FSM interface method
func (s *ConsensusService) Apply(log *raft.Log) interface{} {
	var c Command
	err := json.Unmarshal(log.Data, &c)
	var info service.ServerInfo
	bo, err := json.Marshal(c.AdditionalInfo)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal data: %s", err.Error()))
	}
	err = json.Unmarshal(bo, &info)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal data: %s", err.Error()))
	}
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}
	switch c.Op {
	case "append":
		return s.ApplyAppend(c.OpCode, c.Value)
	case "join":
		return s.ApplyJoin(c.OpCode, info)
	default:
		panic(fmt.Sprintf("unrecognized command: %s", c.Op))
	}
	return nil
}

func (s *ConsensusService) ApplyJoin(consensusAddr string, info interface{}) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := info.(service.ServerInfo)
	if !ok {
		panic(fmt.Sprintf("could not parse server info: %s", info))
	}
	s.servers[consensusAddr] = c
	return nil
}

func (s *ConsensusService) ApplyAppend(op string, value int) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs, op + " " + strconv.Itoa(value))
	if value <= 0 {
		log.Printf("Value could not less or equal than 0: %d", value)
		return nil
	}
	switch op {
	case "DEPOSIT":
		s.balance += value
	case "WITHDRAW":
		s.balance -= value
	default:
		log.Printf("Unrecognized op code: %s", op)
	}
	return nil
}

// raft.FSM interface method
func (s *ConsensusService) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// copy
	cloneLogs := make([]string, len(s.logs))
	cloneLogs = append(s.logs[:0:0], s.logs...)
	cloneBalance := s.balance
	return &FSMSS{
		Logs:    cloneLogs,
		Balance: cloneBalance,
	}, nil
}

// raft.FSM interface method
func (s *ConsensusService) Restore(io io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(io, &hd)
	var ofsmss FSMSS
	err := dec.Decode(&ofsmss)
	if err != nil {
		return err
	}
	s.logs = ofsmss.Logs
	s.balance = ofsmss.Balance
	return nil
}

type FSMSS struct {
	Logs []string
	Balance int
}

func (s *FSMSS) Persist(sink raft.SnapshotSink) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	err := enc.Encode(s)
	if err != nil {
		sink.Cancel()
		return err
	}
	err = sink.Close()
	return nil
}

func (s *FSMSS) Release() {
	// NOOP
}