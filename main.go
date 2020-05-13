package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"rdm/consensus"
	"rdm/service"
)

var httpAddr string
var consensusAddr string
var bootstrapNewCluster bool
var joinAddr string
var id string

func main()  {
	// parse args
	flag.StringVar(&httpAddr, "http", ":10000", "HTTP endpoint")
	flag.StringVar(&consensusAddr, "consensus", ":10001", "Consensus endpoint")
	flag.BoolVar(&bootstrapNewCluster, "bootstrap", false, "Bootstrap new cluster")
	flag.StringVar(&joinAddr, "join", "", "Join an existing cluster")
	flag.StringVar(&id, "id", "", "Node ID")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <log-data-path>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	//
	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No log storage directory specified\n")
		os.Exit(1)
	}
	localStoreDir := flag.Arg(0)
	if localStoreDir == "" {
		fmt.Fprintf(os.Stderr, "No log storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(localStoreDir, 0750)
	//
	consensusSvc := consensus.New(5000)
	consensusSvc.LocalStoreDir = localStoreDir
	consensusSvc.Address = consensusAddr
	consensusSvc.HttpAddress = httpAddr
	err := consensusSvc.Start(id, bootstrapNewCluster)
	if err != nil {
		log.Fatalf("Fatal error: %s", err)
	}
	httpSvc := service.New(httpAddr, consensusSvc)
	err = httpSvc.Start()
	if err != nil {
		log.Fatalf("Fatal error: %s", err)
	}
	if bootstrapNewCluster == false && joinAddr != "" {
		err := func() error {
			b, err := json.Marshal(map[string]string{
				"httpAddr": httpAddr,
				"consensusAddr": consensusAddr,
				"id": id,
			})
			if err != nil {
				return err
			}
			resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			return nil
		}()
		if err != nil {
			log.Fatalf("Fatal error: %s", err)
		}
	}
	fmt.Println("STARTED")
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	// shutdown hook here

	fmt.Println("STOP")
}
