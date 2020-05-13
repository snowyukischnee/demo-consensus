package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
)

type Consensus interface {
	Append(op string, value int) error
	Get() (int, error)
	Join(id string, httpAddr string, consensusAddr string) error
	Leader() (ServerInfo, error)
}

type ServerInfo struct {
	HttpAddr      string `json:"http_addr"`
	ConsensusAddr string `json:"consensus_addr"`
	Id            string `json:"id"`
}

type Service struct {
	address string
	listener net.Listener
	consensus Consensus
}

func New(address string, consensus Consensus) *Service {
	return &Service{
		address: address,
		listener: nil,
		consensus: consensus,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener
	http.Handle("/", s)
	go func() {
		err := server.Serve(s.listener)
		if err != nil {
			log.Fatalf("HTTP service error: %s", err)
		}
	}()
	return nil
}

func (s *Service) Close() error {
	err := s.listener.Close()
	if err != nil {
		return err
	}
	return nil
}

// http.Handler interface method
func (s *Service) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Printf("%s %s", req.Method, req.URL.Path)
	if req.URL.Path == "/join" {
		s.handleJoin(w, req)
	} else if req.URL.Path == "/" {
		s.handleGet(w, req)
	} else if req.URL.Path == "/dw" {
		s.handleDW(w, req)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// Handle node joining an existed cluster
func (s *Service) handleJoin(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "POST":
		m := map[string]string{}
		// copy body
		var bb bytes.Buffer
		bb.ReadFrom(req.Body)
		body := ioutil.NopCloser(bytes.NewReader(bb.Bytes()))
		err := json.NewDecoder(body).Decode(&m)
		if err != nil {
			log.Printf("error: %s", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(m) != 3 {
			log.Printf("error, no. field must equal to 3: %d", len(m))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		httpAddr, ok := m["httpAddr"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		consensusAddr, ok := m["consensusAddr"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		id, ok := m["id"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = s.consensus.Join(id, httpAddr, consensusAddr)
		if err != nil && err.Error() == "not leader" {
			leader, err1 := s.consensus.Leader()
			if err1 != nil {
				log.Printf("error: %s", err1)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			s.forwardRequest(w, req, leader.HttpAddr, ioutil.NopCloser(bytes.NewReader(bb.Bytes())))
		} else if err == nil {
			w.WriteHeader(http.StatusOK)
		} else {
			log.Printf("error: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

//Handle get balance
func (s *Service) handleGet(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		balance, err := s.consensus.Get()
		if err != nil && err.Error() == "not leader" {

		}
		if err != nil {
			log.Printf("error: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
		io.WriteString(w, strconv.Itoa(balance))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

//Handle Deposit/Withdraw
func (s *Service) handleDW(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "POST":
		m := map[string]string{}
		// copy body
		var bb bytes.Buffer
		bb.ReadFrom(req.Body)
		body := ioutil.NopCloser(bytes.NewReader(bb.Bytes()))
		err := json.NewDecoder(body).Decode(&m)
		if err != nil {
			log.Printf("error: %s", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(m) != 2 {
			log.Printf("error, no. field must equal to 2: %d", len(m))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		op, ok := m["op"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		amount, ok := m["amount"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		amountInt, err := strconv.Atoi(amount)
		if err != nil {
			log.Printf("error: %s", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch op {
		case "DEPOSIT":
			err := s.consensus.Append("DEPOSIT", amountInt)
			if err != nil && err.Error() == "not leader" {
				leader, err1 := s.consensus.Leader()
				if err1 != nil {
					log.Printf("error: %s", err1)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				s.forwardRequest(w, req, leader.HttpAddr, ioutil.NopCloser(bytes.NewReader(bb.Bytes())))
			} else if err == nil {
				w.WriteHeader(http.StatusOK)
			} else {
				log.Printf("error: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
		case "WITHDRAW":
			err := s.consensus.Append("WITHDRAW", amountInt)
			if err != nil && err.Error() == "not leader" {
				leader, err1 := s.consensus.Leader()
				if err1 != nil {
					log.Printf("error: %s", err1)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				s.forwardRequest(w, req, leader.HttpAddr, ioutil.NopCloser(bytes.NewReader(bb.Bytes())))
			} else if err == nil {
				w.WriteHeader(http.StatusOK)
			} else {
				log.Printf("error: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
		default:
			log.Printf("Unhandled operation: %s", op)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Service) forwardRequest(w http.ResponseWriter, req *http.Request, address string, reqBody io.ReadCloser) {
	body, err := ioutil.ReadAll(reqBody)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	url := fmt.Sprintf("http://%s%s", address, req.URL.Path)
	preq, err := http.NewRequest(req.Method, url, bytes.NewReader(body))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	preq.Header = make(http.Header)
	for h, val := range req.Header {
		preq.Header[h] = val
	}
	httpClient := http.Client{}
	resp, err := httpClient.Do(preq)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(respBody)
}