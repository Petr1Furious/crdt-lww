package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ReplicaID string

type Timestamp struct {
	Lamport   uint64    `json:"lamport"`
	ReplicaID ReplicaID `json:"replica_id"`
}

type ValueWithTimestamp struct {
	Value     interface{} `json:"value"`
	Timestamp Timestamp   `json:"timestamp"`
}

type Operation struct {
	ReplicaID ReplicaID                     `json:"replica_id"`
	Lamport   uint64                        `json:"lamport"`
	Updates   map[string]ValueWithTimestamp `json:"updates"`
}

type LWWMap struct {
	mu        sync.Mutex
	data      map[string]ValueWithTimestamp
	replicaID ReplicaID
	peers     []string

	versionVector map[ReplicaID]uint64

	operationLog []Operation

	localLamport uint64
}

func NewLWWMap(id ReplicaID, peers []string) *LWWMap {
	return &LWWMap{
		data:          make(map[string]ValueWithTimestamp),
		replicaID:     id,
		peers:         peers,
		versionVector: make(map[ReplicaID]uint64),
	}
}

func (m *LWWMap) incrementLamport() uint64 {
	m.localLamport++
	return m.localLamport
}

func compareTimestamps(a, b Timestamp) int {
	if a.Lamport > b.Lamport {
		return 1
	} else if a.Lamport < b.Lamport {
		return -1
	}
	if a.ReplicaID > b.ReplicaID {
		return 1
	} else if a.ReplicaID < b.ReplicaID {
		return -1
	}
	return 0
}

func (m *LWWMap) applyLocalUpdate(updates map[string]interface{}) {
	m.mu.Lock()
	t := m.incrementLamport()
	opUpdates := make(map[string]ValueWithTimestamp)
	for k, v := range updates {
		newVal := ValueWithTimestamp{
			Value: v,
			Timestamp: Timestamp{
				Lamport:   t,
				ReplicaID: m.replicaID,
			},
		}
		log.Printf("Local update: %s -> %+v", k, newVal)
		m.data[k] = newVal
		opUpdates[k] = newVal
	}
	op := Operation{
		ReplicaID: m.replicaID,
		Lamport:   t,
		Updates:   opUpdates,
	}
	m.recordOperation(op)
	m.mu.Unlock()

	m.broadcastOperation(op)
}

func (m *LWWMap) recordOperation(op Operation) {
	knownLamport, ok := m.versionVector[op.ReplicaID]
	if !ok || op.Lamport > knownLamport {
		m.versionVector[op.ReplicaID] = op.Lamport
	}
	m.operationLog = append(m.operationLog, op)
}

func (m *LWWMap) applyOperation(op Operation) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if op.Lamport > m.localLamport {
		m.localLamport = op.Lamport
	}

	for k, incoming := range op.Updates {
		current, exists := m.data[k]
		if !exists || compareTimestamps(incoming.Timestamp, current.Timestamp) > 0 {
			log.Printf("Remote update: %s -> %+v", k, incoming)
			m.data[k] = incoming
		}
	}
	m.recordOperation(op)
}

func (m *LWWMap) broadcastOperation(op Operation) {
	buf, err := json.Marshal(op)
	if err != nil {
		log.Fatal("Failed to marshal operation:", err)
	}
	for _, peer := range m.peers {
		peerURL := "http://" + peer + "/replicate"
		go func(url string) {
			resp, err := http.Post(url, "application/json", bytes.NewReader(buf))
			if err != nil {
				log.Println("Failed to send operation to", url, ":", err)
				return
			}
			resp.Body.Close()
		}(peerURL)
	}
}

func (m *LWWMap) handlePatch(w http.ResponseWriter, r *http.Request) {
	var updates map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	m.applyLocalUpdate(updates)
	w.WriteHeader(http.StatusOK)
}

func (m *LWWMap) handleReplicate(w http.ResponseWriter, r *http.Request) {
	var op Operation
	if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	m.applyOperation(op)
	w.WriteHeader(http.StatusOK)
}

func (m *LWWMap) handleGet(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	currentState := make(map[string]interface{})
	for k, v := range m.data {
		currentState[k] = v.Value
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(currentState)
}

type SyncRequest struct {
	VersionVector map[ReplicaID]uint64 `json:"version_vector"`
}

type SyncResponse struct {
	MissedOperations []Operation `json:"missed_operations"`
}

func (m *LWWMap) handleSync(w http.ResponseWriter, r *http.Request) {
	var req SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var resp SyncResponse
	for _, op := range m.operationLog {
		if op.Lamport > req.VersionVector[op.ReplicaID] {
			resp.MissedOperations = append(resp.MissedOperations, op)
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

var syncInterval = 5 * time.Second

func (m *LWWMap) syncFromPeers() {
	m.mu.Lock()
	localVV := make(map[ReplicaID]uint64, len(m.versionVector))
	for rid, lam := range m.versionVector {
		localVV[rid] = lam
	}
	m.mu.Unlock()

	reqBody := SyncRequest{VersionVector: localVV}

	buf, _ := json.Marshal(reqBody)

	client := &http.Client{
		Timeout: syncInterval - 1,
	}
	for _, peer := range m.peers {
		go func() {
			resp, err := client.Post("http://"+peer+"/sync", "application/json", bytes.NewReader(buf))
			if err != nil {
				log.Println("Sync request failed to", peer, ":", err)
				return
			}
			var syncResp SyncResponse
			if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
				log.Println("Failed to decode sync response from", peer, ":", err)
			}
			resp.Body.Close()

			for _, op := range syncResp.MissedOperations {
				m.applyOperation(op)
			}

			if len(syncResp.MissedOperations) > 0 {
				log.Printf("Synchronized with peer %s: received %d operations", peer, len(syncResp.MissedOperations))
			}
		}()
	}
}

func main() {
	var id string
	var address string
	var peersFlag string

	flag.StringVar(&id, "id", "replicaA", "Replica ID")
	flag.StringVar(&address, "address", "localhost:8090", "Address of this replica (e.g., node0:8090)")
	flag.StringVar(&peersFlag, "peers", "", "Comma-separated list of peer addresses (e.g., node0:8090,node1:8091,node2:8092)")
	flag.Parse()

	var peers []string
	if peersFlag != "" {
		peers = splitAndTrim(peersFlag)
	}

	m := NewLWWMap(ReplicaID(id), peers)

	http.HandleFunc("/map", m.handlePatch)
	http.HandleFunc("/get", m.handleGet)
	http.HandleFunc("/replicate", m.handleReplicate)
	http.HandleFunc("/sync", m.handleSync)

	fmt.Printf("Starting replica %s on %s with peers %v\n", id, address, peers)

	go func() {
		ticker := time.NewTicker(syncInterval)
		defer ticker.Stop()
		for range ticker.C {
			m.syncFromPeers()
		}
	}()

	log.Fatal(http.ListenAndServe(address, nil))
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
