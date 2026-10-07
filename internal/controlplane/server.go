// Package controlplane
package controlplane

import (
	// "encoding/json/v2"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
	"uuid"

)




type ViewServer struct {
	Application string
	Mu          sync.RWMutex
	View        View
	LastSeen    map[InstanceID]time.Time
	Subscribers map[SubscriberID]*Subscriber
}

func (vs *ViewServer) addSubscriber(subcriber *Subscriber) {
	if vs.Subscribers == nil {
		vs.Subscribers = make(map[SubscriberID]*Subscriber)
	}

	vs.Subscribers[subcriber.ID] = subcriber
}

func (vs *ViewServer) subscribe(conn net.Conn) Subscriber {
	gatewayID := uuid.New()

	conn.Write(gatewayID[:])

	subscriber := Subscriber{ID: SubscriberID(gatewayID.String()), Conn: conn }

	vs.Mu.Lock()
	defer vs.Mu.Unlock()

	vs.addSubscriber(&subscriber)

	return  subscriber
}

func (vs *ViewServer) register(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req RegisterRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "Invalid Json: "+err.Error(), http.StatusBadRequest)
		return
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		http.Error(w, "invalid remote address", http.StatusBadRequest)
		return
	}
	address := net.JoinHostPort(host, strconv.Itoa(req.Port))
	instance := vs.applyRegistration(address, req.Service)
	fmt.Println(req)
	w.WriteHeader(http.StatusOK)
	vs.PrintView()

}

func (vs *ViewServer) applyRegistration(address string, service ServiceID) Instance{
	id := InstanceID(uuid.New().String())
	instance := Instance{id, address}
	vs.Mu.Lock()
	defer vs.Mu.Unlock()
	instances := vs.View.Services[service]
	if instances == nil {
		instances = make(Instances)
		vs.View.Services[service] = instances
	}

	vs.View.Services[service].Add(id, instance)
	vs.LastSeen[instance.ID] = time.Now()

	return instance
}

func (vs *ViewServer) PrintView() {
	vs.Mu.RLock()
	defer vs.Mu.RUnlock()

	data, err := json.MarshalIndent(vs.View, "", "    ")
	if err != nil {
		fmt.Println("error: ", err)
		return
	}

	fmt.Println(string(data))
}

func (vs *ViewServer) heartbeat(w http.ResponseWriter, r *http.Request) {
	id := InstanceID(r.Header.Get("X-Instance-ID"))

	vs.Mu.Lock()
	defer vs.Mu.Unlock()

	if _, exists := vs.LastSeen[id]; !exists {
		http.Error(w, "unknown instance", http.StatusNotFound)
		return
	}

	vs.LastSeen[id] = time.Now()
}

func RunRegistrationServer(vs *ViewServer) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/register", vs.register)
	mux.HandleFunc("/heartbeat", vs.heartbeat)

	return mux
}


func runSubscriptionServer(vs *ViewServer) {
	listener, err := net.Listen("tcp", ":9000")

	if err != nil {
		log.Fatal(err)
	}

	defer listener.Close()


	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go vs.subscribe(conn)
	}
}
