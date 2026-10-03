// Package controlplane
package controlplane

import (
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"time"
)

type ServiceID string
type InstanceID string
type SubscriberID string

type Subscriber struct {
	ID      SubscriberID
	Conn    net.Conn
	Updates chan View
}

type Instance struct {
	ID       InstanceID
	Endpoint netip.AddrPort
}

type Service struct {
	ID        ServiceID
	Instances map[InstanceID]Instance
}
type View struct {
	Version  uint64
	Services map[ServiceID]Service
}

type ViewServer struct {
	Application string
	View        View
	LastSeen    map[InstanceID]time.Time
	subscribers map[SubscriberID]*Subscriber
}

func (vs *ViewServer) regiseter(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
}

func (vs *ViewServer) heartbeat(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
}

func runRegistrationServer(vs *ViewServer) {
	mux := http.NewServeMux()

	mux.HandleFunc("/register", vs.regiseter)
	mux.HandleFunc("/heartbeat", vs.regiseter)
}
