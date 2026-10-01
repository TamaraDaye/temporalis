// Package controlplane
package controlplane

import (
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


func runRegistrationServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/register", register)
	mux.HandleFunc("/heartbeat", heartbeat)
}

func register (w http.ResponseWriter, r *http.Request){ 
}


func heartbeat (w http.ResponseWriter, r *http.Request){ 
}
