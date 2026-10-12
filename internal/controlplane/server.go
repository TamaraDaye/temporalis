// Package controlplane
package controlplane

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"uuid"
	"io"
)

var (
	ErrUnknownInstance = errors.New("unknown instance")
	ErrInvalidService  = errors.New("invalid service")
	ErrInvalidPort     = errors.New("invalid port")
	ErrSubscriberGone  = errors.New("subscriber disconnected")
)

type (
	ServiceID    string
	InstanceID   string
	SubscriberID string
	Instances    map[InstanceID]Instance
)

type Subscriber struct {
    ID      SubscriberID
    Conn    net.Conn
    Updates chan []byte

}

func (s *Subscriber) update() {
}


type RegisterRequest struct {
	Service ServiceID `json:"service"`
	Port    int       `json:"port"`
}
type Ack struct {
	ID string  `json:"id"`
    Version uint64 `json:"version"`
}

func validateRequest(req *RegisterRequest) error {
	var errs []error
	if strings.TrimSpace(string(req.Service)) == "" {
		errs = append(errs, ErrInvalidService)
	}
	if req.Port < 1 || req.Port > 65535 {
		errs = append(errs, ErrInvalidPort)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

type Instance struct {
	ID       InstanceID `json:"id"`
	Endpoint string     `json:"endpoint"`
}

type View struct {
	Version  uint64                  `json:"version"`
	Services map[ServiceID]Instances `json:"services"`
}

func (i Instances) Add(id InstanceID, instance Instance) {
	i[id] = instance
}

type ViewServer struct {
	Application string
	Mu          sync.RWMutex
	View        View
	LastSeen    map[InstanceID]time.Time
	Subscribers map[SubscriberID]*Subscriber
	ViewChanged chan struct{}
}

func newSubscriber(id SubscriberID, conn net.Conn) Subscriber {
	return Subscriber{ID: id, Conn: conn}
}

func (vs *ViewServer) addSubscriber(subcriber *Subscriber) {
	if vs.Subscribers == nil {
		vs.Subscribers = make(map[SubscriberID]*Subscriber)
	}

	vs.Subscribers[subcriber.ID] = subcriber
}

func (vs *ViewServer) subscribe(conn net.Conn) error {
    gatewayID := SubscriberID(uuid.New().String())

    vs.Mu.RLock()

    message := struct {
        ID   SubscriberID `json:"id"`
        View View         `json:"view"`
    }{
        ID:   gatewayID,
        View: vs.View,
    }

    data, err := json.Marshal(message)

    vs.Mu.RUnlock()

    if err != nil {
        return fmt.Errorf("marshal initial view: %w", err)
    }

    if err := writeMessage(conn, data); err != nil {
        return fmt.Errorf("send initial view: %w", err)
    }

    if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
        return fmt.Errorf("set ACK deadline: %w", err)
    }

    defer conn.SetReadDeadline(time.Time{})

    // Read the ACK using the same length-prefixed framing.
    ackData, err := readMessage(conn)
    if err != nil {
        return fmt.Errorf("read gateway ACK: %w", err)
    }

    var ack Ack
    if err := json.Unmarshal(ackData, &ack); err != nil {
        return fmt.Errorf("decode gateway ACK: %w", err)
    }

    initialVersion := message.View.Version

    if ack.Version != initialVersion {
        return fmt.Errorf(
            "unexpected ACK version: got %d, expected %d",
            ack.Version, initialVersion,
        )
    }

    subscriber := newSubscriber(gatewayID, conn)

    vs.Mu.Lock()

    vs.addSubscriber(&subscriber)

    if vs.View.Version > initialVersion {
        latest, err := json.Marshal(vs.View)
        if err != nil {
            delete(vs.Subscribers, gatewayID)
            vs.Mu.Unlock()
            return fmt.Errorf("marshal reconciliation snapshot: %w", err)
        }

        subscriber.Updates <- latest
    }

    vs.Mu.Unlock()

    return nil
}

func readMessage(conn net.Conn) ([]byte, error) {
    var length uint32

    if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
        return nil, fmt.Errorf("read message length: %w", err)
    }

    const maxMessageSize = 10 << 20 // 10 MiB

    if length > maxMessageSize {
        return nil, fmt.Errorf("message too large: %d bytes", length)
    }

    data := make([]byte, length)

    if _, err := io.ReadFull(conn, data); err != nil {
        return nil, fmt.Errorf("read message payload: %w", err)
    }

    return data, nil
}

func writeMessage(conn net.Conn, data []byte) error {
	length := uint32(len(data))

	if err := binary.Write(conn, binary.BigEndian, length); err != nil {
		return fmt.Errorf("writing byte length to %v with error :%w", conn, err)
	}

	_, err := conn.Write(data)
	if err != nil {
		return fmt.Errorf("writing data to %v with error :%w", conn, err)
	}

	return nil
}

func (vs *ViewServer) update() {
}

func (vs *ViewServer) gatewayHeartbeat() {
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

	if err := validateRequest(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		http.Error(w, "invalid remote address", http.StatusBadRequest)
		return
	}
	address := net.JoinHostPort(host, strconv.Itoa(req.Port))
	vs.applyRegistration(address, req.Service)
	w.WriteHeader(http.StatusOK)
}

func (vs *ViewServer) applyRegistration(address string, service ServiceID) Instance {
	id := InstanceID(uuid.New().String())
	instance := Instance{id, address}
	vs.Mu.Lock()
	defer vs.Mu.Unlock()
	instances := vs.View.Services[service]
	if instances == nil {
		instances = make(Instances)
		vs.View.Services[service] = instances
	}

	instances.Add(id, instance)
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

	if err := vs.recordHeartbeat(id); err != nil {
		if errors.Is(err, ErrUnknownInstance) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (vs *ViewServer) recordHeartbeat(id InstanceID) error {
	vs.Mu.Lock()
	defer vs.Mu.Unlock()

	if _, exists := vs.LastSeen[id]; !exists {
		return ErrUnknownInstance
	}

	vs.LastSeen[id] = time.Now()

	return nil
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
