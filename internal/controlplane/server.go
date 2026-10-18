// Package controlplane
package controlplane

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"uuid"
)

var (
	ErrUnknownInstance = errors.New("unknown instance")
	ErrInvalidService  = errors.New("invalid service")
	ErrInvalidPort     = errors.New("invalid port")
	ErrSubscriberGone  = errors.New("subscriber disconnected")
)

const (
	SubscriberHandshakeTimeout = 5 * time.Second
	SubscriberWriteTimeout     = 3 * time.Second
	DefaultHeartbeatTTL        = 10 * time.Second
	DefaultReapInterval        = 2 * time.Second
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

func newSubscriber(id SubscriberID, conn net.Conn) *Subscriber {
	return &Subscriber{ID: id, Conn: conn, Updates: make(chan []byte, 1)}
}

func (s *Subscriber) writeLoop(vs *ViewServer) {
	defer func() {
		err := s.Conn.Close()
		if err != nil {
			log.Printf("error interacting with connection %v", err)
		}
		vs.removeSubscriber(s.ID)
	}()

	for data := range s.Updates {
		if err := writeMessage(s.Conn, data); err != nil {
			log.Printf("couldn't to %s write to gateway %v", s.ID, err)
		}
	}
}

type RegisterRequest struct {
	Service ServiceID `json:"service"`
	Port    int       `json:"port"`
}
type Ack struct {
	ID      string `json:"id"`
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
	mu          sync.RWMutex
	View        View
	LastSeen    map[InstanceID]time.Time
	Subscribers map[SubscriberID]*Subscriber
	ViewChanged chan struct{}
}

func NewViewServer(app string) *ViewServer {
	return &ViewServer{
		Application: app,
		View: View{
			Version:  0,
			Services: make(map[ServiceID]Instances),
		},
		LastSeen:    make(map[InstanceID]time.Time),
		Subscribers: make(map[SubscriberID]*Subscriber),
		ViewChanged: make(chan struct{}, 1),
	}
}

func (vs *ViewServer) addSubscriber(subscriber *Subscriber) {
	if vs.Subscribers == nil {
		vs.Subscribers = make(map[SubscriberID]*Subscriber)
	}

	vs.Subscribers[subscriber.ID] = subscriber
}

func (vs *ViewServer) removeSubscriber(id SubscriberID) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	delete(vs.Subscribers, id)
}

func (s *Subscriber) pushLatest(data []byte) {
	select {
	case <-s.Updates:
	default:
	}

	s.Updates <- data
}

func (vs *ViewServer) subscribe(conn net.Conn) error {
	gatewayID := SubscriberID(uuid.New().String())

	vs.mu.RLock()

	message := struct {
		ID   SubscriberID `json:"id"`
		View View         `json:"view"`
	}{
		ID:   gatewayID,
		View: vs.View,
	}

	data, err := json.Marshal(message)

	vs.mu.RUnlock()

	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("marshal initial view: %w", err)
	}

	if err := writeMessage(conn, data); err != nil {
		_ = conn.Close()
		return fmt.Errorf("send initial view: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = conn.Close()
		return fmt.Errorf("set ACK deadline: %w", err)
	}

	err = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("couldn't read from the subscriber maybe pipe closed %w", err)
	}

	ackData, err := readMessage(conn)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("read gateway ACK: %w", err)
	}

	var ack Ack

	if err := json.Unmarshal(ackData, &ack); err != nil {
		_ = conn.Close()
		return fmt.Errorf("decode gateway ACK: %w", err)
	}

	initialVersion := message.View.Version

	if ack.ID != string(gatewayID) {
		_ = conn.Close()
		return fmt.Errorf("unexpected ACK ID: got %s, expected %s", ack.ID, gatewayID)
	}

	if ack.Version != initialVersion {
		_ = conn.Close()
		return fmt.Errorf("unexpected ACK version: got %d, expected %d", ack.Version, initialVersion)
	}
	subscriber := newSubscriber(gatewayID, conn)

	vs.mu.Lock()
	vs.addSubscriber(subscriber)

	missedUpdate := vs.View.Version > initialVersion

	vs.mu.Unlock()

	if missedUpdate {
		vs.signalViewChanged()
	}

	subscriber.writeLoop(vs)
	return nil
}

func readMessage(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, fmt.Errorf("read message length: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:])

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
	err := conn.SetWriteDeadline(time.Now().Add(SubscriberWriteTimeout))

	if err != nil {
		return fmt.Errorf("set write deadline : %w", err)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))

	if _, err := conn.Write(header[:]); err != nil {
		return fmt.Errorf("write message length: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("write message payload: %w", err)
	}

	return nil
}

func (vs *ViewServer) updateSubscribers(data []byte) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	for _, sub := range vs.Subscribers {
		sub.pushLatest(data)
	}
}

func (vs *ViewServer) register(w http.ResponseWriter, r *http.Request) {
	defer func () { 
		err := r.Body.Close() 
		if err != nil {
			log.Printf("error on gateway end %v", err)
		}
	}()

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
		return
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		http.Error(w, "invalid remote address", http.StatusBadRequest)
		return
	}

	address := net.JoinHostPort(host, strconv.Itoa(req.Port))
	instance := vs.applyRegistration(address, req.Service)

	id := struct {
		ID InstanceID `json:"id"`
	}{
		ID: instance.ID,
	}

	response, err := json.Marshal(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(http.StatusOK)

	if _, err = w.Write(response); err != nil {
		log.Printf("encoutered err %v", err)
	}
}

func (vs *ViewServer) applyRegistration(address string, service ServiceID) Instance {
	vs.mu.Lock()
	instances := vs.View.Services[service]
	if instances == nil {
		instances = make(Instances)
		vs.View.Services[service] = instances
	}

	for _, existing := range instances {
		if existing.Endpoint == address {
			vs.LastSeen[existing.ID] = time.Now()
			vs.mu.Unlock()
			return existing
		}
	}

	id := InstanceID(uuid.New().String())
	instance := Instance{ID: id, Endpoint: address}

	vs.View.Version++
	instances.Add(id, instance)
	vs.LastSeen[instance.ID] = time.Now()
	vs.mu.Unlock()

	vs.signalViewChanged()

	return instance
}

func (vs *ViewServer) PrintView() {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	data, err := json.MarshalIndent(vs.View, "", "    ")
	if err != nil {
		fmt.Println("error: ", err)
		return
	}

	fmt.Println(string(data))
}

func (vs *ViewServer) heartbeat(w http.ResponseWriter, r *http.Request) {
	rawID := r.Header.Get("X-Instance-ID")

	if strings.TrimSpace(string(rawID)) == "" {
		http.Error(w, "instance ID not provided", http.StatusBadRequest)
		return
	}
	id := InstanceID(rawID)

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

func (vs *ViewServer) runReaper(ctx context.Context, ttl, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			vs.reapExpired(now, ttl)
		}
	}
}

func (vs *ViewServer) reapExpired(now time.Time, ttl time.Duration) bool {
	vs.mu.Lock()

	changed := false

	for serviceID, instances := range vs.View.Services {
		for instID := range instances {
			lastSeen, exists := vs.LastSeen[instID]
			if !exists || now.Sub(lastSeen) > ttl {
				delete(instances, instID)
				delete(vs.LastSeen, instID)
				changed = true
			}
		}

		if len(instances) == 0 {
			delete(vs.View.Services, serviceID)
		}
	}
	vs.mu.Unlock()

	if changed {
		vs.View.Version++
		vs.signalViewChanged()
	}

	return changed
}

func (vs *ViewServer) recordHeartbeat(id InstanceID) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if _, exists := vs.LastSeen[id]; !exists {
		return ErrUnknownInstance
	}

	vs.LastSeen[id] = time.Now()

	return nil
}

func (vs *ViewServer) signalViewChanged() {
	select {
	case vs.ViewChanged <- struct{}{}:
	default:
	}
}

func (vs *ViewServer) runPublisher(ctx context.Context) {
	for range vs.ViewChanged {
		vs.mu.RLock()
		data, err := json.Marshal(vs.View)
		vs.mu.RUnlock()

		if err != nil {
			// Log the error and handle it.
			continue
		}

		vs.updateSubscribers(data)
	}
}

func handleSubscription(vs *ViewServer, conn net.Conn) {
    if err := vs.subscribe(conn); err != nil {
        log.Printf("subscription failed: %v", err)
    }
}

func (vs *ViewServer) Start() {
}

func RunRegistrationServer(vs *ViewServer) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /register", vs.register)
	mux.HandleFunc("POST /heartbeat", vs.heartbeat)

	return mux
}

func runSubscriptionServer(vs *ViewServer) error {
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		return fmt.Errorf("listen on subscripiton port: %w", err)
	}

	defer func () {
		err := listener.Close() 
		if err != nil { 
			log.Printf("error with tcp socket %v", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			log.Printf("accept subscripiton connection : %v", err)
		}

		go handleSubscription(vs, conn)

	}
}
