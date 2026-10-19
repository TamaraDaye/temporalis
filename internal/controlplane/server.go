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
	"sync/atomic"
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
	ID          SubscriberID
	Conn        net.Conn
	Updates     chan []byte
	lastVersion uint64
}

type viewSnapshot struct {
	version uint64
	data    []byte
}

type trackedInstance struct {
	service  ServiceID
	lastSeen atomic.Int64
}

func newSubscriber(id SubscriberID, conn net.Conn, viewVersion uint64) *Subscriber {
	return &Subscriber{ID: id, Conn: conn, Updates: make(chan []byte, 1), lastVersion: viewVersion}
}

func (s *Subscriber) writeLoop() {
	for data := range s.Updates {
		if err := writeMessage(s.Conn, data); err != nil {
			log.Printf("could not connect to subscriber %v", err)
			return
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
	LastSeen    map[InstanceID]*trackedInstance
	Subscribers map[SubscriberID]*Subscriber
	ViewChanged chan struct{}
	start       time.Time
}

func NewViewServer(app string) *ViewServer {
	return &ViewServer{
		Application: app,
		View: View{
			Version:  0,
			Services: make(map[ServiceID]Instances),
		},
		LastSeen:    make(map[InstanceID]*trackedInstance),
		Subscribers: make(map[SubscriberID]*Subscriber),
		ViewChanged: make(chan struct{}, 1),
		start: time.Now(),
	}
}

func (vs *ViewServer) addSubscriber(subscriber *Subscriber) {
	if vs.Subscribers == nil {
		vs.Subscribers = make(map[SubscriberID]*Subscriber)
	}

	vs.Subscribers[subscriber.ID] = subscriber
}

func (vs *ViewServer) snapshotInitialMessage(gatewayID SubscriberID) ([]byte, uint64, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	initialVersion := vs.View.Version
	message := struct {
		ID   SubscriberID `json:"id"`
		View View         `json:"view"`
	}{
		ID:   gatewayID,
		View: vs.View,
	}

	data, err := json.Marshal(message)
	return data, initialVersion, err
}

func (vs *ViewServer) registerSubscriber(sub *Subscriber, initialVersion uint64) {
	vs.mu.Lock()
	vs.addSubscriber(sub)
	missedUpdate := vs.View.Version > initialVersion
	vs.mu.Unlock()

	if missedUpdate {
		vs.signalViewChanged()
	}
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

func handleSubscription(vs *ViewServer, conn net.Conn) {
	defer conn.Close()

	gatewayID := SubscriberID(uuid.New().String())

	data, initialVersion, err := vs.snapshotInitialMessage(gatewayID)
	if err != nil {
		log.Printf("marshal initial view for %s: %v", gatewayID, err)
		return
	}

	if err := writeMessage(conn, data); err != nil {
		log.Printf("send initial view to %s: %v", gatewayID, err)
		return
	}

	if err := conn.SetReadDeadline(time.Now().Add(SubscriberHandshakeTimeout)); err != nil {
		log.Printf("set ACK deadline for %s: %v", gatewayID, err)
		return
	}

	ackData, err := readMessage(conn)
	if err != nil {
		log.Printf("read gateway ACK from %s: %v", gatewayID, err)
		return
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		log.Printf("clear ACK deadline for %s: %v", gatewayID, err)
		return
	}

	var ack Ack
	if err := json.Unmarshal(ackData, &ack); err != nil {
		log.Printf("decode gateway ACK from %s: %v", gatewayID, err)
		return
	}

	if ack.ID != string(gatewayID) {
		log.Printf("unexpected ACK ID: got %s, expected %s", ack.ID, gatewayID)
		return
	}

	if ack.Version != initialVersion {
		log.Printf("unexpected ACK version from %s: got %d, expected %d", gatewayID, ack.Version, initialVersion)
		return
	}

	subscriber := newSubscriber(gatewayID, conn, initialVersion)
	vs.registerSubscriber(subscriber, initialVersion)
	defer vs.removeSubscriber(gatewayID)

	subscriber.writeLoop()
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

	bufs := net.Buffers{header[:], data}

	if _, err := bufs.WriteTo(conn); err != nil {
		return fmt.Errorf("write message :%w", err)
	}

	return nil
}

func (vs *ViewServer) updateSubscribers(snapshot viewSnapshot) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	for _, sub := range vs.Subscribers {
		if sub.lastVersion >= snapshot.version {
			continue
		}
		sub.lastVersion = snapshot.version
		sub.pushLatest(snapshot.data)
	}
}

func (vs *ViewServer) deregisterInstance(id InstanceID) error {
	vs.mu.Lock()

	tracked, exists := vs.LastSeen[id]
	if !exists {
		vs.mu.Unlock()
		return ErrUnknownInstance
	}

	delete(vs.LastSeen, id)

	if instances, ok := vs.View.Services[tracked.service]; ok {
		delete(instances, id)
		if len(instances) == 0 {
			delete(vs.View.Services, tracked.service)
		}
	}

	vs.View.Version++
	vs.mu.Unlock()

	vs.signalViewChanged()
	return nil
}

func handleDeregistration(vs *ViewServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rawID := r.Header.Get("X-Instance-ID")

		if strings.TrimSpace(string(rawID)) == "" {
			http.Error(w, "instance ID not provided", http.StatusBadRequest)
			return
		}
		id := InstanceID(rawID)

		vs.deregisterInstance(id)
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
			if tracked, ok := vs.LastSeen[existing.ID]; ok {
				tracked.lastSeen.Store(time.Now().UnixNano())
			}
			vs.mu.Unlock()
			return existing
		}
	}

	id := InstanceID(fmt.Sprintf("%s-%s", service, uuid.New().String()))
	instance := Instance{ID: id, Endpoint: address}

	tracked := &trackedInstance{service: service}
	tracked.lastSeen.Store(int64(time.Since(vs.start)))

	vs.View.Version++
	instances.Add(id, instance)
	vs.LastSeen[instance.ID] = tracked
	vs.mu.Unlock()

	vs.signalViewChanged()

	return instance
}

func handleRegistration(vs *ViewServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
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

func handleHeartBeats(vs *ViewServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
			tracked, exists := vs.LastSeen[instID]
			if !exists || now.Sub(time.Unix(0, tracked.lastSeen.Load())) > ttl {
				delete(instances, instID)
				delete(vs.LastSeen, instID)
				changed = true
			}
		}

		if len(instances) == 0 {
			delete(vs.View.Services, serviceID)
		}
	}

	if changed {
		vs.View.Version++
	}
	vs.mu.Unlock()

	if changed {
		vs.signalViewChanged()
	}

	return changed
}

func (vs *ViewServer) recordHeartbeat(id InstanceID) error {
	vs.mu.RLock()
	tracked, exists := vs.LastSeen[id]
	vs.mu.RUnlock()

	if !exists {
		return ErrUnknownInstance
	}

	tracked.lastSeen.Store(time.Now().UnixNano())
	return nil
}

func (vs *ViewServer) signalViewChanged() {
	select {
	case vs.ViewChanged <- struct{}{}:
	default:
	}
}

func (vs *ViewServer) runPublisher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-vs.ViewChanged:

			vs.mu.RLock()
			version := vs.View.Version
			data, err := json.Marshal(vs.View)
			vs.mu.RUnlock()

			if err != nil {
				log.Printf("couldn't marshal current view %v", err)
				continue
			}

			vs.updateSubscribers(viewSnapshot{version, data})

		}
	}
}

func (vs *ViewServer) Start() {
}

func NewRegistrationHandler(vs *ViewServer) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /register", handleRegistration(vs))
	mux.HandleFunc("DELETE /deregister", handleDeregistration(vs))
	mux.HandleFunc("POST /heartbeat", handleHeartBeats(vs))

	return mux
}

func runSubscriptionServer(ctx context.Context, vs *ViewServer) error {
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		return fmt.Errorf("listen on subscripiton port: %w", err)
	}

	go func() {
		<-ctx.Done()
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

			log.Printf("accept subscription connection %v", err)
			continue
		}

		go handleSubscription(vs, conn)
	}
}
