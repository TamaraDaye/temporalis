// Package controlplane
package controlplane

import (
	"bytes"
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
)

const (
	SubscriberHandshakeTimeout = 5 * time.Second
	SubscriberWriteTimeout     = 3 * time.Second
	DefaultHeartbeatTTL        = 10 * time.Second
	DefaultReapInterval        = 2 * time.Second
	DefaultSubscriptionAddr    = ":9000"
	DefaultHTTPAddr            = ":8080"
	DefaultApplicationName     = "temporalis"
	maxMessageSize             = 10 << 20 // 10 MiB
)

// Subscriber liveness timings. They are variables so tests can shorten them.
var (
	subscriberPingInterval = 15 * time.Second
	subscriberPongWait     = 45 * time.Second
)

// messageType identifies the kind of frame on a subscription connection.
type messageType byte

const (
	messageView messageType = iota + 1
	messageAck
	messagePing
	messagePong
)

func (m messageType) String() string {
	switch m {
	case messageView:
		return "view"
	case messageAck:
		return "ack"
	case messagePing:
		return "ping"
	case messagePong:
		return "pong"
	default:
		return fmt.Sprintf("unknown(%d)", byte(m))
	}
}

type gatewayManager struct {
	mu          sync.RWMutex
	subscribers map[SubscriberID]*Subscriber
	handlers    sync.WaitGroup
}

func newGatewayManager() *gatewayManager {
	return &gatewayManager{subscribers: make(map[SubscriberID]*Subscriber)}
}

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
	done        chan struct{}
	closeOnce   sync.Once
	lastVersion atomic.Uint64
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
	s := &Subscriber{
		ID:      id,
		Conn:    conn,
		Updates: make(chan []byte, 1),
		done:    make(chan struct{}),
	}
	s.lastVersion.Store(viewVersion)
	return s
}

// close tears down the subscriber once, unblocking both loops. Closing the
// connection also releases a read that is parked on Read.
func (s *Subscriber) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.Conn != nil {
			_ = s.Conn.Close()
		}
	})
}

// writeLoop owns all writes on the connection: view snapshots and keepalive
// pings. It exits when the connection is closed or a write fails.
func (s *Subscriber) writeLoop() {
	ticker := time.NewTicker(subscriberPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case data := <-s.Updates:
			if err := writeFrame(s.Conn, messageView, data); err != nil {
				log.Printf("write view to subscriber %s: %v", s.ID, err)
				s.close()
				return
			}
		case <-ticker.C:
			if err := writeFrame(s.Conn, messagePing, nil); err != nil {
				log.Printf("write ping to subscriber %s: %v", s.ID, err)
				s.close()
				return
			}
		}
	}
}

// readLoop watches the connection for pongs and detects half-open peers. If no
// pong arrives within subscriberPongWait it closes the subscriber.
func (s *Subscriber) readLoop() {
	for {
		if err := s.Conn.SetReadDeadline(time.Now().Add(subscriberPongWait)); err != nil {
			s.close()
			return
		}

		typ, _, err := readFrame(s.Conn)
		if err != nil {
			s.close()
			return
		}

		switch typ {
		case messagePong:
			// deadline is refreshed at the top of the loop
		case messagePing:
			// tolerate a client-initiated ping; nothing to do, next read
		default:
			log.Printf("subscriber %s sent unexpected frame %s", s.ID, typ)
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
	view        View
	lastSeen    map[InstanceID]*trackedInstance
	ViewChanged chan struct{}
	snapshot    atomic.Pointer[viewSnapshot]
}

func NewViewServer(app string) *ViewServer {
	vs := &ViewServer{
		Application: app,
		view: View{
			Version:  0,
			Services: make(map[ServiceID]Instances),
		},
		lastSeen:    make(map[InstanceID]*trackedInstance),
		ViewChanged: make(chan struct{}, 1),
	}
	vs.refreshSnapshotLocked()
	return vs
}

// refreshSnapshotLocked marshals the current view and publishes it atomically.
// It must be called while holding vs.mu, after any change to vs.view.
func (vs *ViewServer) refreshSnapshotLocked() {
	data, err := json.Marshal(vs.view)
	if err != nil {
		// View contains only strings, maps and plain structs, so this is
		// effectively unreachable; keep the previous snapshot on failure.
		log.Printf("marshal view snapshot: %v", err)
		return
	}
	vs.snapshot.Store(&viewSnapshot{version: vs.view.Version, data: data})
}

// latestSnapshot returns the latest published view snapshot without taking vs.mu.
func (vs *ViewServer) latestSnapshot() viewSnapshot {
	if snap := vs.snapshot.Load(); snap != nil {
		return *snap
	}
	return viewSnapshot{}
}

func (vs *ViewServer) Version() uint64 {
	return vs.latestSnapshot().version
}


func (g *gatewayManager) add(sub *Subscriber) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.subscribers == nil {
		g.subscribers = make(map[SubscriberID]*Subscriber)
	}
	g.subscribers[sub.ID] = sub
}

func (g *gatewayManager) remove(id SubscriberID) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.subscribers, id)
}

func (g *gatewayManager) len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.subscribers)
}

// spawn runs a subscription handler while tracking it for graceful shutdown.
func (g *gatewayManager) spawn(vs *ViewServer, conn net.Conn) {
	g.handlers.Add(1)
	go func() {
		defer g.handlers.Done()
		handleSubscription(vs, g, conn)
	}()
}

// closeAll terminates every subscriber connection, unblocking its loops.
func (g *gatewayManager) closeAll() {
	g.mu.RLock()
	subscribers := make([]*Subscriber, 0, len(g.subscribers))
	for _, sub := range g.subscribers {
		subscribers = append(subscribers, sub)
	}
	g.mu.RUnlock()

	for _, sub := range subscribers {
		sub.close()
	}
}

// wait blocks until all spawned subscription handlers have returned.
func (g *gatewayManager) wait() {
	g.handlers.Wait()
}

func (g *gatewayManager) updateSubscribers(snapshot viewSnapshot) {
	g.mu.RLock()
	toUpdate := make([]*Subscriber, 0, len(g.subscribers))
	for _, sub := range g.subscribers {
		select {
		case <-sub.done:
			continue
		default:
		}

		if sub.lastVersion.Load() >= snapshot.version {
			continue
		}
		sub.lastVersion.Store(snapshot.version)
		toUpdate = append(toUpdate, sub)
	}
	g.mu.RUnlock()

	for _, sub := range toUpdate {
		sub.pushLatest(snapshot.data)
	}
}

func (vs *ViewServer) snapshotInitialMessage(gatewayID SubscriberID) ([]byte, uint64, error) {
	snap := vs.latestSnapshot()

	message := struct {
		ID   SubscriberID    `json:"id"`
		View json.RawMessage `json:"view"`
	}{
		ID:   gatewayID,
		View: json.RawMessage(snap.data),
	}

	data, err := json.Marshal(message)
	return data, snap.version, err
}

// currentVersion reports the current view version without taking vs.mu.
func (vs *ViewServer) currentVersion() uint64 {
	return vs.Version()
}

func (s *Subscriber) pushLatest(data []byte) {
	select {
	case <-s.done:
		return
	default:
	}

	select {
	case <-s.Updates:
	default:
	}

	select {
	case s.Updates <- data:
	case <-s.done:
	}
}

func handleSubscription(vs *ViewServer, gateways *gatewayManager, conn net.Conn) {
	defer conn.Close()

	gatewayID := SubscriberID(uuid.New().String())

	data, initialVersion, err := vs.snapshotInitialMessage(gatewayID)
	if err != nil {
		log.Printf("marshal initial view for %s: %v", gatewayID, err)
		return
	}

	if err := writeFrame(conn, messageView, data); err != nil {
		log.Printf("send initial view to %s: %v", gatewayID, err)
		return
	}

	if err := conn.SetReadDeadline(time.Now().Add(SubscriberHandshakeTimeout)); err != nil {
		log.Printf("set ACK deadline for %s: %v", gatewayID, err)
		return
	}

	typ, ackData, err := readFrame(conn)
	if err != nil {
		log.Printf("read gateway ACK from %s: %v", gatewayID, err)
		return
	}
	if typ != messageAck {
		log.Printf("unexpected frame from %s during handshake: %s", gatewayID, typ)
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
	gateways.add(subscriber)
	defer gateways.remove(gatewayID)
	defer subscriber.close()

	// The view may have advanced while we were waiting for the ACK, in which
	// case the corresponding ViewChanged signal may already have been consumed.
	// Re-checking the version here guarantees a reconciling broadcast.
	if vs.currentVersion() > initialVersion {
		vs.signalViewChanged()
	}

	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		subscriber.readLoop()
	}()
	go func() {
		defer loops.Done()
		subscriber.writeLoop()
	}()
	loops.Wait()
}

func readFrame(conn net.Conn) (messageType, []byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return 0, nil, fmt.Errorf("read message length: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:])

	if length > maxMessageSize {
		return 0, nil, fmt.Errorf("message too large: %d bytes", length)
	}
	if length < 1 {
		return 0, nil, errors.New("empty frame")
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, nil, fmt.Errorf("read message payload: %w", err)
	}

	return messageType(body[0]), body[1:], nil
}

func writeFrame(conn net.Conn, typ messageType, payload []byte) error {
	length := len(payload) + 1
	if length > maxMessageSize {
		return fmt.Errorf("message too large: %d bytes", length)
	}

	if err := conn.SetWriteDeadline(time.Now().Add(SubscriberWriteTimeout)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(length))

	bufs := net.Buffers{header[:], {byte(typ)}, payload}

	if _, err := bufs.WriteTo(conn); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return nil
}

func (vs *ViewServer) deregisterInstance(id InstanceID) error {
	vs.mu.Lock()

	tracked, exists := vs.lastSeen[id]
	if !exists {
		vs.mu.Unlock()
		return ErrUnknownInstance
	}

	delete(vs.lastSeen, id)

	if instances, ok := vs.view.Services[tracked.service]; ok {
		delete(instances, id)
		if len(instances) == 0 {
			delete(vs.view.Services, tracked.service)
		}
	}

	vs.view.Version++
	vs.refreshSnapshotLocked()
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

		err := vs.deregisterInstance(id)
		if err != nil {
			if errors.Is(err, ErrUnknownInstance) {
				http.Error(w, "Unknown instanceID provided", http.StatusNotFound)
				return
			}
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}

	}
}

func (vs *ViewServer) applyRegistration(address string, service ServiceID) Instance {
	vs.mu.Lock()
	instances := vs.view.Services[service]

	if instances == nil {
		instances = make(Instances)
		vs.view.Services[service] = instances
	}
	for _, existing := range instances {
		if existing.Endpoint == address {
			if tracked, ok := vs.lastSeen[existing.ID]; ok {
				tracked.lastSeen.Store(time.Now().UnixNano())
			}
			vs.mu.Unlock()
			return existing
		}
	}

	id := InstanceID(fmt.Sprintf("%s-%s", service, uuid.New().String()))
	instance := Instance{ID: id, Endpoint: address}

	tracked := &trackedInstance{service: service}
	tracked.lastSeen.Store(time.Now().UnixNano())
	vs.view.Version++
	instances.Add(id, instance)
	vs.lastSeen[instance.ID] = tracked
	vs.refreshSnapshotLocked()
	vs.mu.Unlock()

	vs.signalViewChanged()

	return instance
}

func handleRegistration(vs *ViewServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

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
		//TODO: add host and port field to instances during registraion to avoid flat network reg if not on the same vlan or lan

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
	snap := vs.latestSnapshot()

	var buf bytes.Buffer
	if err := json.Indent(&buf, snap.data, "", "    "); err != nil {
		fmt.Println("error: ", err)
		return
	}

	fmt.Println(buf.String())
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

	for serviceID, instances := range vs.view.Services {
		for instID := range instances {
			tracked, exists := vs.lastSeen[instID]
			if !exists || now.Sub(time.Unix(0, tracked.lastSeen.Load())) > ttl {
				delete(instances, instID)
				delete(vs.lastSeen, instID)
				changed = true
			}
		}

		if len(instances) == 0 {
			delete(vs.view.Services, serviceID)
		}
	}

	if changed {
		vs.view.Version++
		vs.refreshSnapshotLocked()
	}
	vs.mu.Unlock()

	if changed {
		vs.signalViewChanged()
	}

	return changed
}

func (vs *ViewServer) recordHeartbeat(id InstanceID) error {
	vs.mu.RLock()
	tracked, exists := vs.lastSeen[id]
	defer vs.mu.RUnlock()

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

func (vs *ViewServer) runPublisher(ctx context.Context, gateways *gatewayManager) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-vs.ViewChanged:
			// The snapshot is published atomically by the writer, so the
			// publisher never needs to take vs.mu.
			gateways.updateSubscribers(vs.latestSnapshot())
		}
	}
}

// Config controls a control-plane Server. Zero values are replaced with the
// package defaults by NewServer.
type Config struct {
	Application      string
	HTTPAddr         string
	SubscriptionAddr string
	HeartbeatTTL     time.Duration
	ReapInterval     time.Duration
}

func DefaultConfig() Config {
	return Config{
		Application:      DefaultApplicationName,
		HTTPAddr:         DefaultHTTPAddr,
		SubscriptionAddr: DefaultSubscriptionAddr,
		HeartbeatTTL:     DefaultHeartbeatTTL,
		ReapInterval:     DefaultReapInterval,
	}
}

func (c Config) withDefaults() Config {
	if c.Application == "" {
		c.Application = DefaultApplicationName
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = DefaultHTTPAddr
	}
	if c.SubscriptionAddr == "" {
		c.SubscriptionAddr = DefaultSubscriptionAddr
	}
	if c.HeartbeatTTL <= 0 {
		c.HeartbeatTTL = DefaultHeartbeatTTL
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = DefaultReapInterval
	}
	return c
}

// Server wires the view, gateway manager, HTTP API, subscription stream,
// publisher and reaper into one runnable process.
type Server struct {
	cfg      Config
	view     *ViewServer
	gateways *gatewayManager
}

func NewServer(cfg Config) *Server {
	cfg = cfg.withDefaults()
	return &Server{
		cfg:      cfg,
		view:     NewViewServer(cfg.Application),
		gateways: newGatewayManager(),
	}
}

// Run starts all control-plane components and blocks until ctx is cancelled or
// a component fails. Shutdown is graceful: the HTTP server drains, the
// subscription listener closes, and subscriber connections are torn down.
func (s *Server) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 4)

	go func() {
		s.view.runPublisher(ctx, s.gateways)
		errCh <- nil
	}()
	go func() {
		s.view.runReaper(ctx, s.cfg.HeartbeatTTL, s.cfg.ReapInterval)
		errCh <- nil
	}()

	httpServer := &http.Server{
		Addr:    s.cfg.HTTPAddr,
		Handler: NewRegistrationHandler(s.view),
	}
	go func() {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	go func() {
		errCh <- runSubscriptionServerOn(ctx, s.view, s.gateways, s.cfg.SubscriptionAddr)
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("shutdown HTTP server: %v", err)
		}
	}()

	var firstErr error
	for i := 0; i < 4; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	return firstErr
}

func NewRegistrationHandler(vs *ViewServer) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /register", handleRegistration(vs))
	mux.HandleFunc("DELETE /deregister", handleDeregistration(vs))
	mux.HandleFunc("POST /heartbeat", handleHeartBeats(vs))

	return mux
}

func runSubscriptionServerOn(ctx context.Context, vs *ViewServer, gateways *gatewayManager, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on subscription port: %w", err)
	}

	return serveSubscriptions(ctx, vs, gateways, listener)
}

func serveSubscriptions(ctx context.Context, vs *ViewServer, gateways *gatewayManager, listener net.Listener) error {
	// Tear down live gateway connections and wait for their handlers before
	// reporting shutdown complete.
	defer func() {
		gateways.closeAll()
		gateways.wait()
	}()

	go func() {
		<-ctx.Done()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("close subscription listener: %v", err)
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

		gateways.spawn(vs, conn)
	}
}
