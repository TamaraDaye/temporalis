package controlplane

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// helper: performs a complete subscriber handshake over a net.Conn and returns the initial View.
func completeHandshake(t *testing.T, clientConn net.Conn) (SubscriberID, View) {
	t.Helper()

	typ, raw, err := readFrame(clientConn)
	if err != nil {
		t.Fatalf("failed to read initial view: %v", err)
	}
	if typ != messageView {
		t.Fatalf("expected initial view frame, got %s", typ)
	}

	var initMsg struct {
		ID   SubscriberID `json:"id"`
		View View         `json:"view"`
	}
	if err := json.Unmarshal(raw, &initMsg); err != nil {
		t.Fatalf("failed to unmarshal initial view: %v", err)
	}

	ackBytes, err := json.Marshal(Ack{
		ID:      string(initMsg.ID),
		Version: initMsg.View.Version,
	})
	if err != nil {
		t.Fatalf("failed to marshal ack: %v", err)
	}

	if err := writeFrame(clientConn, messageAck, ackBytes); err != nil {
		t.Fatalf("failed to write ack: %v", err)
	}

	return initMsg.ID, initMsg.View
}

// readViewFrame reads the next view frame, answering keepalive pings along the
// way. It is what a real gateway client would do.
func readViewFrame(t *testing.T, conn net.Conn) View {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	for {
		typ, payload, err := readFrame(conn)
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}

		switch typ {
		case messageView:
			var view View
			if err := json.Unmarshal(payload, &view); err != nil {
				t.Fatalf("unmarshal view: %v", err)
			}
			return view
		case messagePing:
			if err := writeFrame(conn, messagePong, nil); err != nil {
				t.Fatalf("reply to ping: %v", err)
			}
		default:
			t.Fatalf("unexpected frame while waiting for view: %s", typ)
		}
	}
}

// 1. Test Request Validation & Framing Boundaries
func TestValidateRequestAndFraming(t *testing.T) {
	t.Run("validateRequest", func(t *testing.T) {
		err := validateRequest(&RegisterRequest{Service: "   ", Port: 0})
		if !errors.Is(err, ErrInvalidService) || !errors.Is(err, ErrInvalidPort) {
			t.Fatalf("expected both ErrInvalidService and ErrInvalidPort, got: %v", err)
		}

		err = validateRequest(&RegisterRequest{Service: "auth", Port: 70000})
		if !errors.Is(err, ErrInvalidPort) {
			t.Fatalf("expected ErrInvalidPort for port 70000, got: %v", err)
		}

		if err := validateRequest(&RegisterRequest{Service: "auth", Port: 8080}); err != nil {
			t.Fatalf("expected valid request, got: %v", err)
		}
	})

	t.Run("readFrame oversized payload", func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()

		go func() {
			var header [4]byte
			binary.BigEndian.PutUint32(header[:], (10<<20)+1) // 10 MiB + 1 byte
			_, _ = c1.Write(header[:])
		}()

		_, _, err := readFrame(c2)
		if err == nil || !strings.Contains(err.Error(), "message too large") {
			t.Fatalf("expected oversized message error, got: %v", err)
		}
	})

	t.Run("readFrame unexpected EOF", func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c2.Close()

		go func() {
			var header [4]byte
			binary.BigEndian.PutUint32(header[:], 100)
			_, _ = c1.Write(header[:])
			_, _ = c1.Write([]byte("short"))
			c1.Close()
		}()

		_, _, err := readFrame(c2)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected io.ErrUnexpectedEOF, got: %v", err)
		}
	})

	t.Run("readFrame empty frame", func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()

		go func() {
			var header [4]byte
			_, _ = c1.Write(header[:])
		}()

		_, _, err := readFrame(c2)
		if err == nil || !strings.Contains(err.Error(), "empty frame") {
			t.Fatalf("expected empty frame error, got: %v", err)
		}
	})
}

// 2. Test HTTP Registration, Deduplication, Heartbeats, and Deregistration
func TestHTTPRegistrationLifecycle(t *testing.T) {
	vs := NewViewServer("test-app")
	mux := NewRegistrationHandler(vs)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// A. Invalid JSON & Validation failures
	resp, err := http.Post(ts.URL+"/register", "application/json", strings.NewReader(`{"unknown":123}`))
	if err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown field, got: %v", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Post(ts.URL+"/register", "application/json", strings.NewReader(`{"service":"", "port":-1}`))
	if err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid service/port, got: %v", resp.StatusCode)
	}
	resp.Body.Close()

	// B. Valid Registration
	resp, err = http.Post(ts.URL+"/register", "application/json", strings.NewReader(`{"service":"orders", "port":9001}`))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on register, got: %v, err: %v", resp.StatusCode, err)
	}

	var regResp struct {
		ID InstanceID `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		t.Fatalf("failed to decode register response: %v", err)
	}
	resp.Body.Close()

	if !strings.HasPrefix(string(regResp.ID), "orders-") {
		t.Fatalf("expected service-prefixed ID 'orders-...', got: %s", regResp.ID)
	}

	// C. Duplicate Registration from same host:port should NOT bump Version or create new ID
	resp2, err := http.Post(ts.URL+"/register", "application/json", strings.NewReader(`{"service":"orders", "port":9001}`))
	if err != nil {
		t.Fatalf("duplicate register request failed: %v", err)
	}
	var regResp2 struct {
		ID InstanceID `json:"id"`
	}
	_ = json.NewDecoder(resp2.Body).Decode(&regResp2)
	resp2.Body.Close()

	if regResp.ID != regResp2.ID {
		t.Fatalf("expected duplicate registration to return same ID %s, got %s", regResp.ID, regResp2.ID)
	}
	if vs.Version() != 1 {
		t.Fatalf("expected version to remain 1 on duplicate register, got %d", vs.Version())
	}

	// D. Heartbeat (Valid, Missing Header, Unknown ID)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/heartbeat", nil)
	req.Header.Set("X-Instance-ID", string(regResp.ID))
	hbResp, err := http.DefaultClient.Do(req)
	if err != nil || hbResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 on valid heartbeat, got: %v", hbResp.StatusCode)
	}

	reqMissing, _ := http.NewRequest(http.MethodPost, ts.URL+"/heartbeat", nil)
	hbMissing, _ := http.DefaultClient.Do(reqMissing)
	if hbMissing.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on missing X-Instance-ID, got: %d", hbMissing.StatusCode)
	}

	reqUnknown, _ := http.NewRequest(http.MethodPost, ts.URL+"/heartbeat", nil)
	reqUnknown.Header.Set("X-Instance-ID", "orders-nonexistent")
	hbUnknown, _ := http.DefaultClient.Do(reqUnknown)
	if hbUnknown.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 on unknown X-Instance-ID, got: %d", hbUnknown.StatusCode)
	}

	// E. Deregister (Valid & Unknown)
	delReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/deregister", nil)
	delReq.Header.Set("X-Instance-ID", string(regResp.ID))
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil || (delResp.StatusCode != http.StatusOK && delResp.StatusCode != http.StatusNoContent) {
		t.Fatalf("unexpected status on deregister: %v", delResp.StatusCode)
	}

	// Deregistering a second time should return 404
	delResp2, _ := http.DefaultClient.Do(delReq)
	if delResp2.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 on second deregister, got: %d", delResp2.StatusCode)
	}
}

// 3. Test Reaper Eviction & Synthetic Time Advancement
func TestReaperEviction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vs := NewViewServer("reaper-test")
	inst1 := vs.applyRegistration("10.0.0.1:8080", "payments")
	inst2 := vs.applyRegistration("10.0.0.2:8080", "payments")

	// Advance inst2's heartbeat 8 seconds into the future
	future8s := time.Now().Add(8 * time.Second)
	vs.lastSeen[inst2.ID].lastSeen.Store(future8s.UnixNano())

	// Reap at t = +11s with 10s TTL: inst1 should expire, inst2 should survive
	reapTime1 := time.Now().Add(11 * time.Second)
	if changed := vs.reapExpired(reapTime1, 10*time.Second); !changed {
		t.Fatalf("expected reapExpired to evict inst1")
	}

	vs.mu.RLock()
	_, exists1 := vs.view.Services["payments"][inst1.ID]
	_, exists2 := vs.view.Services["payments"][inst2.ID]
	vs.mu.RUnlock()

	if exists1 || !exists2 {
		t.Fatalf("expected inst1 evicted (%v) and inst2 retained (%v)", exists1, exists2)
	}

	// Reap at t = +25s: inst2 should also expire and "payments" service map should be deleted
	reapTime2 := time.Now().Add(25 * time.Second)
	if changed := vs.reapExpired(reapTime2, 10*time.Second); !changed {
		t.Fatalf("expected reapExpired to evict inst2")
	}

	vs.mu.RLock()
	_, serviceExists := vs.view.Services["payments"]
	vs.mu.RUnlock()
	if serviceExists {
		t.Fatalf("expected empty 'payments' service map to be cleaned up")
	}

	// Verify runReaper exits cleanly when ctx is canceled
	done := make(chan struct{})
	go func() {
		vs.runReaper(ctx, 50*time.Millisecond, 10*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runReaper did not exit on ctx cancel")
	}
}

// 4. Test Mid-Handshake Race Coalescing & Version Deduplication (lastVersion)
func TestMidHandshakeReconciliationAndDedup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vs := NewViewServer("coalesce-test")
	gateways := newGatewayManager()
	go vs.runPublisher(ctx, gateways)

	// Connect Subscriber A (already up-to-date)
	srvA, cliA := net.Pipe()
	defer cliA.Close()
	go handleSubscription(vs, gateways, srvA)

	_, initViewA := completeHandshake(t, cliA)
	if initViewA.Version != 0 {
		t.Fatalf("expected Subscriber A initial version 0, got %d", initViewA.Version)
	}

	// Now start Subscriber B's handshake, but PAUSE before sending Subscriber B's ACK!
	srvB, cliB := net.Pipe()
	defer cliB.Close()
	go handleSubscription(vs, gateways, srvB)

	typInitB, rawInitB, err := readFrame(cliB)
	if err != nil {
		t.Fatalf("failed reading Subscriber B initial view: %v", err)
	}
	if typInitB != messageView {
		t.Fatalf("expected view frame for Subscriber B, got %s", typInitB)
	}
	var initMsgB struct {
		ID   SubscriberID `json:"id"`
		View View         `json:"view"`
	}
	_ = json.Unmarshal(rawInitB, &initMsgB)

	// WHILE Subscriber B is mid-handshake (at Version 0), register a new backend (Version -> 1)!
	vs.applyRegistration("10.0.0.9:8080", "search")

	// Subscriber A is already registered, so it immediately receives Version 1:
	viewA1 := readViewFrame(t, cliA)
	if viewA1.Version != 1 {
		t.Fatalf("expected Subscriber A to receive Version 1, got %d", viewA1.Version)
	}

	// Now Subscriber B finishes its ACK for Version 0:
	ackB, _ := json.Marshal(Ack{ID: string(initMsgB.ID), Version: initMsgB.View.Version})
	if err := writeFrame(cliB, messageAck, ackB); err != nil {
		t.Fatalf("Subscriber B failed to send ACK: %v", err)
	}

	// Because the current version (1) > initialVersion (0), handleSubscription
	// triggers signalViewChanged().
	// 1) Subscriber B MUST receive Version 1 reconciliation snapshot:
	viewB1 := readViewFrame(t, cliB)
	if viewB1.Version != 1 {
		t.Fatalf("expected Subscriber B reconciled version 1, got %d", viewB1.Version)
	}

	// 2) Subscriber A MUST NOT receive a duplicate Version 1 broadcast (thanks to lastVersion check)!
	_ = cliA.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, _, err := readFrame(cliA); err == nil {
		t.Fatalf("expected Subscriber A to be skipped for duplicate Version 1, but received a frame")
	}
}

// 5. Test Handshake Failure Paths (Bad ACK ID, Bad Version, Timeout)
func TestSubscriptionHandshakeFailures(t *testing.T) {
	vs := NewViewServer("handshake-fail-test")
	gateways := newGatewayManager()

	t.Run("wrong ACK ID", func(t *testing.T) {
		srv, cli := net.Pipe()
		defer cli.Close()
		done := make(chan struct{})
		go func() {
			handleSubscription(vs, gateways, srv)
			close(done)
		}()

		_, raw, _ := readFrame(cli)
		var initMsg struct {
			ID   SubscriberID `json:"id"`
			View View         `json:"view"`
		}
		_ = json.Unmarshal(raw, &initMsg)

		badAck, _ := json.Marshal(Ack{ID: "wrong-id", Version: initMsg.View.Version})
		_ = writeFrame(cli, messageAck, badAck)

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("expected handleSubscription to exit on wrong ACK ID")
		}
	})

	t.Run("wrong ACK Version", func(t *testing.T) {
		srv, cli := net.Pipe()
		defer cli.Close()
		done := make(chan struct{})
		go func() {
			handleSubscription(vs, gateways, srv)
			close(done)
		}()

		_, raw, _ := readFrame(cli)
		var initMsg struct {
			ID   SubscriberID `json:"id"`
			View View         `json:"view"`
		}
		_ = json.Unmarshal(raw, &initMsg)

		badAck, _ := json.Marshal(Ack{ID: string(initMsg.ID), Version: 999})
		_ = writeFrame(cli, messageAck, badAck)

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("expected handleSubscription to exit on wrong ACK Version")
		}
	})

	t.Run("wrong frame type", func(t *testing.T) {
		srv, cli := net.Pipe()
		defer cli.Close()
		done := make(chan struct{})
		go func() {
			handleSubscription(vs, gateways, srv)
			close(done)
		}()

		if _, _, err := readFrame(cli); err != nil {
			t.Fatalf("read initial view: %v", err)
		}
		_ = writeFrame(cli, messageView, []byte("{}"))

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("expected handleSubscription to exit on wrong handshake frame")
		}
	})
}

// 6. High-Concurrency Race Detector Stress Test (run with `go test -race`)
func TestConcurrentStressWithRaceDetector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vs := NewViewServer("stress-test")
	gateways := newGatewayManager()
	go vs.runPublisher(ctx, gateways)
	go vs.runReaper(ctx, 50*time.Millisecond, 10*time.Millisecond)

	// Connect 5 active subscribers continuously reading updates
	var subConns []net.Conn
	for i := 0; i < 5; i++ {
		srv, cli := net.Pipe()
		subConns = append(subConns, cli)
		go handleSubscription(vs, gateways, srv)
		_, _ = completeHandshake(t, cli)

		// Drain updates in background
		go func(c net.Conn) {
			for {
				typ, _, err := readFrame(c)
				if err != nil {
					return
				}
				if typ == messagePing {
					_ = writeFrame(c, messagePong, nil)
				}
			}
		}(cli)
	}
	defer func() {
		for _, c := range subConns {
			c.Close()
		}
	}()

	// Spawn 30 concurrent workers hammering register, heartbeat, deregister, and PrintView
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			addr := net.JoinHostPort("127.0.0.1", "8000")
			inst := vs.applyRegistration(addr, ServiceID("svc"))
			for j := 0; j < 20; j++ {
				_ = vs.recordHeartbeat(inst.ID)
			}
			if workerID%3 == 0 {
				_ = vs.deregisterInstance(inst.ID)
			}
		}(i)
	}

	wg.Wait()
}

// --- gatewayManager unit coverage -----------------------------------------

func TestGatewayManagerLifecycle(t *testing.T) {
	gm := newGatewayManager()
	if gm.len() != 0 {
		t.Fatalf("expected empty gateway manager, got %d", gm.len())
	}

	sub := newSubscriber("gw-1", nil, 0)
	gm.add(sub)
	if gm.len() != 1 {
		t.Fatalf("expected 1 subscriber after add, got %d", gm.len())
	}

	// A subscriber already at the snapshot version must not be written to.
	gm.updateSubscribers(viewSnapshot{version: 0, data: []byte("v0")})
	select {
	case d := <-sub.Updates:
		t.Fatalf("subscriber at version 0 got an unexpected update %q", d)
	default:
	}

	// A newer snapshot must be delivered exactly once.
	gm.updateSubscribers(viewSnapshot{version: 1, data: []byte("v1")})
	select {
	case d := <-sub.Updates:
		if string(d) != "v1" {
			t.Fatalf("expected v1 update, got %q", d)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for v1 update")
	}

	// Re-broadcasting the same version must be a no-op.
	gm.updateSubscribers(viewSnapshot{version: 1, data: []byte("v1-again")})
	select {
	case d := <-sub.Updates:
		t.Fatalf("expected duplicate version to be skipped, got %q", d)
	default:
	}

	if sub.lastVersion.Load() != 1 {
		t.Fatalf("expected lastVersion 1, got %d", sub.lastVersion.Load())
	}

	gm.remove("gw-1")
	if gm.len() != 0 {
		t.Fatalf("expected 0 subscribers after remove, got %d", gm.len())
	}

	// Removing an unknown subscriber and broadcasting to an empty set are safe.
	gm.remove("gw-missing")
	gm.updateSubscribers(viewSnapshot{version: 2, data: []byte("v2")})
}

func TestGatewayManagerLazyMap(t *testing.T) {
	gm := &gatewayManager{}
	gm.add(newSubscriber("gw-lazy", nil, 0))
	if gm.len() != 1 {
		t.Fatalf("expected lazy map initialisation, got %d", gm.len())
	}
}

func TestPushLatestCoalescesUpdates(t *testing.T) {
	sub := newSubscriber("gw-coalesce", nil, 0)
	sub.pushLatest([]byte("first"))
	sub.pushLatest([]byte("second"))

	select {
	case d := <-sub.Updates:
		if string(d) != "second" {
			t.Fatalf("expected coalesced buffer to hold latest value, got %q", d)
		}
	default:
		t.Fatal("expected a buffered update")
	}

	// The buffer holds a single, most-recent snapshot.
	select {
	case d := <-sub.Updates:
		t.Fatalf("expected only one buffered update, got extra %q", d)
	default:
	}
}

// --- ViewServer coverage ---------------------------------------------------

func TestPrintView(t *testing.T) {
	vs := NewViewServer("print-test")
	vs.applyRegistration("10.0.0.1:8080", "orders")

	// Capture PrintView's stdout so the test stays quiet and asserts output.
	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w

	vs.PrintView()

	if err := w.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	os.Stdout = origStdout

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	if !strings.Contains(string(out), "orders") {
		t.Fatalf("expected PrintView output to contain registered service, got %q", out)
	}
}

func TestViewSnapshotPublication(t *testing.T) {
	vs := NewViewServer("snapshot-test")
	if got := vs.Version(); got != 0 {
		t.Fatalf("expected initial version 0, got %d", got)
	}

	snap := vs.latestSnapshot()
	if snap.version != 0 {
		t.Fatalf("expected snapshot version 0, got %d", snap.version)
	}
	if !json.Valid(snap.data) {
		t.Fatalf("expected valid snapshot JSON, got %q", snap.data)
	}

	inst := vs.applyRegistration("10.0.0.1:8080", "orders")
	if got := vs.Version(); got != 1 {
		t.Fatalf("expected version 1 after registration, got %d", got)
	}

	snap = vs.latestSnapshot()
	if snap.version != 1 {
		t.Fatalf("expected snapshot version 1, got %d", snap.version)
	}
	if !strings.Contains(string(snap.data), inst.Endpoint) {
		t.Fatalf("expected snapshot to contain endpoint %q, got %s", inst.Endpoint, snap.data)
	}

	// A duplicate registration updates liveness but must not bump the version
	// or republish a different snapshot.
	vs.applyRegistration("10.0.0.1:8080", "orders")
	if got := vs.Version(); got != 1 {
		t.Fatalf("expected duplicate registration to keep version 1, got %d", got)
	}

	if err := vs.deregisterInstance(inst.ID); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	if got := vs.Version(); got != 2 {
		t.Fatalf("expected version 2 after deregistration, got %d", got)
	}
	if strings.Contains(string(vs.latestSnapshot().data), inst.Endpoint) {
		t.Fatalf("expected deregistered endpoint to be absent from snapshot")
	}
}

func TestSnapshotInitialMessage(t *testing.T) {
	vs := NewViewServer("initial-message-test")
	vs.applyRegistration("10.0.0.1:8080", "orders")

	raw, version, err := vs.snapshotInitialMessage("gateway-1")
	if err != nil {
		t.Fatalf("snapshotInitialMessage: %v", err)
	}
	if version != 1 {
		t.Fatalf("expected version 1, got %d", version)
	}

	var msg struct {
		ID   SubscriberID    `json:"id"`
		View json.RawMessage `json:"view"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("unmarshal initial message: %v", err)
	}
	if msg.ID != "gateway-1" {
		t.Fatalf("expected gateway-1, got %s", msg.ID)
	}
	if !json.Valid(msg.View) {
		t.Fatalf("expected embedded view to be valid JSON, got %q", msg.View)
	}
}

func TestPrintViewEmptySnapshot(t *testing.T) {
	var vs ViewServer

	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w

	vs.PrintView()

	if err := w.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	os.Stdout = origStdout

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	if !strings.Contains(string(out), "error:") {
		t.Fatalf("expected PrintView error output for empty snapshot, got %q", out)
	}
}

func TestReaperLoopEvictsExpired(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vs := NewViewServer("reaper-loop-test")
	inst := vs.applyRegistration("10.0.0.1:8080", "payments")

	// Backdate the heartbeat so the first reaper tick evicts it.
	vs.lastSeen[inst.ID].lastSeen.Store(time.Now().Add(-time.Hour).UnixNano())

	done := make(chan struct{})
	go func() {
		vs.runReaper(ctx, 10*time.Millisecond, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		vs.mu.RLock()
		_, exists := vs.view.Services["payments"][inst.ID]
		vs.mu.RUnlock()
		if !exists {
			break
		}

		select {
		case <-deadline:
			t.Fatal("reaper loop did not evict the expired instance")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runReaper did not exit on ctx cancel")
	}
}

func TestHandleDeregistrationMissingID(t *testing.T) {
	vs := NewViewServer("dereg-missing")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/deregister", nil)

	handleDeregistration(vs)(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing X-Instance-ID, got %d", rec.Code)
	}
}

func TestHandleRegistrationInvalidRemoteAddr(t *testing.T) {
	vs := NewViewServer("register-addr")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(`{"service":"orders","port":9001}`))
	req.RemoteAddr = "not-a-host-port"

	handleRegistration(vs)(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid remote address, got %d", rec.Code)
	}
}

// errResponseWriter fails on Write to exercise handleRegistration's write-error
// logging path.
type errResponseWriter struct {
	header http.Header
}

func (e *errResponseWriter) Header() http.Header {
	if e.header == nil {
		e.header = make(http.Header)
	}
	return e.header
}
func (e *errResponseWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func (e *errResponseWriter) WriteHeader(int)           {}

func TestHandleRegistrationWriteError(t *testing.T) {
	vs := NewViewServer("register-write")
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(`{"service":"orders","port":9001}`))
	req.RemoteAddr = "127.0.0.1:12345"

	// Should not panic even though the response writer rejects the write.
	handleRegistration(vs)(&errResponseWriter{}, req)

	if vs.Version() != 1 {
		t.Fatalf("expected registration to still be applied, got version %d", vs.Version())
	}
}

// --- handleSubscription error-path coverage -------------------------------

func runHandleSubscription(t *testing.T, vs *ViewServer, gm *gatewayManager, conn net.Conn) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		handleSubscription(vs, gm, conn)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleSubscription did not return")
	}
}

func TestHandleSubscriptionConnectionFailures(t *testing.T) {
	t.Run("initial write error", func(t *testing.T) {
		vs := NewViewServer("sub-write-err")
		gm := newGatewayManager()
		runHandleSubscription(t, vs, gm, &stubConn{writeErr: errors.New("write failed")})
		if gm.len() != 0 {
			t.Fatalf("expected no subscriber registered, got %d", gm.len())
		}
	})

	t.Run("set read deadline error", func(t *testing.T) {
		vs := NewViewServer("sub-deadline-err")
		gm := newGatewayManager()
		runHandleSubscription(t, vs, gm, &stubConn{setDeadlineErr: errors.New("deadline failed")})
	})

	t.Run("ack read error", func(t *testing.T) {
		vs := NewViewServer("sub-read-err")
		gm := newGatewayManager()
		runHandleSubscription(t, vs, gm, &stubConn{readErr: io.EOF})
	})

	t.Run("clear deadline error", func(t *testing.T) {
		vs := NewViewServer("sub-clear-err")
		gm := newGatewayManager()
		conn := &stubConn{zeroDeadlineErr: errors.New("clear failed")}
		conn.readBuf.Write(frame(messageAck, []byte(`{"id":"whatever","version":0}`)))
		runHandleSubscription(t, vs, gm, conn)
	})

	t.Run("invalid ack json", func(t *testing.T) {
		vs := NewViewServer("sub-bad-json")
		gm := newGatewayManager()
		conn := &stubConn{}
		conn.readBuf.Write(frame(messageAck, []byte("not json")))
		runHandleSubscription(t, vs, gm, conn)
	})

	t.Run("client closes before ack", func(t *testing.T) {
		vs := NewViewServer("sub-close-early")
		gm := newGatewayManager()
		srv, cli := net.Pipe()
		go func() {
			// Read the initial view then hang up without an ACK.
			_, _, _ = readFrame(cli)
			cli.Close()
		}()
		runHandleSubscription(t, vs, gm, srv)
	})
}

func TestWriteFrameError(t *testing.T) {
	srv, cli := net.Pipe()
	cli.Close()
	defer srv.Close()

	if err := writeFrame(srv, messageView, []byte("payload")); err == nil {
		t.Fatal("expected writeFrame to fail when the peer is closed")
	}
}

func TestSubscriberPingPongKeepsConnection(t *testing.T) {
	prevPing, prevPong := subscriberPingInterval, subscriberPongWait
	subscriberPingInterval = 10 * time.Millisecond
	subscriberPongWait = 250 * time.Millisecond
	defer func() {
		subscriberPingInterval = prevPing
		subscriberPongWait = prevPong
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vs := NewViewServer("ping-test")
	gm := newGatewayManager()
	go vs.runPublisher(ctx, gm)
	srv, cli := net.Pipe()
	defer cli.Close()

	done := make(chan struct{})
	go func() {
		handleSubscription(vs, gm, srv)
		close(done)
	}()

	completeHandshake(t, cli)
	waitFor(t, time.Second, func() bool { return gm.len() == 1 })

	// The server must emit a ping; answer it like a gateway would.
	if err := cli.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	typ, _, err := readFrame(cli)
	if err != nil {
		t.Fatalf("read ping: %v", err)
	}
	if typ != messagePing {
		t.Fatalf("expected ping, got %s", typ)
	}
	if err := writeFrame(cli, messagePong, nil); err != nil {
		t.Fatalf("write pong: %v", err)
	}
	_ = cli.SetReadDeadline(time.Time{})

	// The connection must still deliver a view update after the ping/pong.
	vs.applyRegistration("10.0.0.1:8080", "orders")
	view := readViewFrame(t, cli)
	if view.Version != 1 {
		t.Fatalf("expected version 1 after ping/pong, got %d", view.Version)
	}

	// Stop answering pings: the server should detect the half-open connection.
	waitFor(t, 2*time.Second, func() bool { return gm.len() == 0 })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleSubscription did not exit after pong timeout")
	}
}

func TestGatewayManagerCloseAll(t *testing.T) {
	gm := newGatewayManager()
	srv, cli := net.Pipe()
	defer cli.Close()

	sub := newSubscriber("gw-close", srv, 0)
	gm.add(sub)
	gm.closeAll()

	select {
	case <-sub.done:
	default:
		t.Fatal("expected closeAll to close the subscriber")
	}
}

// --- subscription server coverage -----------------------------------------

func TestServeSubscriptionsHandshakeAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vs := NewViewServer("serve-test")
	gm := newGatewayManager()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- serveSubscriptions(ctx, vs, gm, listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial subscription server: %v", err)
	}

	_, _ = completeHandshake(t, conn)

	// The subscriber should now be tracked by the gateway manager.
	waitFor(t, time.Second, func() bool { return gm.len() == 1 })

	conn.Close()

	// Shutting the context down must close the listener and return cleanly.
	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serveSubscriptions returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serveSubscriptions did not exit on ctx cancel")
	}
}

func TestRunSubscriptionServerOnInvalidAddr(t *testing.T) {
	vs := NewViewServer("bad-addr")
	if err := runSubscriptionServerOn(context.Background(), vs, newGatewayManager(), "256.256.256.256:99999"); err == nil {
		t.Fatal("expected listen error for invalid address")
	}
}

// errorListener returns a transient error once, then reports a closed listener
// so the accept loop's retry path is exercised.
type errorListener struct {
	mu    sync.Mutex
	calls int
}

func (l *errorListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.calls == 1 {
		return nil, errors.New("transient accept failure")
	}
	return nil, net.ErrClosed
}
func (l *errorListener) Close() error   { return nil }
func (l *errorListener) Addr() net.Addr { return stubAddr{} }

func TestServeSubscriptionsRetriesAccept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := serveSubscriptions(ctx, NewViewServer("accept-retry"), newGatewayManager(), &errorListener{})
	if err != nil {
		t.Fatalf("serveSubscriptions returned error on closed listener: %v", err)
	}
}

func TestServerRunAndShutdown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.SubscriptionAddr = "127.0.0.1:0"
	cfg.HeartbeatTTL = 50 * time.Millisecond
	cfg.ReapInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- NewServer(cfg).Run(ctx) }()

	// Give both listeners a moment to bind, then shut everything down.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Server.Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Server.Run did not exit on ctx cancel")
	}
}

func TestServerRunListenError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTPAddr = "256.256.256.256:99999"
	cfg.SubscriptionAddr = "127.0.0.1:0"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := NewServer(cfg).Run(ctx); err == nil {
		t.Fatal("expected Server.Run to return the HTTP listen error")
	}
}

func TestConfigWithDefaults(t *testing.T) {
	got := (Config{}).withDefaults()
	want := DefaultConfig()
	if got != want {
		t.Fatalf("expected defaults %+v, got %+v", want, got)
	}
}

func TestMessageTypeString(t *testing.T) {
	cases := map[messageType]string{
		messageView:     "view",
		messageAck:      "ack",
		messagePing:     "ping",
		messagePong:     "pong",
		messageType(99): "unknown(99)",
	}
	for m, want := range cases {
		if got := m.String(); got != want {
			t.Fatalf("messageType(%d).String() = %q, want %q", m, got, want)
		}
	}
}

func TestZeroValueViewServer(t *testing.T) {
	var vs ViewServer
	if got := vs.latestSnapshot(); got.version != 0 || got.data != nil {
		t.Fatalf("expected empty snapshot, got %+v", got)
	}
	if got := vs.Version(); got != 0 {
		t.Fatalf("expected version 0, got %d", got)
	}
}

func TestPushLatestAfterClose(t *testing.T) {
	sub := newSubscriber("closed", nil, 0)
	sub.close()

	sub.pushLatest([]byte("late"))

	select {
	case <-sub.Updates:
		t.Fatal("expected no update to be buffered after close")
	default:
	}
}

func TestWriteLoopStopsOnClose(t *testing.T) {
	sub := newSubscriber("write-done", nil, 0)

	done := make(chan struct{})
	go func() {
		sub.writeLoop()
		close(done)
	}()

	sub.close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writeLoop did not stop after close")
	}
}

func TestReadLoopClosesOnEOF(t *testing.T) {
	conn := &stubConn{}
	conn.readBuf.Write(frame(messageView, []byte("{}"))) // unexpected frame, then EOF

	sub := newSubscriber("read-loop", conn, 0)

	done := make(chan struct{})
	go func() {
		sub.readLoop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("readLoop did not return on EOF")
	}

	select {
	case <-sub.done:
	default:
		t.Fatal("expected readLoop to close the subscriber")
	}
}

func TestWriteFrameTooLarge(t *testing.T) {
	srv, cli := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	if err := writeFrame(srv, messageView, make([]byte, maxMessageSize)); err == nil {
		t.Fatal("expected writeFrame to reject an oversized payload")
	}
}

func TestUpdateSubscribersSkipsClosed(t *testing.T) {
	gm := newGatewayManager()
	sub := newSubscriber("closed-update", nil, 0)
	gm.add(sub)
	sub.close()

	gm.updateSubscribers(viewSnapshot{version: 1, data: []byte("v1")})

	select {
	case <-sub.Updates:
		t.Fatal("expected closed subscriber to be skipped")
	default:
	}
}

// waitFor polls cond until it is true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
