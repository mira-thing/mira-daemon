package bluetooth

import (
	"encoding/binary"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

// mgmtEvent builds a raw mgmt packet: header {opcode, index, plen} LE + params
func mgmtEvent(opcode uint16, params []byte) []byte {
	buf := make([]byte, 6+len(params))
	binary.LittleEndian.PutUint16(buf[0:2], opcode)
	binary.LittleEndian.PutUint16(buf[2:4], 0) // controller index
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(params)))
	copy(buf[6:], params)
	return buf
}

// disconnectParams: bdaddr is wire-order (reversed), type, reason
func disconnectParams(reversedAddr [6]byte, reason byte) []byte {
	return disconnectParamsTyped(reversedAddr, mgmtAddrBREDR, reason)
}

func disconnectParamsTyped(reversedAddr [6]byte, addrType, reason byte) []byte {
	p := make([]byte, 8)
	copy(p, reversedAddr[:])
	p[6] = addrType
	p[7] = reason
	return p
}

// connectParams: bdaddr, type, then flags(4) + eir_len(2)
func connectParams(reversedAddr [6]byte, addrType byte) []byte {
	p := make([]byte, 13)
	copy(p, reversedAddr[:])
	p[6] = addrType
	return p
}

func TestParseMgmtDisconnect_ValidEvent(t *testing.T) {
	t.Parallel()

	// AA:BB:CC:DD:EE:FF arrives reversed on the wire
	buf := mgmtEvent(mgmtEvDeviceDisconnected,
		disconnectParams([6]byte{0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA}, mgmtReasonRemote))

	opcode, addr, addrType, reason, ok := parseMgmtEvent(buf)
	if !ok {
		t.Fatal("expected ok")
	}
	if opcode != mgmtEvDeviceDisconnected {
		t.Errorf("opcode: got %#x", opcode)
	}
	if isLEAddrType(addrType) {
		t.Error("BR/EDR must not be classified as an LE link")
	}
	if addr != "AA:BB:CC:DD:EE:FF" {
		t.Errorf("addr: got %q", addr)
	}
	if reason != mgmtReasonRemote {
		t.Errorf("reason: got %d", reason)
	}
}

func TestParseMgmtDisconnect_RejectsOtherOpcodesAndShortPackets(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"other opcode":   mgmtEvent(0x0001, disconnectParams([6]byte{}, 1)),
		"short header":   {0x0C, 0x00, 0x00},
		"short params":   mgmtEvent(mgmtEvDeviceDisconnected, []byte{1, 2, 3}),
		"plen oversells": func() []byte { b := mgmtEvent(mgmtEvDeviceDisconnected, disconnectParams([6]byte{}, 1)); return b[:10] }(),
	}
	for name, buf := range cases {
		if _, _, _, _, ok := parseMgmtEvent(buf); ok {
			t.Errorf("%s: expected !ok", name)
		}
	}
}

func newWatcherTestManager(addr string) *Manager {
	m := newTestManager()
	m.SeedKnownDevices([]librespot.BluetoothKnownDevice{
		{Address: addr, LastConnected: time.Now()},
	})
	return m
}

func TestHandleDisconnectReason_RemoteAfterRealUseMarksManual(t *testing.T) {
	t.Parallel()

	m := newWatcherTestManager("AA:BB:CC:DD:EE:FF")
	m.manualMu.Lock()
	m.connectedSince["AA:BB:CC:DD:EE:FF"] = time.Now().Add(-5 * time.Minute)
	m.manualMu.Unlock()
	m.setPanSessionUp("AA:BB:CC:DD:EE:FF")

	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonRemote)

	if !m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("remote disconnect after real use should mark manual")
	}
	if m.panSessionWasUp("AA:BB:CC:DD:EE:FF") {
		t.Fatal("the ACL session ended, the PAN-session flag must reset")
	}
}

func TestHandleDisconnectReason_RemoteWithoutPanSessionKeepsReconnect(t *testing.T) {
	t.Parallel()

	m := newWatcherTestManager("AA:BB:CC:DD:EE:FF")
	m.manualMu.Lock()
	m.connectedSince["AA:BB:CC:DD:EE:FF"] = time.Now().Add(-5 * time.Minute)
	m.manualMu.Unlock()

	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonRemote)

	if m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("remote disconnect with no PAN this session must not mark manual")
	}
}

func TestHandleDisconnectReason_RemoteRightAfterConnectIsProfileChurn(t *testing.T) {
	t.Parallel()

	m := newWatcherTestManager("AA:BB:CC:DD:EE:FF")
	m.manualMu.Lock()
	m.connectedSince["AA:BB:CC:DD:EE:FF"] = time.Now().Add(-2 * time.Second)
	m.manualMu.Unlock()

	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonRemote)

	if m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("remote disconnect seconds after connect must not mark manual (Samsung post-pair teardown)")
	}
}

func TestHandleDisconnectReason_RemoteWithoutConnectTimestampMarksManual(t *testing.T) {
	t.Parallel()

	// device connected before the daemon started
	m := newWatcherTestManager("AA:BB:CC:DD:EE:FF")
	m.setPanSessionUp("AA:BB:CC:DD:EE:FF")
	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonRemote)

	if !m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("remote disconnect with unknown uptime but PAN up should mark manual")
	}
}

func TestHandleDisconnectReason_RemoteAfterPanDropKeepsReconnecting(t *testing.T) {
	t.Parallel()

	m := newWatcherTestManager("AA:BB:CC:DD:EE:FF")
	// tethering-off: the PAN (bnep0) drops, then the phone drops the ACL right
	// after. that's a network change, not a deliberate "stop reconnecting".
	m.markNetworkDrop()
	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonRemote)

	if m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("remote disconnect right after a PAN drop must not suppress auto-reconnect")
	}
}

func TestHandleDisconnectReason_RemoteUnknownDeviceIgnored(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonRemote)

	if m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("devices outside the known list must not be marked")
	}
}

func TestHandleDisconnectReason_TimeoutClearsManualMark(t *testing.T) {
	t.Parallel()

	// not in the known list so the early-page goroutine (which needs D-Bus)
	// doesn't spawn; the clear happens before the known-device gate
	m := newTestManager()
	m.markManualDisconnect("AA:BB:CC:DD:EE:FF")

	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonTimeout)

	if m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("timeout (out of range) must clear a manual mark so reconnect resumes")
	}
}

func TestHandleDisconnectReason_LocalReasonsNoPolicyChange(t *testing.T) {
	t.Parallel()

	m := newWatcherTestManager("AA:BB:CC:DD:EE:FF")
	for _, reason := range []uint8{mgmtReasonUnknown, mgmtReasonLocal, mgmtReasonAuth, mgmtReasonSuspend} {
		m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", reason)
		if m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
			t.Fatalf("reason %d must not mark manual", reason)
		}
	}
}

func TestHandleDisconnectReason_RepeatedAuthFailureEmitsBondLost(t *testing.T) {
	t.Parallel()

	m := newWatcherTestManager("AA:BB:CC:DD:EE:FF")
	var events []string
	m.emit = func(eventType string, payload any) {
		events = append(events, eventType)
	}

	// first auth failure
	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonAuth)
	if len(events) != 0 {
		t.Fatalf("one auth-failure must not emit, got %v", events)
	}
	if m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("one auth-failure must not pause auto-reconnect")
	}

	// second within the window
	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonAuth)
	if len(events) != 1 || events[0] != EventBondLost {
		t.Fatalf("expected [%s], got %v", EventBondLost, events)
	}
	if !m.isManualDisconnect("AA:BB:CC:DD:EE:FF") {
		t.Fatal("bond-lost must pause auto-reconnect (stop hammering the phone)")
	}
}

func TestHandleDisconnectReason_AuthFailureStreakResetByConnect(t *testing.T) {
	t.Parallel()

	m := newWatcherTestManager("AA:BB:CC:DD:EE:FF")
	var events []string
	m.emit = func(eventType string, payload any) {
		events = append(events, eventType)
	}

	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonAuth)
	m.clearAuthFailures("AA:BB:CC:DD:EE:FF") // what a successful connect does
	m.handleDisconnectReason("AA:BB:CC:DD:EE:FF", mgmtReasonAuth)

	if len(events) != 0 {
		t.Fatalf("streak broken by a successful connect must not emit, got %v", events)
	}
}

func TestHandleDisconnectReason_AuthFailureUnknownDeviceIgnored(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	var events []string
	m.emit = func(eventType string, payload any) {
		events = append(events, eventType)
	}

	m.handleDisconnectReason("11:22:33:44:55:66", mgmtReasonAuth)
	m.handleDisconnectReason("11:22:33:44:55:66", mgmtReasonAuth)

	if len(events) != 0 {
		t.Fatalf("auth-failures from unknown devices must not emit, got %v", events)
	}
}

func TestParseMgmtEvent_ConnectedLE(t *testing.T) {
	t.Parallel()

	// connected payload continues with flags(4) + eir_len(2)
	pkt := mgmtEvent(mgmtEvDeviceConnected, connectParams([6]byte{0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA}, mgmtAddrLERandom))
	opcode, addr, addrType, reason, ok := parseMgmtEvent(pkt)
	if !ok {
		t.Fatal("should parse a device-connected event")
	}
	if opcode != mgmtEvDeviceConnected {
		t.Errorf("opcode = %#x", opcode)
	}
	if addr != "AA:BB:CC:DD:EE:FF" {
		t.Errorf("addr = %q", addr)
	}
	if !isLEAddrType(addrType) {
		t.Errorf("addrType %#x should be LE", addrType)
	}
	if reason != 0 {
		t.Errorf("connected events carry no reason, got %#x", reason)
	}
}

func TestParseMgmtEvent_DisconnectedBREDRKeepsReason(t *testing.T) {
	t.Parallel()

	pkt := mgmtEvent(mgmtEvDeviceDisconnected, disconnectParamsTyped([6]byte{0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA}, mgmtAddrBREDR, mgmtReasonRemote))
	opcode, addr, addrType, reason, ok := parseMgmtEvent(pkt)
	if !ok {
		t.Fatal("should parse a device-disconnected event")
	}
	if opcode != mgmtEvDeviceDisconnected || addr != "AA:BB:CC:DD:EE:FF" {
		t.Errorf("opcode=%#x addr=%q", opcode, addr)
	}
	if isLEAddrType(addrType) {
		t.Error("BR/EDR must not be classified as an LE link")
	}
	if reason != mgmtReasonRemote {
		t.Errorf("reason = %#x, want %#x", reason, mgmtReasonRemote)
	}
}

func TestParseMgmtEvent_RejectsShortAndUnrelated(t *testing.T) {
	t.Parallel()

	if _, _, _, _, ok := parseMgmtEvent([]byte{0x0B}); ok {
		t.Error("a truncated packet must not parse")
	}
	// disconnect needs the reason byte, so a 7-byte payload is short
	short := mgmtEvent(mgmtEvDeviceDisconnected, connectParams([6]byte{}, mgmtAddrLEPublic)[:7])
	if _, _, _, _, ok := parseMgmtEvent(short); ok {
		t.Error("a disconnect with no reason byte must not parse")
	}
	if _, _, _, _, ok := parseMgmtEvent(mgmtEvent(0x0001, connectParams([6]byte{}, mgmtAddrBREDR))); ok {
		t.Error("unrelated opcodes must be ignored")
	}
}
