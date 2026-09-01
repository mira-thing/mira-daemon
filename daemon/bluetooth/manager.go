package bluetooth

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/vishvananda/netlink"

	librespot "github.com/devgianlu/go-librespot"
)

// upper bound on any single D-Bus call
const dbusCallTimeout = 5 * time.Second

type Manager struct {
	log                librespot.Logger
	conn               *dbus.Conn
	adapter            dbus.ObjectPath
	agent              *agent
	emit               Emitter
	mu                 sync.Mutex
	pendingDisconnects sync.Map

	// most recent PAN-connected device
	panMu          sync.Mutex
	lastPanAddress string

	// serialize PAN connect/reconnect attempts without blocking
	networkMu sync.Mutex

	// when non-nil, signals an active background loop retrying PAN while offline
	offlineRetryMu   sync.Mutex
	offlineRetryStop chan struct{}

	// assume the user intentionally turned tethering off
	panRetryMu      sync.Mutex
	panRetryHistory []time.Time

	selfTeardownMu sync.Mutex
	selfTeardownAt time.Time

	panBackoffMu     sync.Mutex
	panBackoffUntil  map[string]time.Time
	panBackoffDelay  map[string]time.Duration
	panBackoffBumped map[string]time.Time

	// whether PAN came up at any point during the current ACL session
	panSessionMu sync.Mutex
	panSessionUp map[string]bool

	// prioritized reconnect list
	knownMu               sync.Mutex
	knownDevices          []librespot.BluetoothKnownDevice
	onKnownDevicesChanged func(devs []librespot.BluetoothKnownDevice)

	// manual-disconnect marks + per-device connect timestamps
	manualMu          sync.Mutex
	manualDisconnects map[string]time.Time
	connectedSince    map[string]time.Time
	connectedNow      map[string]bool

	// peers holding an LE link, keyed by address, from the mgmt socket
	leLinksMu sync.Mutex
	leLinks   map[string]bool

	// most recent bnep0 (PAN) drop
	networkDropMu     sync.Mutex
	lastNetworkDropAt time.Time

	// gates auto-PAN so a newly connected device cant take over a working connection
	onlineMu sync.Mutex
	online   bool

	// prevents overlapping Device1.Connect calls
	activeReconnectMu       sync.Mutex
	activeReconnectInFlight bool

	recoveryMu    sync.Mutex
	authFailCount map[string]int
	authFailLast  map[string]time.Time
	sdpBounceAt   map[string]time.Time

	// volume key peripheral BLE
	hid *hidVolume

	// iAP2 sidecar for ios volume
	iap2         *iap2Volume
	iap2WatchMu  sync.Mutex
	iap2Watching map[string]bool
}

const (
	panRetryThreshold = 3
	panRetryWindow    = 30 * time.Second

	// a connection older than this is "stable"
	panStableAfter        = 60 * time.Second
	panStableRetryAllowed = 1
)

func NewManager(log librespot.Logger, emit Emitter) (*Manager, error) {
	conn, err := dbus.SystemBus()
	if err != nil {
		return nil, fmt.Errorf("connect to system bus: %w", err)
	}
	log.Info("bluetooth: connected to system bus")

	// daemon may race ahead of BlueZ on cold boot
	var adapter dbus.ObjectPath
	for attempt := 1; attempt <= 10; attempt++ {
		adapter, err = findDefaultAdapter(conn)
		if err == nil {
			break
		}
		log.Debugf("bluetooth: adapter not ready (attempt %d/10): %v", attempt, err)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("find bluetooth adapter: %w", err)
	}
	log.Infof("bluetooth: using adapter %s", adapter)

	m := &Manager{
		log:               log,
		conn:              conn,
		adapter:           adapter,
		emit:              emit,
		manualDisconnects: make(map[string]time.Time),
		connectedSince:    make(map[string]time.Time),
		connectedNow:      make(map[string]bool),
		panBackoffUntil:   make(map[string]time.Time),
		panBackoffDelay:   make(map[string]time.Duration),
		panBackoffBumped:  make(map[string]time.Time),
		panSessionUp:      make(map[string]bool),
		authFailCount:     make(map[string]int),
		authFailLast:      make(map[string]time.Time),
		sdpBounceAt:       make(map[string]time.Time),
	}

	a, err := newAgent(log, conn, m)
	if err != nil {
		return nil, fmt.Errorf("register bluetooth agent: %w", err)
	}
	m.agent = a

	if err := m.setPower(true); err != nil {
		return nil, fmt.Errorf("power on adapter: %w", err)
	}

	// ios volume path
	m.iap2 = newIap2Volume(log)

	m.monitorDisconnects()

	var connectedPaired []string
	for attempt := 1; ; attempt++ {
		devs, err := m.GetDevices()
		if err != nil {
			if attempt < 3 {
				time.Sleep(time.Second)
				continue
			}
			m.log.WithError(err).Warn("bluetooth: startup device snapshot failed, no watchdogs armed for already-connected phones")
			break
		}
		for _, d := range devs {
			if !d.Connected {
				continue
			}
			addr := d.Address
			m.manualMu.Lock()
			m.connectedNow[addr] = true
			m.connectedSince[addr] = time.Now()
			m.manualMu.Unlock()
			m.log.Debugf("bluetooth: %s already connected at startup", addr)
			if d.Paired {
				connectedPaired = append(connectedPaired, addr)
				go m.ensureIap2Session(addr)
			}
		}
		break
	}

	// BLE HID volume keys register here
	if hv, err := newHIDVolume(log, conn, adapter, m.adapterAlias()); err != nil {
		log.WithError(err).Warn("bluetooth: hid: volume key service unavailable")
	} else {
		m.hid = hv
		hv.leLinkUp = m.anyLEConnected
		hv.start(m.sweepHIDWatchdogs)
		for _, addr := range connectedPaired {
			m.armHIDWatchdog(addr)
		}
	}

	m.monitorNetworkInterfaces()

	go m.routeArbiterLoop()

	// disconnect reasons let us tell a deliberate phone-side disconnect from a dropout
	if err := m.watchMgmtDisconnects(); err != nil {
		m.log.WithError(err).Warn("bluetooth: mgmt watcher unavailable, deliberate disconnects will look like dropouts")
	}

	return m, nil
}

func findDefaultAdapter(conn *dbus.Conn) (dbus.ObjectPath, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dbusCallTimeout)
	defer cancel()

	var owner string
	obj := conn.Object("org.freedesktop.DBus", "/org/freedesktop/DBus")
	if err := obj.CallWithContext(ctx, "org.freedesktop.DBus.GetNameOwner", 0, "org.bluez").Store(&owner); err != nil {
		return "", fmt.Errorf("get bluez owner: %w", err)
	}

	var objects map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	obj = conn.Object("org.bluez", "/")
	if err := obj.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0).Store(&objects); err != nil {
		return "", fmt.Errorf("get managed objects: %w", err)
	}

	for path, interfaces := range objects {
		if _, hasAdapter := interfaces[bluezAdapterInterface]; hasAdapter {
			return path, nil
		}
	}

	return "", fmt.Errorf("no bluetooth adapter found")
}

func (m *Manager) monitorDisconnects() {
	if err := m.conn.AddMatchSignal(
		dbus.WithMatchInterface("org.freedesktop.DBus.Properties"),
		dbus.WithMatchMember("PropertiesChanged"),
		dbus.WithMatchPathNamespace("/org/bluez"),
	); err != nil {
		m.log.WithError(err).Error("bluetooth: failed to subscribe to property changes")
		return
	}

	signals := make(chan *dbus.Signal, 10)
	m.conn.Signal(signals)

	go func() {
		for signal := range signals {
			if signal.Name != "org.freedesktop.DBus.Properties.PropertiesChanged" {
				continue
			}
			if len(signal.Body) < 3 {
				continue
			}

			iface, _ := signal.Body[0].(string)
			if iface != bluezDeviceInterface {
				continue
			}

			changes, _ := signal.Body[1].(map[string]dbus.Variant)

			devicePath := string(signal.Path)
			address := strings.TrimPrefix(devicePath, string(m.adapter)+"/dev_")
			address = strings.ReplaceAll(address, "_", ":")

			if pairedV, ok := changes["Paired"]; ok {
				if paired, _ := pairedV.Value().(bool); paired {
					m.handleDevicePaired(devicePath, address)
				}
			}

			if connectedV, ok := changes["Connected"]; ok {
				connected, _ := connectedV.Value().(bool)
				if connected {
					m.handleDeviceConnected(devicePath, address)
				} else {
					m.handleDeviceDisconnected(devicePath, address)
				}
			}
		}
	}()
}

// emits EventPaired once per pair
func (m *Manager) handleDevicePaired(devicePath, address string) {
	m.log.Infof("bluetooth: device paired: %s", devicePath)

	// clear pending pair so a stale Cancel() doesn't fire later
	if m.agent != nil {
		m.agent.clearCurrentIfDevice(devicePath)
	}

	info, err := m.GetDeviceInfo(address)
	if err != nil {
		m.log.WithError(err).Debugf("bluetooth: failed to enrich paired event for %s", address)
		info = &DeviceInfo{Address: address, Paired: true}
	}

	// list the device immediately
	name := info.Alias
	if name == "" {
		name = info.Name
	}
	m.recordPairedDevice(address, name)

	m.clearAuthFailures(address)
	m.clearManualDisconnect(address)

	m.manualMu.Lock()
	if !m.connectedNow[address] {
		m.connectedNow[address] = true
		m.connectedSince[address] = time.Now()
	}
	m.manualMu.Unlock()

	if m.hid != nil {
		m.armHIDWatchdog(address)
	}

	go m.ensureIap2Session(address)

	if m.emit != nil {
		m.emit(EventPaired, DevicePairedPayload{Device: info})
	}
}

// fires on Connected:true
func (m *Manager) handleDeviceConnected(devicePath, address string) {
	m.log.Infof("bluetooth: device connected: %s", devicePath)

	// the device coming back always re-arms auto-reconnect
	m.manualMu.Lock()
	delete(m.manualDisconnects, address)
	m.connectedSince[address] = time.Now()
	m.connectedNow[address] = true
	m.manualMu.Unlock()
	m.clearAuthFailures(address)

	info, err := m.GetDeviceInfo(address)
	if err != nil {
		m.log.WithError(err).Debugf("bluetooth: failed to enrich connect event for %s", address)
		if m.emit != nil {
			m.emit(EventConnect, DeviceConnectedPayload{Address: address})
		}
	} else if m.emit != nil {
		m.emit(EventConnect, DeviceConnectedPayload{Device: info})
	}

	// only chase PAN for paired devices, random discovering peripherals shouldn't trigger Connect
	if info == nil || !info.Paired {
		return
	}

	if m.hid != nil {
		// catches bonded hosts that connect but never subscribe to volume keys
		m.armHIDWatchdog(address)
	}

	// ios controls volume over iap2
	go m.ensureIap2Session(address)

	// if we are already online, a newly connected device must not take over
	m.panMu.Lock()
	activePan := m.lastPanAddress
	m.panMu.Unlock()
	if activePan != address {
		if m.IsOnline() {
			m.log.Infof("bluetooth: %s connected while already online, leaving the network untouched", address)
			return
		}
		if activePan != "" && m.NetworkUp() {
			m.log.Infof("bluetooth: %s connected but PAN already active via %s, skipping auto-PAN", address, activePan)
			return
		}
	}

	// gate auto-PAN on the peer actually advertising NAP
	// iPhone doesn't publish NAP UUID until Personal Hotspot is on so we wait 20s with one Device1.Connect nudge
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		available, err := m.waitForNAPService(ctx, address)
		if err != nil {
			m.log.WithError(err).Debugf("bluetooth: NAP wait aborted for %s", address)
			return
		}
		if !available {
			m.log.Warnf("bluetooth: %s never advertised PAN-NAP, peer likely has tethering disabled", address)
			if m.emit != nil {
				m.emit(EventNAPUnavailable, NetworkConnectedPayload{Address: address})
			}
			// a pairing created while the hotspot was not up
			m.trySdpRefreshBounce(address)
			return
		}

		if err := m.ConnectNetwork(address); err != nil {
			m.log.WithError(err).Warnf("bluetooth: auto-PAN failed for %s after NAP advertised", address)
			return
		}
		m.log.Debugf("bluetooth: auto-PAN succeeded for %s", address)
	}()
}

// polls Device1.UUIDs for the NAP UUID
func (m *Manager) waitForNAPService(ctx context.Context, address string) (bool, error) {
	devicePath := formatDevicePath(m.adapter, address)
	obj := m.conn.Object(bluezBusName, devicePath)

	check := func() (hasNAP bool, resolved bool, connected bool, err error) {
		props := make(map[string]dbus.Variant)
		if err := m.dbusCall(obj, "org.freedesktop.DBus.Properties.GetAll", bluezDeviceInterface).Store(&props); err != nil {
			return false, false, false, err
		}
		if v, ok := props["Connected"]; ok {
			connected, _ = v.Value().(bool)
		}
		if v, ok := props["ServicesResolved"]; ok {
			resolved, _ = v.Value().(bool)
		}
		if v, ok := props["UUIDs"]; ok {
			if uuids, ok := v.Value().([]string); ok {
				for _, u := range uuids {
					if strings.EqualFold(u, panNAPUUID) {
						hasNAP = true
						break
					}
				}
			}
		}
		return hasNAP, resolved, connected, nil
	}

	nudged := false
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		hasNAP, resolved, connected, err := check()
		if err != nil {
			return false, err
		}
		if !connected {
			return false, fmt.Errorf("device %s disconnected while waiting for NAP", address)
		}
		if hasNAP {
			return true, nil
		}
		if resolved && !nudged {
			nudged = true
			m.log.Debugf("bluetooth: %s ServicesResolved=true but no NAP yet, nudging via Device1.Connect", address)
			if err := m.dbusCall(obj, bluezDeviceInterface+".Connect").Err; err != nil {
				m.log.WithError(err).Debugf("bluetooth: Device1.Connect nudge failed for %s (continuing to poll)", address)
			}
		}

		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
		}
	}
}

// watches a connected phone for the iAP2 service and starts a volume session
func (m *Manager) ensureIap2Session(address string) {
	if m.iap2 == nil {
		return
	}
	m.iap2WatchMu.Lock()
	if m.iap2Watching == nil {
		m.iap2Watching = make(map[string]bool)
	}
	if m.iap2Watching[address] {
		m.iap2WatchMu.Unlock()
		return
	}
	m.iap2Watching[address] = true
	m.iap2WatchMu.Unlock()
	defer func() {
		m.iap2WatchMu.Lock()
		delete(m.iap2Watching, address)
		m.iap2WatchMu.Unlock()
	}()

	const rounds = 8
	for i := 1; i <= rounds; i++ {
		found, connected := m.waitForServiceUUID(address, iap2ServiceUUID, 15*time.Second)
		if found {
			m.log.Infof("bluetooth: %s advertises iAP2, establishing volume session", address)
			m.iap2.EnsureSession(address)
			return
		}
		if !connected {
			m.log.Debugf("bluetooth: %s disconnected before advertising iAP2", address)
			return
		}
		if i < rounds {
			time.Sleep(30 * time.Second)
		}
	}
	m.log.Infof("bluetooth: %s never advertised iAP2, no iPhone volume session for this connection", address)
}

// polls until the device service list carries the UUID, the device disconnects,
// or the timeout passes
func (m *Manager) waitForServiceUUID(address, uuid string, timeout time.Duration) (bool, bool) {
	devicePath := formatDevicePath(m.adapter, address)
	obj := m.conn.Object(bluezBusName, devicePath)

	deadline := time.Now().Add(timeout)
	errStreak := 0
	for time.Now().Before(deadline) {
		props := make(map[string]dbus.Variant)
		if err := m.dbusCall(obj, "org.freedesktop.DBus.Properties.GetAll", bluezDeviceInterface).Store(&props); err != nil {
			errStreak++
			if errStreak >= 5 {
				m.log.WithError(err).Debugf("bluetooth: %s service lookup failing, giving up uuid wait", address)
				return false, false
			}
			time.Sleep(time.Second)
			continue
		}
		errStreak = 0
		if v, ok := props["Connected"]; ok {
			if connected, _ := v.Value().(bool); !connected {
				return false, false
			}
		}
		if v, ok := props["UUIDs"]; ok {
			if uuids, ok := v.Value().([]string); ok {
				for _, u := range uuids {
					if strings.EqualFold(u, uuid) {
						return true, true
					}
				}
			}
		}
		time.Sleep(time.Second)
	}
	return false, true
}

// disconnects and reconnects the device once to retrigger the pan connection
func (m *Manager) trySdpRefreshBounce(address string) {
	const sdpBounceCooldown = 3 * time.Minute

	m.recoveryMu.Lock()
	if m.sdpBounceAt == nil {
		m.sdpBounceAt = make(map[string]time.Time)
	}
	if last, ok := m.sdpBounceAt[address]; ok && time.Since(last) < sdpBounceCooldown {
		m.recoveryMu.Unlock()
		return
	}
	m.sdpBounceAt[address] = time.Now()
	m.recoveryMu.Unlock()

	m.log.Infof("bluetooth: bouncing %s once to refresh its service record (stale-SDP recovery)", address)

	devicePath := formatDevicePath(m.adapter, address)
	obj := m.conn.Object(bluezBusName, devicePath)

	m.pendingDisconnects.Store(address, true)
	if err := m.dbusCall(obj, bluezDeviceInterface+".Disconnect").Err; err != nil {
		m.pendingDisconnects.Delete(address)
		m.log.WithError(err).Debugf("bluetooth: SDP-refresh disconnect failed for %s", address)
		return
	}
	time.Sleep(2 * time.Second)
	if err := m.dbusCall(obj, bluezDeviceInterface+".Connect").Err; err != nil {
		m.log.WithError(err).Debugf("bluetooth: SDP-refresh reconnect failed for %s (device will auto-reconnect)", address)
	}
}

func (m *Manager) handleDeviceDisconnected(devicePath, address string) {
	if _, pending := m.pendingDisconnects.LoadAndDelete(address); !pending {
		if m.emit != nil {
			m.emit(EventDisconnect, DeviceDisconnectedPayload{Address: address})
		}
	}

	m.log.Infof("bluetooth: device disconnected: %s", devicePath)

	m.manualMu.Lock()
	delete(m.connectedNow, address)
	m.manualMu.Unlock()

	// tear down the iAP2 volume session
	m.iap2.DropSession(address)

	if m.agent != nil {
		m.agent.clearCurrentIfDevice(devicePath)
	}
}

func (m *Manager) monitorNetworkInterfaces() {
	linkUpdates := make(chan netlink.LinkUpdate)
	done := make(chan struct{})

	if err := netlink.LinkSubscribe(linkUpdates, done); err != nil {
		m.log.WithError(err).Error("bluetooth: failed to subscribe to netlink updates")
		return
	}

	go func() {
		for update := range linkUpdates {
			if update.Header.Type == syscall.RTM_DELLINK && update.Link.Attrs().Name == panInterface {
				if m.consumeSelfTeardown() {
					m.log.Debug("bluetooth: bnep0 removed by our own forced reconnect, not counting as a drop")
					continue
				}
				m.log.Info("bluetooth: bnep0 interface removed")
				m.markNetworkDrop()
				if m.emit != nil {
					m.emit(EventNetworkDisconnect, nil)
				}
				m.tryRecoverPan()
			}
		}
	}()
}

// tries to re-establish PAN if the BT link is still up
func (m *Manager) tryRecoverPan() {
	m.panMu.Lock()
	addr := m.lastPanAddress
	m.panMu.Unlock()
	if addr == "" {
		return
	}

	go func() {
		// phones tear BNEP down first and drop the ACL a beat later
		time.Sleep(3 * time.Second)
		if m.isManualDisconnect(addr) {
			m.log.Debugf("bluetooth: PAN auto-recover skipped for %s (manual disconnect)", addr)
			return
		}
		if m.inPanBackoff(addr) {
			m.log.Debugf("bluetooth: PAN auto-recover skipped for %s (flap backoff)", addr)
			return
		}
		info, err := m.GetDeviceInfo(addr)
		if err != nil {
			m.log.WithError(err).Debugf("bluetooth: PAN auto-recover skipped for %s (no device info)", addr)
			return
		}
		if !info.Connected {
			m.log.Debugf("bluetooth: PAN auto-recover skipped for %s (BT link is down too)", addr)
			return
		}

		// on a stable session only one recovery per window
		threshold := panRetryThreshold
		if uptime, ok := m.connectionUptime(addr); ok && uptime > panStableAfter {
			threshold = panStableRetryAllowed
		}
		// breaker ticks only for drops we actually try to recover
		allowed, recentDrops := m.shouldRetryPanThreshold(time.Now(), threshold)
		if !allowed {
			if threshold == panStableRetryAllowed {
				delay := m.bumpPanBackoff(addr)
				m.log.Warnf("bluetooth: PAN to %s dropped again within %s of recovering, pausing auto-reconnect for %s (escalates while it keeps flapping)",
					addr, panRetryWindow, delay)
			} else {
				m.log.Warnf("bluetooth: PAN dropped %d times in %s, backing off auto-recover (assuming intentional disconnect)",
					recentDrops+1, panRetryWindow)
			}
			return
		}
		m.log.Infof("bluetooth: bnep0 dropped while %s still BT-connected, retrying PAN", addr)
		if err := m.ConnectNetwork(addr); err != nil {
			m.log.WithError(err).Warn("bluetooth: PAN auto-recover failed")
		}
	}()
}

func (m *Manager) shouldRetryPan(now time.Time) (allowed bool, recentDrops int) {
	return m.shouldRetryPanThreshold(now, panRetryThreshold)
}

func (m *Manager) shouldRetryPanThreshold(now time.Time, threshold int) (allowed bool, recentDrops int) {
	m.panRetryMu.Lock()
	defer m.panRetryMu.Unlock()

	// prune via slice[:0] trick, no alloc per call
	cutoff := now.Add(-panRetryWindow)
	fresh := m.panRetryHistory[:0]
	for _, t := range m.panRetryHistory {
		if t.After(cutoff) {
			fresh = append(fresh, t)
		}
	}
	m.panRetryHistory = fresh

	if len(m.panRetryHistory) >= threshold {
		return false, len(m.panRetryHistory)
	}
	m.panRetryHistory = append(m.panRetryHistory, now)
	return true, len(m.panRetryHistory) - 1
}

const (
	panBackoffMin        = 30 * time.Second
	panBackoffMax        = 5 * time.Minute
	panBackoffDecayAfter = 10 * time.Minute
)

func (m *Manager) bumpPanBackoff(address string) time.Duration {
	m.panBackoffMu.Lock()
	defer m.panBackoffMu.Unlock()
	if m.panBackoffUntil == nil {
		m.panBackoffUntil = make(map[string]time.Time)
		m.panBackoffDelay = make(map[string]time.Duration)
		m.panBackoffBumped = make(map[string]time.Time)
	}
	if t, ok := m.panBackoffBumped[address]; ok && time.Since(t) > panBackoffDecayAfter {
		delete(m.panBackoffDelay, address)
	}
	d := m.panBackoffDelay[address] * 2
	if d < panBackoffMin {
		d = panBackoffMin
	}
	if d > panBackoffMax {
		d = panBackoffMax
	}
	m.panBackoffDelay[address] = d
	m.panBackoffUntil[address] = time.Now().Add(d)
	m.panBackoffBumped[address] = time.Now()
	return d
}

func (m *Manager) inPanBackoff(address string) bool {
	m.panBackoffMu.Lock()
	defer m.panBackoffMu.Unlock()
	return time.Now().Before(m.panBackoffUntil[address])
}

func (m *Manager) clearPanBackoffPause(address string) {
	m.panBackoffMu.Lock()
	delete(m.panBackoffUntil, address)
	m.panBackoffMu.Unlock()
}

func (m *Manager) clearPanBackoff(address string) {
	m.panBackoffMu.Lock()
	delete(m.panBackoffUntil, address)
	delete(m.panBackoffDelay, address)
	delete(m.panBackoffBumped, address)
	m.panBackoffMu.Unlock()
}

func (m *Manager) noteSelfTeardown() {
	m.selfTeardownMu.Lock()
	m.selfTeardownAt = time.Now()
	m.selfTeardownMu.Unlock()
}

func (m *Manager) consumeSelfTeardown() bool {
	m.selfTeardownMu.Lock()
	defer m.selfTeardownMu.Unlock()
	if m.selfTeardownAt.IsZero() || time.Since(m.selfTeardownAt) >= 5*time.Second {
		return false
	}
	m.selfTeardownAt = time.Time{}
	return true
}

func (m *Manager) setPanSessionUp(address string) {
	m.panSessionMu.Lock()
	if m.panSessionUp == nil {
		m.panSessionUp = make(map[string]bool)
	}
	m.panSessionUp[address] = true
	m.panSessionMu.Unlock()
}

func (m *Manager) panSessionWasUp(address string) bool {
	m.panSessionMu.Lock()
	defer m.panSessionMu.Unlock()
	return m.panSessionUp[address]
}

func (m *Manager) clearPanSession(address string) {
	m.panSessionMu.Lock()
	delete(m.panSessionUp, address)
	m.panSessionMu.Unlock()
}

const (
	offlineRetryInterval     = 15 * time.Second
	offlineRetryFastAttempts = 20
	offlineRetrySlowInterval = 1 * time.Minute
)

func (m *Manager) SetOfflineRetry(active bool) {
	m.offlineRetryMu.Lock()
	defer m.offlineRetryMu.Unlock()

	if active {
		if m.offlineRetryStop != nil {
			return // already running
		}
		stop := make(chan struct{})
		m.offlineRetryStop = stop
		go m.offlineRetryLoop(stop)
		m.log.Debug("bluetooth: offline retry loop started")
		return
	}

	if m.offlineRetryStop != nil {
		close(m.offlineRetryStop)
		m.offlineRetryStop = nil
		m.log.Debug("bluetooth: offline retry loop stopped")
	}
}

func (m *Manager) offlineRetryLoop(stop <-chan struct{}) {
	defer func() {
		m.offlineRetryMu.Lock()
		if m.offlineRetryStop != nil && (<-chan struct{})(m.offlineRetryStop) == stop {
			m.offlineRetryStop = nil
		}
		m.offlineRetryMu.Unlock()
	}()

	// first attempt immediately, don't wait a full 15s on cold boot
	m.runOfflineRetryAttempt(1)

	ticker := time.NewTicker(offlineRetryInterval)
	defer ticker.Stop()

	attempts := 1
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			attempts++
			// after the fast window assume the peer is genuinely away and slow down
			if attempts == offlineRetryFastAttempts {
				m.log.Infof("bluetooth: offline retry slowing to every %s after %d attempts", offlineRetrySlowInterval, attempts)
				ticker.Reset(offlineRetrySlowInterval)
			}
			m.runOfflineRetryAttempt(attempts)
		}
	}
}

func (m *Manager) runOfflineRetryAttempt(attempt int) {
	candidates := m.reconnectCandidates()
	if len(candidates) == 0 {
		if attempt%20 == 1 {
			m.log.Infof("bluetooth: offline retry skipped, no reconnect candidates (known devices are manual-marked or in flap backoff)")
		} else {
			m.log.Tracef("bluetooth: offline retry skipped, no reconnect candidates")
		}
		return
	}

	// any candidate already ACL-connected wins
	var disconnected []string
	for _, addr := range candidates {
		info, err := m.GetDeviceInfo(addr)
		if err != nil {
			m.log.WithError(err).Tracef("bluetooth: offline retry skipped %s (no device info)", addr)
			continue
		}
		if info.Connected {
			if m.NetworkUp() {
				// bnep0 up but offline = zombie PAN
				m.log.Infof("bluetooth: offline retry %d, %s bnep0 up but offline (zombie), forcing PAN reconnect", attempt, addr)
				if err := m.ConnectNetworkForced(addr); err != nil {
					m.log.WithError(err).Debugf("bluetooth: offline retry %d forced reconnect failed", attempt)
				}
			} else {
				// bnep0 down = tethering off, or a clean PAN drop
				m.log.Infof("bluetooth: offline retry %d, %s ACL up + bnep0 down, attempting PAN connect", attempt, addr)
				if err := m.ConnectNetwork(addr); err != nil {
					m.log.Debugf("bluetooth: offline retry %d, PAN not available for %s yet (tethering off?): %v", attempt, addr, err)
				}
			}
			return
		}
		disconnected = append(disconnected, addr)
	}

	if len(disconnected) == 0 {
		return
	}
	m.log.Debugf("bluetooth: offline retry attempt %d, no candidate BT-connected, actively paging %d device(s)", attempt, len(disconnected))
	m.tryActiveReconnect(disconnected)
}

func (m *Manager) tryActiveReconnect(addrs []string) {
	if len(addrs) == 0 {
		return
	}

	m.activeReconnectMu.Lock()
	if m.activeReconnectInFlight {
		m.activeReconnectMu.Unlock()
		m.log.Tracef("bluetooth: active reconnect skipped, previous attempt still in flight")
		return
	}
	m.activeReconnectInFlight = true
	m.activeReconnectMu.Unlock()

	go func() {
		defer func() {
			m.activeReconnectMu.Lock()
			m.activeReconnectInFlight = false
			m.activeReconnectMu.Unlock()
		}()

		for _, addr := range addrs {
			// marks can land while we paging a higher priority device
			if m.isManualDisconnect(addr) || m.inPanBackoff(addr) {
				continue
			}

			// bounded so one absent device cant eat the whole attempt
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

			devicePath := formatDevicePath(m.adapter, addr)
			obj := m.conn.Object(bluezBusName, devicePath)

			m.log.Infof("bluetooth: actively paging %s for reconnect", addr)
			err := obj.CallWithContext(ctx, bluezDeviceInterface+".Connect", 0).Err
			cancel()
			if err != nil {
				m.log.WithError(err).Debugf("bluetooth: active reconnect to %s failed (peer likely out of range or BT off)", addr)
				continue
			}
			m.log.Infof("bluetooth: active reconnect to %s succeeded, PAN follows via signal handler", addr)
			return
		}
	}()
}

// RecoverNetworkAfterResume does a safe PAN recovery after the device wakes from sleep
func (m *Manager) RecoverNetworkAfterResume(addr string) {
	// waking up = the user is back
	m.ClearManualDisconnects()

	if addr == "" {
		m.panMu.Lock()
		addr = m.lastPanAddress
		m.panMu.Unlock()
	}
	if addr == "" {
		addr = m.topReconnectCandidate()
	}
	if addr == "" {
		m.log.Debug("bluetooth: resume recovery skipped, no prior PAN address")
		return
	}

	info, err := m.GetDeviceInfo(addr)
	if err != nil {
		m.log.WithError(err).Debugf("bluetooth: resume recovery skipped for %s (no device info)", addr)
		return
	}

	if !info.Connected {
		m.log.Infof("bluetooth: resume recovery, %s not BT-connected, paging", addr)
		m.tryActiveReconnect(m.reconnectCandidates())
		return
	}

	if !m.NetworkUp() {
		m.log.Infof("bluetooth: resume recovery, ACL up but PAN down, reconnecting PAN to %s", addr)
		if err := m.ConnectNetworkForced(addr); err != nil {
			m.log.WithError(err).Debugf("bluetooth: resume PAN reconnect failed")
		}
		return
	}

	m.log.Debugf("bluetooth: resume recovery, %s already connected with PAN up", addr)
}

// invokes a D-Bus method with dbusCallTimeout
// returns the adapters user visible name
func (m *Manager) adapterAlias() string {
	obj := m.conn.Object(bluezBusName, m.adapter)
	var v dbus.Variant
	if err := m.dbusCall(obj, "org.freedesktop.DBus.Properties.Get", bluezAdapterInterface, "Alias").Store(&v); err == nil {
		if alias, ok := v.Value().(string); ok && alias != "" {
			return alias
		}
	}
	return "Mira"
}

// records an LE transport connect or disconnect seen on the mgmt socket
func (m *Manager) noteLELink(address string, up bool) {
	m.leLinksMu.Lock()
	if m.leLinks == nil {
		m.leLinks = make(map[string]bool)
	}
	if up {
		m.leLinks[address] = true
	} else {
		delete(m.leLinks, address)
	}
	remaining := len(m.leLinks)
	m.leLinksMu.Unlock()

	state := "down"
	if up {
		state = "up"
	}
	m.log.Debugf("bluetooth: le link %s %s (%d still up)", address, state, remaining)

	if !up && remaining == 0 && m.hid != nil {
		go m.hid.forceAdvRefresh()
	}
}

// reports whether any peer holds an LE link
func (m *Manager) anyLEConnected() bool {
	m.leLinksMu.Lock()
	defer m.leLinksMu.Unlock()
	return len(m.leLinks) > 0
}

// queues a signed volume key event
func (m *Manager) SendHIDVolumeSteps(steps int) bool {
	if m == nil || m.hid == nil {
		return false
	}
	return m.hid.sendSteps(steps)
}

// reports the iAP2 session state
func (m *Manager) Iap2Status() (state, lastErr string, present bool) {
	if m == nil {
		return "unavailable", "", false
	}
	return m.iap2.Status()
}

// routes a volume event to whichever phone volume path is live
func (m *Manager) SendPhoneVolumeSteps(steps int) bool {
	if m == nil {
		return false
	}
	if m.iap2.SendVolumeSteps(steps) {
		return true
	}
	return m.SendHIDVolumeSteps(steps)
}

// reports whether the HID service is registered with bluez
func (m *Manager) HIDVolumeStatus() (registered, subscribed, subDead bool) {
	if m == nil || m.hid == nil {
		return false, false, false
	}
	h := m.hid
	h.regMu.Lock()
	registered = h.appRegistered && h.advRegistered
	subDead = h.subDead
	h.regMu.Unlock()
	return registered, h.input.isNotifying(), subDead
}

// counts volume reports emitted since start
func (m *Manager) HIDVolumeSent() int {
	if m == nil || m.hid == nil {
		return 0
	}
	return m.hid.sentCount()
}

// explains the advertisement state for the debug screen
func (m *Manager) HIDVolumeAdvState() string {
	if m == nil || m.hid == nil {
		return "unavailable"
	}
	return m.hid.advState()
}

// reports whether the address currently holds an ACL link
func (m *Manager) isConnectedNow(address string) bool {
	m.manualMu.Lock()
	defer m.manualMu.Unlock()
	return m.connectedNow[address]
}

// starts the bonded but not subscribed check for a connected phone
func (m *Manager) armHIDWatchdog(address string) {
	if m.hid == nil {
		return
	}
	m.hid.armSubscribeWatchdog(address, func() bool {
		if m.isConnectedNow(address) {
			return true
		}
		info, err := m.GetDeviceInfo(address)
		return err == nil && info.Connected
	})
}

// releases bluez registrations
func (m *Manager) Close() {
	m.iap2.Close()
	if m.hid != nil {
		m.hid.close()
	}
}

func (m *Manager) dbusCall(obj dbus.BusObject, method string, args ...any) *dbus.Call {
	ctx, cancel := context.WithTimeout(context.Background(), dbusCallTimeout)
	defer cancel()

	m.log.Tracef("dbus call: %s on %s", method, obj.Path())
	c := obj.CallWithContext(ctx, method, 0, args...)
	// log failures at trace
	if c.Err != nil {
		m.log.WithError(c.Err).Tracef("dbus call FAILED: %s on %s", method, obj.Path())
	} else {
		m.log.Tracef("dbus call OK: %s on %s", method, obj.Path())
	}
	return c
}

func (m *Manager) setPower(enable bool) error {
	obj := m.conn.Object(bluezBusName, m.adapter)
	return m.dbusCall(obj, "org.freedesktop.DBus.Properties.Set",
		bluezAdapterInterface, "Powered", dbus.MakeVariant(enable)).Err
}

func (m *Manager) SetDiscoverable(enable bool) error {
	m.log.Debugf("bluetooth: SetDiscoverable(%v) entry, acquiring mutex", enable)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log.Debugf("bluetooth: SetDiscoverable(%v) mutex acquired", enable)

	obj := m.conn.Object(bluezBusName, m.adapter)

	// disable auto-off when enabling, set bluetooth default (180s) when disabling
	var timeout uint32 = 180
	if enable {
		timeout = 0
	}
	if err := m.dbusCall(obj, "org.freedesktop.DBus.Properties.Set",
		bluezAdapterInterface, "DiscoverableTimeout", dbus.MakeVariant(timeout)).Err; err != nil {
		m.log.WithError(err).Debug("bluetooth: failed to set DiscoverableTimeout")
	}

	if err := m.dbusCall(obj, "org.freedesktop.DBus.Properties.Set",
		bluezAdapterInterface, "Discoverable", dbus.MakeVariant(enable)).Err; err != nil {
		return fmt.Errorf("set Discoverable=%v: %w", enable, err)
	}

	if err := m.dbusCall(obj, "org.freedesktop.DBus.Properties.Set",
		bluezAdapterInterface, "Pairable", dbus.MakeVariant(enable)).Err; err != nil {
		return fmt.Errorf("set Pairable=%v: %w", enable, err)
	}

	m.log.Infof("bluetooth: discoverable=%v pairable=%v timeout=%d", enable, enable, timeout)
	return nil
}

// SetTrusted lets the device reconnect + authorize services
func (m *Manager) SetTrusted(address string, trusted bool) error {
	devicePath := formatDevicePath(m.adapter, address)
	obj := m.conn.Object(bluezBusName, devicePath)
	return m.dbusCall(obj, "org.freedesktop.DBus.Properties.Set",
		bluezDeviceInterface, "Trusted", dbus.MakeVariant(trusted)).Err
}

func formatDevicePath(adapter dbus.ObjectPath, address string) dbus.ObjectPath {
	return dbus.ObjectPath(fmt.Sprintf("%s/dev_%s", adapter, strings.ReplaceAll(address, ":", "_")))
}

func fillDeviceInfo(info *DeviceInfo, props map[string]dbus.Variant) {
	if v, ok := props["Name"]; ok {
		info.Name, _ = v.Value().(string)
	}
	if v, ok := props["Alias"]; ok {
		info.Alias, _ = v.Value().(string)
	}
	if v, ok := props["Class"]; ok {
		if c, ok := v.Value().(uint32); ok {
			info.Class = fmt.Sprintf("%d", c)
		}
	}
	if v, ok := props["Icon"]; ok {
		info.Icon, _ = v.Value().(string)
	}
	if v, ok := props["Paired"]; ok {
		info.Paired, _ = v.Value().(bool)
	}
	if v, ok := props["Trusted"]; ok {
		info.Trusted, _ = v.Value().(bool)
	}
	if v, ok := props["Blocked"]; ok {
		info.Blocked, _ = v.Value().(bool)
	}
	if v, ok := props["Connected"]; ok {
		info.Connected, _ = v.Value().(bool)
	}
	if v, ok := props["LegacyPairing"]; ok {
		info.LegacyPairing, _ = v.Value().(bool)
	}
}

func (m *Manager) GetDeviceInfo(address string) (*DeviceInfo, error) {
	m.log.Debugf("bluetooth: GetDeviceInfo(%s) entry", address)

	devicePath := formatDevicePath(m.adapter, address)
	obj := m.conn.Object(bluezBusName, devicePath)

	props := make(map[string]dbus.Variant)
	if err := m.dbusCall(obj, "org.freedesktop.DBus.Properties.GetAll", bluezDeviceInterface).Store(&props); err != nil {
		return nil, err
	}

	info := &DeviceInfo{Address: address}
	fillDeviceInfo(info, props)

	batteryProps := make(map[string]dbus.Variant)
	if err := m.dbusCall(obj, "org.freedesktop.DBus.Properties.GetAll", bluezBatteryInterface).Store(&batteryProps); err == nil {
		if v, ok := batteryProps["Percentage"]; ok {
			if p, ok := v.Value().(uint8); ok {
				info.BatteryPercentage = int(p)
			}
		}
	}

	return info, nil
}

func (m *Manager) GetDevices() ([]DeviceInfo, error) {
	m.log.Debug("bluetooth: GetDevices entry")
	m.mu.Lock()
	defer m.mu.Unlock()

	objects := make(map[dbus.ObjectPath]map[string]map[string]dbus.Variant)
	obj := m.conn.Object(bluezBusName, "/")
	if err := m.dbusCall(obj, "org.freedesktop.DBus.ObjectManager.GetManagedObjects").Store(&objects); err != nil {
		return nil, fmt.Errorf("get managed objects: %w", err)
	}

	var devices []DeviceInfo
	for path, interfaces := range objects {
		deviceProps, ok := interfaces[bluezDeviceInterface]
		if !ok {
			continue
		}

		address := strings.TrimPrefix(string(path), string(m.adapter)+"/dev_")
		address = strings.ReplaceAll(address, "_", ":")

		info := DeviceInfo{Address: address}
		fillDeviceInfo(&info, deviceProps)

		if batteryProps, ok := interfaces[bluezBatteryInterface]; ok {
			if v, ok := batteryProps["Percentage"]; ok {
				if p, ok := v.Value().(uint8); ok {
					info.BatteryPercentage = int(p)
				}
			}
		}

		devices = append(devices, info)
	}

	return devices, nil
}

func (m *Manager) RemoveDevice(address string) error {
	m.log.Debugf("bluetooth: RemoveDevice(%s) entry", address)
	m.mu.Lock()
	defer m.mu.Unlock()

	devicePath := formatDevicePath(m.adapter, address)
	obj := m.conn.Object(bluezBusName, m.adapter)

	if err := m.dbusCall(obj, bluezAdapterInterface+".RemoveDevice", devicePath).Err; err != nil {
		return err
	}

	if m.hid != nil {
		m.hid.clearWatchdog(address)
	}
	return nil
}

// arms the subscription watchdog for every connected paired phone
func (m *Manager) sweepHIDWatchdogs() {
	devs, err := m.GetDevices()
	if err != nil {
		return
	}
	for _, d := range devs {
		if d.Paired && d.Connected {
			m.armHIDWatchdog(d.Address)
		}
	}
}

func (m *Manager) AcceptPairing() error { return m.agent.acceptPairing() }

func (m *Manager) DenyPairing() error { return m.agent.rejectPairing() }

func (m *Manager) GetCurrentPairingRequest() *PairingRequest {
	if m.agent == nil {
		return nil
	}
	return m.agent.getCurrent()
}

func (m *Manager) ConnectDevice(address string) error {
	cmd := exec.Command("nmcli", "device", "connect", address)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nmcli connect: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}

	m.log.Infof("bluetooth: connected to %s via nmcli", address)

	go func() {
		deviceInfo, err := m.GetDeviceInfo(address)
		if m.emit == nil {
			return
		}
		if err != nil {
			m.log.WithError(err).Warn("bluetooth: failed to fetch device info after connect")
			m.emit(EventConnect, DeviceConnectedPayload{Address: address})
			return
		}
		m.emit(EventConnect, DeviceConnectedPayload{Address: address, Device: deviceInfo})
	}()

	return nil
}

// PageDevice is the explicit "connect to this phone" action from the UI
func (m *Manager) PageDevice(address string) error {
	if !m.isKnownDevice(address) {
		return fmt.Errorf("unknown device %s", address)
	}
	m.clearManualDisconnect(address)
	m.clearPanBackoff(address)

	info, err := m.GetDeviceInfo(address)
	if err == nil && info.Connected {
		// ACL already up, just (re-)establish PAN
		go func() {
			if err := m.ConnectNetworkForced(address); err != nil {
				m.log.WithError(err).Warnf("bluetooth: explicit PAN connect to %s failed", address)
			}
		}()
		return nil
	}

	m.tryActiveReconnect([]string{address})
	return nil
}

func (m *Manager) DisconnectDevice(address string) error {
	m.log.Debugf("bluetooth: DisconnectDevice(%s) entry", address)
	m.mu.Lock()
	defer m.mu.Unlock()

	devicePath := formatDevicePath(m.adapter, address)
	obj := m.conn.Object(bluezBusName, devicePath)

	m.pendingDisconnects.Store(address, true)
	// asked for disconnect
	m.markManualDisconnect(address)

	if err := m.dbusCall(obj, "org.bluez.Device1.Disconnect").Err; err != nil {
		m.pendingDisconnects.Delete(address)
		m.clearManualDisconnect(address)
		return fmt.Errorf("disconnect device: %w", err)
	}

	if m.emit != nil {
		m.emit(EventDisconnect, DeviceDisconnectedPayload{Address: address})
	}

	return nil
}

// ConnectNetwork brings up BT-PAN
func (m *Manager) ConnectNetwork(address string) error {
	return m.connectNetworkInternal(address, false)
}

// does a full teardown + reconnect
func (m *Manager) ConnectNetworkForced(address string) error {
	return m.connectNetworkInternal(address, true)
}

func (m *Manager) connectNetworkInternal(address string, force bool) error {
	m.log.Debugf("bluetooth: ConnectNetwork(%s, force=%v) entry", address, force)
	m.networkMu.Lock()
	defer m.networkMu.Unlock()

	devicePath := formatDevicePath(m.adapter, address)
	obj := m.conn.Object(bluezBusName, devicePath)

	if force {
		m.noteSelfTeardown()
		if err := m.dbusCall(obj, bluezNetworkInterface+".Disconnect").Err; err != nil {
			m.log.WithError(err).Debugf("bluetooth: pre-reconnect Disconnect failed (ok if not connected)")
		}
		time.Sleep(500 * time.Millisecond)
	} else {
		// bnep0 already up + same address tracked
		m.panMu.Lock()
		lastAddr := m.lastPanAddress
		m.panMu.Unlock()
		if lastAddr == address {
			if link, err := netlink.LinkByName(panInterface); err == nil && link.Attrs().Flags&net.FlagUp != 0 {
				m.setPanSessionUp(address)
				m.log.Debugf("bluetooth: ConnectNetwork(%s) skipped, PAN already up", address)
				return nil
			}
		}
	}

	if err := m.dbusCall(obj, bluezNetworkInterface+".Connect", "nap").Err; err != nil {
		return fmt.Errorf("network connect (nap): %w", err)
	}

	link, err := netlink.LinkByName(panInterface)
	if err != nil || link.Attrs().Flags&net.FlagUp == 0 {
		return fmt.Errorf("%s interface is not up", panInterface)
	}

	// NM doesnt auto manage bnep0 on the car thing
	m.requestDHCP()

	m.panMu.Lock()
	m.lastPanAddress = address
	m.panMu.Unlock()

	// PAN coming up is proof the user wants this device connected
	m.clearManualDisconnect(address)
	m.clearPanBackoffPause(address)
	m.setPanSessionUp(address)

	go func() {
		var name string
		if info, err := m.GetDeviceInfo(address); err == nil {
			if info.Alias != "" {
				name = info.Alias
			} else {
				name = info.Name
			}
		}
		m.recordPanConnected(address, name)
	}()

	if m.emit != nil {
		m.emit(EventNetworkConnect, NetworkConnectedPayload{Address: address})
	}

	return nil
}

// gives the offline-retry loop a target before any fresh ConnectNetwork
func (m *Manager) SeedLastPanAddress(addr string) {
	if addr == "" {
		return
	}
	m.panMu.Lock()
	m.lastPanAddress = addr
	m.panMu.Unlock()
	m.log.Debugf("bluetooth: seeded lastPanAddress=%s from persisted state", addr)
}

// requestDHCP runs dhclient in the background for an IPv4 lease on bnep0
func (m *Manager) requestDHCP() {
	dhclient, err := exec.LookPath("dhclient")
	if err != nil {
		m.log.Warn("bluetooth: dhclient not found, bnep0 will have no IP until configured manually")
		return
	}

	// -nw forks, -1 gives up after one DISCOVER cycle
	cmd := exec.Command(dhclient, "-nw", "-1",
		"-pf", "/run/dhclient.bnep0.pid",
		"-lf", "/run/dhclient.bnep0.leases",
		panInterface)
	if err := cmd.Start(); err != nil {
		m.log.WithError(err).Warn("bluetooth: dhclient failed to start")
		return
	}
	m.log.Infof("bluetooth: dhclient started for %s (pid %d)", panInterface, cmd.Process.Pid)

	// reap so it doesn't zombie
	go func() { _ = cmd.Wait() }()
}

func (m *Manager) NetworkUp() bool {
	link, err := netlink.LinkByName(panInterface)
	if err != nil {
		return false
	}
	return link.Attrs().Flags&net.FlagUp != 0
}

func (m *Manager) SetOnline(online bool) {
	m.onlineMu.Lock()
	m.online = online
	m.onlineMu.Unlock()
}

func (m *Manager) IsOnline() bool {
	m.onlineMu.Lock()
	defer m.onlineMu.Unlock()
	return m.online
}

func (m *Manager) TetherRouteState() string {
	if m == nil {
		return ""
	}
	r := panDefaultRoute()
	if r == nil {
		return ""
	}
	if r.Priority >= demotedMetric {
		return "demoted"
	}
	return "ok"
}
