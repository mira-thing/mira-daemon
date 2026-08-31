package bluetooth

// BLE volume peripheral
// lets car thing present itself as an input device so the connected phone accepts volume changes

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/godbus/dbus/v5"

	librespot "github.com/devgianlu/go-librespot"
)

const (
	gattServiceIface  = "org.bluez.GattService1"
	gattCharIface     = "org.bluez.GattCharacteristic1"
	gattDescIface     = "org.bluez.GattDescriptor1"
	gattManagerIface  = "org.bluez.GattManager1"
	leAdvIface        = "org.bluez.LEAdvertisement1"
	leAdvManagerIface = "org.bluez.LEAdvertisingManager1"
	dbusPropsIface    = "org.freedesktop.DBus.Properties"
	dbusObjMgrIface   = "org.freedesktop.DBus.ObjectManager"

	hidAppPath = dbus.ObjectPath("/com/mira/hid")
	hidAdvPath = dbus.ObjectPath("/com/mira/hid/advertisement0")

	uuidHIDService   = "00001812-0000-1000-8000-00805f9b34fb"
	uuidBattService  = "0000180f-0000-1000-8000-00805f9b34fb"
	uuidDISService   = "0000180a-0000-1000-8000-00805f9b34fb"
	uuidProtocolMode = "00002a4e-0000-1000-8000-00805f9b34fb"
	uuidReportMap    = "00002a4b-0000-1000-8000-00805f9b34fb"
	uuidReport       = "00002a4d-0000-1000-8000-00805f9b34fb"
	uuidHIDInfo      = "00002a4a-0000-1000-8000-00805f9b34fb"
	uuidControlPoint = "00002a4c-0000-1000-8000-00805f9b34fb"
	uuidBattLevel    = "00002a19-0000-1000-8000-00805f9b34fb"
	uuidPnPID        = "00002a50-0000-1000-8000-00805f9b34fb"
	uuidManufacturer = "00002a29-0000-1000-8000-00805f9b34fb"
	uuidModelNumber  = "00002a24-0000-1000-8000-00805f9b34fb"
	uuidReportRef    = "00002908-0000-1000-8000-00805f9b34fb"

	// input report bits
	hidBitVolumeDown byte = 0x01
	hidBitVolumeUp   byte = 0x02

	hidPressHold       = 30 * time.Millisecond
	hidStepGap         = 80 * time.Millisecond
	hidMaxStepsPerSend = 16
	hidRegisterRetry   = 30 * time.Second

	// bonded phone connected this long without subscribing -> nudge it once
	hidSubscribeWatchdogDelay = 45 * time.Second
	hidWatchdogCooldown       = 10 * time.Minute
	hidAdvCycleGap            = 2 * time.Second

	hidAdvAppearanceKeyboard uint16 = 0x03C1
)

var hidReportMapV16 = []byte{
	0x05, 0x0C, // Usage Page (Consumer)
	0x09, 0x01, // Usage (Consumer Control)
	0xA1, 0x01, // Collection (Application)
	0x85, 0x01, //   Report ID (1)
	0x15, 0x00, //   Logical Minimum (0)
	0x25, 0x01, //   Logical Maximum (1)
	0x75, 0x01, //   Report Size (1)
	0x95, 0x02, //   Report Count (2)
	0x09, 0xEA, //   Usage (Volume Down)
	0x09, 0xE9, //   Usage (Volume Up)
	0x81, 0x02, //   Input (Data, Variable, Absolute)
	0x95, 0x06, //   Report Count (6) padding
	0x81, 0x03, //   Input (Const, Variable, Absolute)
	0xC0, // End Collection
}

// hidInfoValue
var hidInfoValue = []byte{0x11, 0x01, 0x00, 0x03}

var pnpIDValue = []byte{0x02, 0x6B, 0x1D, 0x46, 0x02, 0x00, 0x01}

func gattErr(name, msg string) *dbus.Error {
	return dbus.NewError(name, []interface{}{msg})
}

func shortUUID(u string) string {
	if len(u) == 36 && strings.HasSuffix(u, "-0000-1000-8000-00805f9b34fb") {
		return strings.TrimPrefix(u[:8], "0000")
	}
	return u
}

// renders the peer/link detail bluez passes with every read and write
func gattPeer(options map[string]dbus.Variant) string {
	dev, link, mtu := "?", "?", "?"
	if v, ok := options["device"]; ok {
		if path, ok := v.Value().(dbus.ObjectPath); ok {
			dev = string(path)
			if i := strings.LastIndex(dev, "dev_"); i >= 0 {
				dev = strings.ReplaceAll(dev[i+4:], "_", ":")
			}
		}
	}
	if v, ok := options["link"]; ok {
		if l, ok := v.Value().(string); ok && l != "" {
			link = l
		}
	}
	if v, ok := options["mtu"]; ok {
		mtu = fmt.Sprint(v.Value())
	}
	return fmt.Sprintf("%s link=%s mtu=%s", dev, link, mtu)
}

type gattProps struct {
	iface  string
	getAll func() map[string]dbus.Variant
}

func (p *gattProps) Get(iface, prop string) (dbus.Variant, *dbus.Error) {
	all, err := p.GetAll(iface)
	if err != nil {
		return dbus.Variant{}, err
	}
	v, ok := all[prop]
	if !ok {
		return dbus.Variant{}, gattErr("org.freedesktop.DBus.Error.InvalidArgs", "unknown property "+prop)
	}
	return v, nil
}

func (p *gattProps) GetAll(iface string) (map[string]dbus.Variant, *dbus.Error) {
	if iface != p.iface {
		return nil, gattErr("org.freedesktop.DBus.Error.InvalidArgs", "unknown interface "+iface)
	}
	return p.getAll(), nil
}

func (p *gattProps) Set(iface, prop string, value dbus.Variant) *dbus.Error {
	return gattErr("org.freedesktop.DBus.Error.PropertyReadOnly", prop+" is read-only")
}

type gattService struct {
	path dbus.ObjectPath
	uuid string
}

func (s *gattService) props() map[string]dbus.Variant {
	return map[string]dbus.Variant{
		"UUID":    dbus.MakeVariant(s.uuid),
		"Primary": dbus.MakeVariant(true),
	}
}

type gattDesc struct {
	path   dbus.ObjectPath
	uuid   string
	char   dbus.ObjectPath
	flags  []string
	value  []byte
	log    librespot.Logger
	onRead func()
}

func (d *gattDesc) props() map[string]dbus.Variant {
	return map[string]dbus.Variant{
		"UUID":           dbus.MakeVariant(d.uuid),
		"Characteristic": dbus.MakeVariant(d.char),
		"Flags":          dbus.MakeVariant(d.flags),
	}
}

func (d *gattDesc) ReadValue(options map[string]dbus.Variant) ([]byte, *dbus.Error) {
	if d.log != nil {
		d.log.Infof("bluetooth: hid: desc read %s by %s", shortUUID(d.uuid), gattPeer(options))
	}
	if d.onRead != nil {
		d.onRead()
	}
	return d.value, nil
}

func (d *gattDesc) WriteValue(value []byte, options map[string]dbus.Variant) *dbus.Error {
	if d.log != nil {
		d.log.Infof("bluetooth: hid: desc write %s by %s (%d bytes)", shortUUID(d.uuid), gattPeer(options), len(value))
	}
	return nil
}

type gattChar struct {
	path    dbus.ObjectPath
	uuid    string
	service dbus.ObjectPath
	flags   []string
	descs   []*gattDesc
	conn    *dbus.Conn
	log     librespot.Logger

	onStopNotify  func()
	onStartNotify func()
	onRead        func()

	mu        sync.Mutex
	value     []byte
	notifying bool
}

func (c *gattChar) props() map[string]dbus.Variant {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]dbus.Variant{
		"UUID":      dbus.MakeVariant(c.uuid),
		"Service":   dbus.MakeVariant(c.service),
		"Flags":     dbus.MakeVariant(c.flags),
		"Notifying": dbus.MakeVariant(c.notifying),
	}
}

func (c *gattChar) ReadValue(options map[string]dbus.Variant) ([]byte, *dbus.Error) {
	c.log.Infof("bluetooth: hid: read %s by %s", shortUUID(c.uuid), gattPeer(options))
	if c.onRead != nil {
		c.onRead()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.value...), nil
}

func (c *gattChar) WriteValue(value []byte, options map[string]dbus.Variant) *dbus.Error {
	c.log.Infof("bluetooth: hid: write %s by %s (%d bytes)", shortUUID(c.uuid), gattPeer(options), len(value))
	c.mu.Lock()
	c.value = append([]byte(nil), value...)
	c.mu.Unlock()
	return nil
}

func (c *gattChar) StartNotify() *dbus.Error {
	c.log.Infof("bluetooth: hid: host subscribed to %s", c.uuid)
	c.mu.Lock()
	c.notifying = true
	c.mu.Unlock()
	if c.onStartNotify != nil {
		c.onStartNotify()
	}
	return nil
}

func (c *gattChar) StopNotify() *dbus.Error {
	c.log.Infof("bluetooth: hid: host unsubscribed from %s", c.uuid)
	c.mu.Lock()
	c.notifying = false
	c.mu.Unlock()
	if c.onStopNotify != nil {
		c.onStopNotify()
	}
	return nil
}

func (c *gattChar) isNotifying() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.notifying
}

// pushes a new value via PropertiesChanged
func (c *gattChar) notifyValue(b []byte) error {
	c.mu.Lock()
	c.value = append([]byte(nil), b...)
	c.mu.Unlock()
	return c.conn.Emit(c.path, dbusPropsIface+".PropertiesChanged", gattCharIface,
		map[string]dbus.Variant{"Value": dbus.MakeVariant(b)}, []string{})
}

type gattApp struct {
	services []*gattService
	chars    []*gattChar
}

func (a *gattApp) GetManagedObjects() (map[dbus.ObjectPath]map[string]map[string]dbus.Variant, *dbus.Error) {
	objs := make(map[dbus.ObjectPath]map[string]map[string]dbus.Variant)
	for _, s := range a.services {
		objs[s.path] = map[string]map[string]dbus.Variant{gattServiceIface: s.props()}
	}
	for _, c := range a.chars {
		objs[c.path] = map[string]map[string]dbus.Variant{gattCharIface: c.props()}
		for _, d := range c.descs {
			objs[d.path] = map[string]map[string]dbus.Variant{gattDescIface: d.props()}
		}
	}
	return objs, nil
}

// LE advertisement object
type hidAdvertisement struct{}

func (a *hidAdvertisement) props() map[string]dbus.Variant {
	return map[string]dbus.Variant{
		"Type":         dbus.MakeVariant("peripheral"),
		"ServiceUUIDs": dbus.MakeVariant([]string{"1812"}),
		"Appearance":   dbus.MakeVariant(hidAdvAppearanceKeyboard),
		"Discoverable": dbus.MakeVariant(true),
	}
}

func (a *hidAdvertisement) Release() *dbus.Error { return nil }

type hidVolume struct {
	log     librespot.Logger
	conn    *dbus.Conn
	adapter dbus.ObjectPath
	input   *gattChar

	regMu         sync.Mutex
	appRegistered bool
	advRegistered bool
	closed        bool
	advFailures   int
	// the connected phone was nudged and still won't subscribe to volume
	subDead bool

	// which HID characteristics the host has actually read since daemon start
	readMu   sync.Mutex
	charRead map[string]bool

	// input reports actually emitted
	sentMu      sync.Mutex
	sentReports int

	onAdvUp func()

	watchdogMu sync.Mutex
	watchdogAt map[string]time.Time

	// reports whether any peer holds an LE link
	leLinkUp func() bool

	pokeCh   chan struct{}
	sendCh   chan int // signed steps
	stop     chan struct{}
	stopOnce sync.Once
}

// builds and exports the GATT tree + advertisement
func newHIDVolume(log librespot.Logger, conn *dbus.Conn, adapter dbus.ObjectPath, localName string) (*hidVolume, error) {
	hidSvc := &gattService{path: hidAppPath + "/service0", uuid: uuidHIDService}
	battSvc := &gattService{path: hidAppPath + "/service1", uuid: uuidBattService}
	disSvc := &gattService{path: hidAppPath + "/service2", uuid: uuidDISService}

	newChar := func(svc dbus.ObjectPath, idx int, uuid string, flags []string, value []byte) *gattChar {
		return &gattChar{
			path:    dbus.ObjectPath(fmt.Sprintf("%s/char%d", svc, idx)),
			uuid:    uuid,
			service: svc,
			flags:   flags,
			value:   value,
			conn:    conn,
			log:     log,
		}
	}

	protocolMode := newChar(hidSvc.path, 0, uuidProtocolMode, []string{"read", "write-without-response"}, []byte{0x01})
	reportMap := newChar(hidSvc.path, 1, uuidReportMap, []string{"encrypt-read"}, hidReportMapV16)
	input := newChar(hidSvc.path, 2, uuidReport, []string{"encrypt-read", "encrypt-notify"}, []byte{0x00})
	input.descs = []*gattDesc{{
		path: input.path + "/desc0", uuid: uuidReportRef,
		char: input.path, flags: []string{"read"},
		value: []byte{0x01, 0x01}, // Report ID 1
		log:   log,
	}}
	hidInfo := newChar(hidSvc.path, 3, uuidHIDInfo, []string{"read"}, hidInfoValue)
	controlPoint := newChar(hidSvc.path, 4, uuidControlPoint, []string{"write-without-response"}, []byte{})
	battLevel := newChar(battSvc.path, 0, uuidBattLevel, []string{"read", "notify"}, []byte{100})
	pnp := newChar(disSvc.path, 0, uuidPnPID, []string{"read"}, pnpIDValue)
	manufacturer := newChar(disSvc.path, 1, uuidManufacturer, []string{"read"}, []byte(localName))
	model := newChar(disSvc.path, 2, uuidModelNumber, []string{"read"}, []byte("Car Thing"))

	app := &gattApp{
		services: []*gattService{hidSvc, battSvc, disSvc},
		chars:    []*gattChar{protocolMode, reportMap, input, hidInfo, controlPoint, battLevel, pnp, manufacturer, model},
	}

	if err := conn.Export(app, hidAppPath, dbusObjMgrIface); err != nil {
		return nil, fmt.Errorf("export hid app: %w", err)
	}
	for _, s := range app.services {
		s := s
		if err := conn.Export(s, s.path, gattServiceIface); err != nil {
			return nil, fmt.Errorf("export hid service %s: %w", s.uuid, err)
		}
		if err := conn.Export(&gattProps{gattServiceIface, s.props}, s.path, dbusPropsIface); err != nil {
			return nil, fmt.Errorf("export hid service props %s: %w", s.uuid, err)
		}
	}
	for _, c := range app.chars {
		c := c
		if err := conn.Export(c, c.path, gattCharIface); err != nil {
			return nil, fmt.Errorf("export hid char %s: %w", c.uuid, err)
		}
		if err := conn.Export(&gattProps{gattCharIface, c.props}, c.path, dbusPropsIface); err != nil {
			return nil, fmt.Errorf("export hid char props %s: %w", c.uuid, err)
		}
		for _, d := range c.descs {
			d := d
			if err := conn.Export(d, d.path, gattDescIface); err != nil {
				return nil, fmt.Errorf("export hid desc %s: %w", d.uuid, err)
			}
			if err := conn.Export(&gattProps{gattDescIface, d.props}, d.path, dbusPropsIface); err != nil {
				return nil, fmt.Errorf("export hid desc props %s: %w", d.uuid, err)
			}
		}
	}
	adv := &hidAdvertisement{}
	if err := conn.Export(adv, hidAdvPath, leAdvIface); err != nil {
		return nil, fmt.Errorf("export hid advertisement: %w", err)
	}
	if err := conn.Export(&gattProps{leAdvIface, adv.props}, hidAdvPath, dbusPropsIface); err != nil {
		return nil, fmt.Errorf("export hid advertisement props: %w", err)
	}

	h := &hidVolume{
		log:        log,
		conn:       conn,
		adapter:    adapter,
		input:      input,
		watchdogAt: make(map[string]time.Time),
		pokeCh:     make(chan struct{}, 1),
		sendCh:     make(chan int, 8),
		stop:       make(chan struct{}),
	}

	input.onStopNotify = func() {
		h.regMu.Lock()
		rearm := h.onAdvUp
		h.regMu.Unlock()
		if rearm != nil {
			go rearm()
		}
	}
	// an explicit resubscribe proves the knob works again
	input.onStartNotify = func() { h.setSubDead(false) }
	for _, c := range []*gattChar{pnp, hidInfo, reportMap} {
		c := c
		c.onRead = func() { h.markRead(c.uuid) }
	}
	input.descs[0].onRead = func() { h.markRead(uuidReportRef) }
	return h, nil
}

// registers with bluez (via the reconciler goroutine) and launches the send app
func (h *hidVolume) start(onAdvUp func()) {
	h.regMu.Lock()
	h.onAdvUp = onAdvUp
	h.regMu.Unlock()
	go h.reconcilerLoop()
	go h.sendWorker()
	h.poke()
}

// The controller cannot advertise at all while a phone holds an LE link
func (h *hidVolume) advWantedLocked() bool {
	return !h.closed
}

func (h *hidVolume) poke() {
	select {
	case h.pokeCh <- struct{}{}:
	default:
	}
}

// bluezCall invokes a bluez method with a timeout so a wedged bluetoothd can
// never hang the reconciler forever
func (h *hidVolume) bluezCall(method string, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), dbusCallTimeout)
	defer cancel()
	return h.conn.Object(bluezBusName, h.adapter).CallWithContext(ctx, method, 0, args...).Err
}

// isBluezErr reports whether err is the named org.bluez.Error.*
func isBluezErr(err error, name string) bool {
	var dbusErr dbus.Error
	if !errors.As(err, &dbusErr) {
		return false
	}
	return dbusErr.Name == "org.bluez.Error."+name
}

func (h *hidVolume) reconcilerLoop() {
	for {
		select {
		case <-h.stop:
			return
		case <-h.pokeCh:
		}
		for {
			h.regMu.Lock()
			settled := h.reconcileLocked()
			h.regMu.Unlock()
			if settled {
				break
			}
			select {
			case <-h.stop:
				return
			case <-time.After(hidRegisterRetry):
			case <-h.pokeCh:
			}
		}
	}
}

// drives bluez registration toward the desired state
func (h *hidVolume) reconcileLocked() bool {
	if h.closed {
		return true
	}
	if !h.appRegistered {
		if err := h.bluezCall(gattManagerIface+".RegisterApplication", hidAppPath, map[string]dbus.Variant{}); err != nil && !isBluezErr(err, "AlreadyExists") {
			h.log.WithError(err).Warn("bluetooth: hid: gatt app registration failed, will retry in background")
			return false
		}
		h.appRegistered = true
		h.log.Infof("bluetooth: hid: volume-key gatt service registered")
	}
	if h.advWantedLocked() && !h.advRegistered {
		if err := h.bluezCall(leAdvManagerIface+".RegisterAdvertisement", hidAdvPath, map[string]dbus.Variant{}); err != nil && !isBluezErr(err, "AlreadyExists") {
			h.advFailures++
			// keep failing registrations visible in field logs
			if h.advFailures == 1 || h.advFailures%10 == 0 {
				h.log.WithError(err).Warnf("bluetooth: hid: advertisement registration failed (attempt %d), retrying every %s", h.advFailures, hidRegisterRetry)
			} else {
				h.log.WithError(err).Debug("bluetooth: hid: advertisement registration retry failed")
			}
			return false
		}
		if h.advFailures > 0 {
			h.log.Infof("bluetooth: hid: advertisement registered after %d failed attempts", h.advFailures)
			h.advFailures = 0
		}
		h.advRegistered = true
		h.log.Infof("bluetooth: hid: volume-key advertisement up")
		if h.onAdvUp != nil {
			go h.onAdvUp()
		}
	} else if !h.advWantedLocked() && h.advRegistered {
		if err := h.bluezCall(leAdvManagerIface+".UnregisterAdvertisement", hidAdvPath); err != nil && !isBluezErr(err, "DoesNotExist") {
			// bluez may still be radiating the keyboard identity, keep trying
			h.log.WithError(err).Warn("bluetooth: hid: advertisement unregister failed, will retry (identity may still be visible)")
			return false
		}
		h.advRegistered = false
		h.log.Infof("bluetooth: hid: volume-key advertisement down")
	}
	return true
}

// re-registers the advertisement after the last LE link drops
func (h *hidVolume) forceAdvRefresh() {
	h.regMu.Lock()
	if h.closed {
		h.regMu.Unlock()
		return
	}
	if !h.advRegistered {
		h.regMu.Unlock()
		h.poke()
		return
	}
	if err := h.bluezCall(leAdvManagerIface+".UnregisterAdvertisement", hidAdvPath); err != nil && !isBluezErr(err, "DoesNotExist") {
		h.log.WithError(err).Debug("bluetooth: hid: advertisement unregister failed during refresh")
		h.regMu.Unlock()
		return
	}
	h.advRegistered = false
	h.regMu.Unlock()
	h.log.Info("bluetooth: hid: le link down, rebuilding volume-key advertisement")

	select {
	case <-h.stop:
		return
	case <-time.After(hidAdvCycleGap):
	}
	h.poke()
}

// records an emitted report
func (h *hidVolume) noteSent() (int, bool) {
	h.sentMu.Lock()
	defer h.sentMu.Unlock()
	h.sentReports++
	return h.sentReports, h.sentReports == 1 || h.sentReports%50 == 0
}

func (h *hidVolume) sentCount() int {
	h.sentMu.Lock()
	defer h.sentMu.Unlock()
	return h.sentReports
}

func (h *hidVolume) markRead(uuid string) {
	h.readMu.Lock()
	if h.charRead == nil {
		h.charRead = make(map[string]bool)
	}
	h.charRead[shortUUID(uuid)] = true
	h.readMu.Unlock()
}

func (h *hidVolume) readSummary() string {
	yn := func(ok bool) string {
		if ok {
			return "y"
		}
		return "n"
	}
	h.readMu.Lock()
	var out []string
	for _, u := range []string{"2a50", "2a4a", "2a4b", "2908"} {
		out = append(out, u+"="+yn(h.charRead[u]))
	}
	h.readMu.Unlock()
	return "hid attach since start: " + strings.Join(out, " ") + " notify=" + yn(h.input.isNotifying())
}

// flips whether knob sends should be reported as unusable
func (h *hidVolume) setSubDead(dead bool) {
	h.regMu.Lock()
	changed := !h.closed && h.subDead != dead
	if changed {
		h.subDead = dead
	}
	h.regMu.Unlock()
	if changed && !dead {
		h.log.Infof("bluetooth: hid: volume-key subscription restored")
	}
}

func (h *hidVolume) watchdogClaim(addr string, now time.Time) bool {
	h.watchdogMu.Lock()
	defer h.watchdogMu.Unlock()
	if h.watchdogAt == nil {
		h.watchdogAt = make(map[string]time.Time)
	}
	if t, ok := h.watchdogAt[addr]; ok && now.Sub(t) < hidWatchdogCooldown {
		return false
	}
	h.watchdogAt[addr] = now
	return true
}

// schedules a check that a connected bonded host actually subscribes to volume
func (h *hidVolume) armSubscribeWatchdog(addr string, stillConnected func() bool) {

	h.setSubDead(false)
	h.log.Debugf("bluetooth: hid: subscribe watchdog armed for %s", addr)
	go func() {
		select {
		case <-h.stop:
			return
		case <-time.After(hidSubscribeWatchdogDelay):
		}
		h.regMu.Lock()
		ready := !h.closed && h.advWantedLocked()
		h.regMu.Unlock()
		h.log.Debugf("bluetooth: hid: subscribe watchdog fire check for %s: ready=%v notifying=%v connected=%v",
			addr, ready, h.input.isNotifying(), stillConnected())
		if !ready || h.input.isNotifying() || !stillConnected() {
			return
		}
		if !h.watchdogClaim(addr, time.Now()) {
			return
		}
		// Detection only
		h.log.Warnf("bluetooth: hid: %s connected %.0fs without subscribing to volume keys, reporting knob as unusable for this phone (%s)",
			addr, hidSubscribeWatchdogDelay.Seconds(), h.readSummary())
		h.setSubDead(true)
	}()
}

func (h *hidVolume) clearWatchdog(addr string) {
	h.watchdogMu.Lock()
	delete(h.watchdogAt, addr)
	h.watchdogMu.Unlock()
	h.setSubDead(false)
}

func (h *hidVolume) advState() string {
	h.regMu.Lock()
	app, adv := h.appRegistered, h.advRegistered
	h.regMu.Unlock()
	switch {
	case !app:
		return "gatt service not registered"
	case !adv:
		return "advertisement pending (bluez retry)"
	case h.leLinkUp != nil && h.leLinkUp():
		return "registered, not radiating (phone holds the LE link)"
	default:
		return "advertising"
	}
}

// reports whether volume keys can be used atm
func (h *hidVolume) available() bool {
	h.regMu.Lock()
	registered := h.appRegistered
	h.regMu.Unlock()
	return registered && h.input.isNotifying()
}

func (h *hidVolume) sendSteps(steps int) bool {
	if steps == 0 || !h.available() {
		return false
	}
	select {
	case h.sendCh <- steps:
		return true
	default:
		h.log.Debug("bluetooth: hid: volume queue full, dropping step burst")
		return true
	}
}

func (h *hidVolume) sendWorker() {
	for {
		select {
		case <-h.stop:
			return
		case steps := <-h.sendCh:
			bit := hidBitVolumeUp
			if steps < 0 {
				bit = hidBitVolumeDown
				steps = -steps
			}
			if steps > hidMaxStepsPerSend {
				steps = hidMaxStepsPerSend
			}
			for i := 0; i < steps; i++ {
				if err := h.input.notifyValue([]byte{bit}); err != nil {
					h.log.WithError(err).Debug("bluetooth: hid: volume press dropped")
					break
				}
				time.Sleep(hidPressHold)
				if err := h.input.notifyValue([]byte{0x00}); err != nil {
					h.log.WithError(err).Debug("bluetooth: hid: volume release dropped")
					break
				}
				if n, loud := h.noteSent(); loud {
					h.log.Infof("bluetooth: hid: %d volume report(s) emitted (host subscribed=%v)", n, h.input.isNotifying())
				}
				if i < steps-1 {
					time.Sleep(hidStepGap)
				}
			}
		}
	}
}

func (h *hidVolume) close() {
	h.stopOnce.Do(func() { close(h.stop) })
	h.regMu.Lock()
	h.closed = true
	adv, app := h.advRegistered, h.appRegistered
	h.advRegistered, h.appRegistered = false, false
	h.regMu.Unlock()

	if adv {
		if err := h.bluezCall(leAdvManagerIface+".UnregisterAdvertisement", hidAdvPath); err != nil {
			h.log.WithError(err).Debug("bluetooth: hid: advertisement unregister failed")
		}
	}
	if app {
		if err := h.bluezCall(gattManagerIface+".UnregisterApplication", hidAppPath); err != nil {
			h.log.WithError(err).Debug("bluetooth: hid: gatt app unregister failed")
		}
	}
}
