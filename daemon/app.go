package daemon

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/devgianlu/go-librespot/apresolve"
	"github.com/devgianlu/go-librespot/daemon/bluetooth"
	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	devicespb "github.com/devgianlu/go-librespot/proto/spotify/connectstate/devices"
	"github.com/devgianlu/go-librespot/session"
	"golang.org/x/exp/rand"
)

type App struct {
	log librespot.Logger
	cfg *Config

	stateStore StateStore

	client *http.Client

	resolver *apresolve.ApResolver

	deviceId    string
	deviceType  devicespb.DeviceType
	clientToken string
	state       *librespot.AppState

	server   ApiServer
	logoutCh chan *AppPlayer

	// currentPlayer points at the running AppPlayer (nil between sessions), so
	// App-level actions like resume-from-suspend can reach its dealer.
	currentPlayer atomic.Pointer[AppPlayer]

	// holds the live pathfinder hashes
	hashes *hashStore

	// virtualTouch is a lazily-created uinput touchscreen used to inject a tap
	// on resume (re-engages Chromium's keyboard seat). nil until first created.
	virtualTouchMu sync.Mutex
	virtualTouch   *os.File

	// auth state for /auth/status
	authMu       sync.RWMutex
	authRequired bool
	authURL      string
	// authKnown distinguishes the state so the frontend can branch correctly
	authKnown bool

	bt *bluetooth.Manager

	// voice is the on-device voice service
	voice *voiceService

	retryNowCh chan struct{}

	// pre-network attempt sits in DNS resolution for 20-30s. parking here until network is up dodges that
	onlineMu sync.Mutex
	isOnline bool
	onlineCh chan struct{}
	netDrops int // online->offline transitions this session

	// daemon uptime
	startedAt time.Time

	// last observed clock jump
	clockSteps clockStepTracker

	// check in service
	checkinId     string
	checkinKick   chan struct{}
	checkinStatus checkinTracker

	closed bool
}

func parseDeviceType(val string) (devicespb.DeviceType, error) {
	valEnum, ok := devicespb.DeviceType_value[strings.ToUpper(val)]
	if !ok {
		return 0, fmt.Errorf("invalid device type: %s", val)
	}

	return devicespb.DeviceType(valEnum), nil
}

func New(opts *Options) (*App, error) {
	if opts == nil {
		return nil, errors.New("daemon: Options is required")
	}
	if opts.Logger == nil {
		return nil, errors.New("daemon: Options.Logger is required")
	}
	if opts.Config == nil {
		return nil, errors.New("daemon: Options.Config is required")
	}
	if opts.StateStore == nil {
		return nil, errors.New("daemon: Options.StateStore is required")
	}

	app := &App{
		log:         opts.Logger,
		cfg:         opts.Config,
		stateStore:  opts.StateStore,
		logoutCh:    make(chan *AppPlayer),
		client:      &http.Client{Timeout: 30 * time.Second},
		retryNowCh:  make(chan struct{}, 1),
		onlineCh:    make(chan struct{}),
		checkinKick: make(chan struct{}, 1),
		hashes:      newHashStore(),
		startedAt:   time.Now(),
	}

	var err error
	app.deviceType, err = parseDeviceType(app.cfg.DeviceType)
	if err != nil {
		return nil, err
	}

	app.state, err = opts.StateStore.Load()
	if err != nil {
		return nil, fmt.Errorf("loading state: %w", err)
	}
	if app.state == nil {
		app.state = &librespot.AppState{}
	}

	app.resolver = apresolve.NewApResolver(app.log, app.client)

	if app.cfg.DeviceId != "" {
		app.deviceId = app.cfg.DeviceId
	} else if app.state.DeviceId != "" {
		app.deviceId = app.state.DeviceId
	} else {
		deviceIdBytes := make([]byte, 20)
		_, _ = rand.Read(deviceIdBytes)
		app.deviceId = hex.EncodeToString(deviceIdBytes)
		app.log.Infof("generated new device id: %s", app.deviceId)

		app.state.Lock()
		app.state.DeviceId = app.deviceId
		app.state.Unlock()
		if err := app.persistState(); err != nil {
			return nil, err
		}
	}

	if app.cfg.ClientToken != "" {
		app.clientToken = app.cfg.ClientToken
	}

	if opts.APIServer != nil {
		app.server = opts.APIServer
	} else {
		app.server, _ = NewStubApiServer(app.log)
	}

	// /auth/status can answer before any AppPlayer exists
	app.server.SetAuthHandler(app.GetAuthState)

	// app owns reset because it touches BT manager + state store
	app.server.SetSystemHandler(app)
	app.server.SetSettingsHandler(app)
	app.server.SetDebugHandler(app)

	// mirror persisted settings into the firmware brightness conf
	if len(app.state.Settings) > 0 {
		app.mirrorBacklightConf(app.state.Settings)
	}

	// BT manager init, non-fatal on dev/test systems without BlueZ
	emit := func(eventType string, payload any) {
		app.server.Emit(&ApiEvent{
			Type: ApiEventType(eventType),
			Data: payload,
		})
	}
	if bm, err := bluetooth.NewManager(app.log, emit); err != nil {
		app.log.WithError(err).Warn("bluetooth: manager unavailable (continuing without)")
	} else {
		app.bt = bm
		app.server.SetBluetoothHandler(bm)

		// seed the prioritized reconnect list from persisted state
		known := app.state.KnownBluetoothDevices
		if len(known) == 0 && app.state.LastBluetoothPanAddress != "" {
			// migrated single device inherits default priority
			known = []librespot.BluetoothKnownDevice{{Address: app.state.LastBluetoothPanAddress, Starred: true}}
		}
		if len(known) > 0 {
			bm.SeedKnownDevices(known)
			bm.SeedLastPanAddress(known[0].Address)
		}

		// persist on change so next reboot has the list
		bm.SetKnownDevicesChangedHandler(func(devs []librespot.BluetoothKnownDevice) {
			// Fires from Bluetooth manager goroutines
			app.state.Lock()
			app.state.KnownBluetoothDevices = devs
			if len(devs) > 0 {
				app.state.LastBluetoothPanAddress = devs[0].Address
			} else {
				app.state.LastBluetoothPanAddress = ""
			}
			app.state.Unlock()
			if err := app.persistState(); err != nil {
				app.log.WithError(err).Warn("failed to persist known bluetooth devices")
			}
		})
	}

	// network monitor emits network_status events + drives BT discoverability
	onNetTransition := func(online bool) {
		app.setOnlineState(online)

		if online {
			select {
			case app.retryNowCh <- struct{}{}:
			default:
			}
			// the network just came back everything that was talking to Spotify is now half-open
			app.client.CloseIdleConnections()
			if p := app.currentPlayer.Load(); p != nil && p.sess != nil {
				app.log.Info("network: back online, forcing dealer reconnect + flushing stale connections")
				p.sess.Dealer().ForceReconnect()
			}
		}

		if app.bt == nil {
			return
		}
		// gate for auto-PAN
		app.bt.SetOnline(online)
		app.bt.SetOfflineRetry(!online)
	}
	startNetworkMonitor(app.log, app.server, onNetTransition)
	app.startClockWatch()

	app.checkinId = readCheckinId()
	app.startCheckin()

	app.startButtonFallback()
	time.AfterFunc(3*time.Minute, func() {
		if app.server.WSClients() == 0 {
			app.log.Warnf("ui: no UI client since boot (chromium/weston down?)")
		}
	})

	// on device voice service
	if app.cfg.Voice.Enabled {
		app.voice = newVoiceService(app)
		app.server.SetVoiceHandler(app.voice)
		app.voice.Start()
	}

	return app, nil
}

// SetAuthState records whether auth is required + the OAuth URL
// sets authKnown to true so the frontend can drop its initial loading state
func (app *App) SetAuthState(required bool, url string) {
	app.authMu.Lock()
	app.authRequired = required
	app.authURL = url
	app.authKnown = true
	app.authMu.Unlock()
}

// GetAuthState returns (required, url, known)
func (app *App) GetAuthState() (required bool, url string, known bool) {
	app.authMu.RLock()
	defer app.authMu.RUnlock()
	return app.authRequired, app.authURL, app.authKnown
}

// run starts the observer daemon
func (app *App) Run(ctx context.Context) error {
	// no RTC on the Car Thing
	if err := waitForClock(ctx, app.log); err != nil {
		// only ctx.Err can come back, timeout falls through to warn + continue
		return err
	}

	switch app.cfg.Credentials.Type {
	case "interactive":
		return app.runInteractive(ctx)
	case "spotify_token":
		return app.runSpotifyToken(ctx, app.cfg.Credentials.SpotifyToken.Username, app.cfg.Credentials.SpotifyToken.AccessToken)
	default:
		return fmt.Errorf("unknown or unsupported credentials type for observer mode: %s", app.cfg.Credentials.Type)
	}
}

// max wait for NTP before proceeding anyway, downstream TLS retries handle the rest
const clockMaxWait = 60 * time.Second

func waitForClock(ctx context.Context, log librespot.Logger) error {
	const minYear = 2025
	if time.Now().Year() >= minYear {
		return nil
	}
	log.Infof("waiting for clock to reach >= %d (currently %s, max wait %s)",
		minYear, time.Now().Format(time.RFC3339), clockMaxWait)

	deadline := time.NewTimer(clockMaxWait)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			log.Warnf("clock still bad after %s (now %s), proceeding anyway, TLS will retry until NTP catches up",
				clockMaxWait, time.Now().Format(time.RFC3339))
			return nil
		case <-ticker.C:
			if time.Now().Year() >= minYear {
				log.Infof("clock is sane (%s), proceeding to Spotify session", time.Now().Format(time.RFC3339))
				return nil
			}
		}
	}
}

func (app *App) Close() error {
	if app.closed {
		return nil
	}
	app.closed = true

	if app.voice != nil {
		app.voice.Stop()
	}
	if app.bt != nil {
		app.bt.Close()
	}
	if app.server != nil {
		return app.server.Close()
	}
	return nil
}

func (app *App) persistState() error {
	if err := app.stateStore.Save(app.state); err != nil {
		return fmt.Errorf("persisting state: %w", err)
	}
	return nil
}

// GetSettings returns a copy of the persisted frontend settings or nil
func (app *App) GetSettings() []byte {
	app.state.Lock()
	defer app.state.Unlock()
	if len(app.state.Settings) == 0 {
		return nil
	}
	out := make([]byte, len(app.state.Settings))
	copy(out, app.state.Settings)
	return out
}

// PutSettings replaces the stored settings blob and persists it
func (app *App) PutSettings(body []byte) error {
	buf := make([]byte, len(body))
	copy(buf, body)
	app.state.Lock()
	app.state.Settings = buf
	app.state.Unlock()
	// keep the firmware auto_brightness service in sync
	app.mirrorBacklightConf(body)
	// react to the voice mic on/off toggle
	if app.voice != nil {
		app.voice.setWakeEnabled(voiceMicFromSettings(body))
	}
	// react to the telemetry consent card / settings toggle
	if c := checkinConsentFromSettings(body); c != "" {
		app.setCheckinConsent(c)
	}
	return app.persistState()
}

// reads the "voiceMic" preference from the UI settings
func voiceMicFromSettings(body []byte) bool {
	var s struct {
		VoiceMic *bool `json:"voiceMic"`
	}
	if json.Unmarshal(body, &s) == nil && s.VoiceMic != nil {
		return *s.VoiceMic
	}
	return true
}

// PerformReset wipes the device to a full factory state and reboots
func (app *App) PerformReset() {
	app.log.Warn("system: performing factory reset")

	// 02a-firstboot runs reset-data + reset-settings before /var is
	// mounted, reformatting /dev/data (and /dev/settings) which wipes everything
	if err := exec.Command("/usr/bin/uenv", "set", "firstboot", "1").Run(); err != nil {
		app.log.WithError(err).Error("system: 'uenv set firstboot 1' failed - falling back to surgical wipe")
		app.surgicalWipe()
	} else {
		app.log.Warn("system: firstboot=1 flagged - /dev/data will be reformatted on next boot")
	}

	time.Sleep(500 * time.Millisecond)
	app.log.Warn("system: rebooting for factory reset")
	app.reboot()
}

// fallback when the full reformat cant be flagged (uenv missing/failed)
func (app *App) surgicalWipe() {
	app.log.Warn("system: surgical wipe (reformat unavailable)")

	// BT bondings via BlueZ RemoveDevice (cleaner than rm'ing /var/lib/bluetooth)
	if app.bt != nil {
		devs, err := app.bt.GetDevices()
		if err != nil {
			app.log.WithError(err).Warn("system: failed to enumerate BT devices for removal")
		} else {
			for _, d := range devs {
				if !d.Paired {
					continue
				}
				if err := app.bt.RemoveDevice(d.Address); err != nil {
					app.log.WithError(err).Warnf("system: failed to remove BT device %s", d.Address)
				} else {
					app.log.Infof("system: removed BT bonding for %s", d.Address)
				}
			}
		}
	}

	if err := app.stateStore.Wipe(); err != nil {
		app.log.WithError(err).Warn("system: failed to wipe persisted state")
	}
	if err := exec.Command("sh", "-c", "rm -rf /var/cache/chrome_storage/* 2>/dev/null").Run(); err != nil {
		app.log.WithError(err).Warn("system: failed to wipe chromium profile")
	}
}

// reboot runs the reboot command with a busybox fallback.
func (app *App) reboot() {
	if err := exec.Command("/sbin/reboot").Run(); err != nil {
		app.log.WithError(err).Warn("system: /sbin/reboot failed, trying busybox reboot")
		if err := exec.Command("reboot").Run(); err != nil {
			app.log.WithError(err).Error("system: reboot command failed; user must power-cycle manually")
		}
	}
}

// PerformRestart reboots without wiping any state.
func (app *App) PerformRestart() {
	app.log.Warn("system: restarting")
	time.Sleep(200 * time.Millisecond) // let the HTTP 200 flush before we go down
	app.reboot()
}

// backlight is off for an extra second so a visual glitch doesnt show
func (app *App) PerformSuspend() {
	app.log.Info("system: suspending")
	script := `for bl in /sys/class/backlight/*/bl_power; do echo 4 > "$bl" 2>/dev/null; done
echo mem > /sys/power/state
sleep 1
for bl in /sys/class/backlight/*/bl_power; do echo 0 > "$bl" 2>/dev/null; done`
	if err := exec.Command("sh", "-c", script).Run(); err != nil {
		app.log.WithError(err).Warn("system: suspend script failed")
	}
	app.log.Info("system: resumed from suspend")
	app.onResume()
}

// onResume recovers state that suspend-to-RAM breaks
func (app *App) onResume() {
	p := app.currentPlayer.Load()
	app.log.Infof("system: resume recovery (player active: %t)", p != nil)

	// suspend tears down bt so on wake the PAN link is down
	if app.bt != nil {
		go app.bt.RecoverNetworkAfterResume("")
	}

	// restore button input
	go app.restoreInputAfterResume()

	// drop the half-open dealer socket so the recv loop reconnects
	if p != nil && p.sess != nil {
		p.sess.Dealer().ForceReconnect()
	}
}

// restoreInputAfterResume re-engages chromiums input
func (app *App) restoreInputAfterResume() {
	if err := app.ensureVirtualTouch(); err != nil {
		app.log.WithError(err).Warn("system: virtual touch unavailable (buttons may need a manual screen tap)")
		return
	}
	// delay so chromium + virtual device are ready
	time.Sleep(2500 * time.Millisecond)
	if err := app.emitVirtualTap(); err != nil {
		app.log.WithError(err).Warn("system: input recovery tap failed")
	}
}

// uinput ioctls
const (
	uiSetEvbit   = 0x40045564
	uiSetKeybit  = 0x40045565
	uiSetAbsbit  = 0x40045567
	uiSetPropbit = 0x4004556e
	uiDevCreate  = 0x5501
)

const (
	evSyn           = 0x00
	evKey           = 0x01
	evAbs           = 0x03
	synReport       = 0x00
	btnTouch        = 0x14a
	absMtSlot       = 0x2f
	absMtTouchMajor = 0x30
	absMtPositionX  = 0x35
	absMtPositionY  = 0x36
	absMtTrackingID = 0x39
	absMtPressure   = 0x3a
	inputPropDirect = 0x01
	absCnt          = 64
)

// ensureVirtualTouch lazily creates a uinput virtual touchscreen
func (app *App) ensureVirtualTouch() error {
	app.virtualTouchMu.Lock()
	defer app.virtualTouchMu.Unlock()
	if app.virtualTouch != nil {
		return nil
	}
	f, err := createVirtualTouchscreen()
	if err != nil {
		return err
	}
	app.virtualTouch = f
	app.log.Info("system: created uinput virtual touchscreen for resume input recovery")
	return nil
}

// emitVirtualTap sends one down+up tap through the virtual touchscreen
func (app *App) emitVirtualTap() error {
	app.virtualTouchMu.Lock()
	f := app.virtualTouch
	app.virtualTouchMu.Unlock()
	if f == nil {
		return fmt.Errorf("virtual touch not initialized")
	}

	ev := func(typ, code uint16, val int32) []byte {
		b := make([]byte, 16)
		binary.LittleEndian.PutUint16(b[8:], typ)
		binary.LittleEndian.PutUint16(b[10:], code)
		binary.LittleEndian.PutUint32(b[12:], uint32(val))
		return b
	}
	write := func(events ...[]byte) error {
		var buf []byte
		for _, e := range events {
			buf = append(buf, e...)
		}
		_, err := f.Write(buf)
		return err
	}

	// down near a corner
	if err := write(
		ev(evAbs, absMtTrackingID, 1),
		ev(evAbs, absMtPositionX, 20),
		ev(evAbs, absMtPositionY, 20),
		ev(evAbs, absMtPressure, 13),
		ev(evKey, btnTouch, 1),
		ev(evSyn, synReport, 0),
	); err != nil {
		return err
	}
	time.Sleep(40 * time.Millisecond)
	return write(
		ev(evAbs, absMtTrackingID, -1),
		ev(evAbs, absMtPressure, 0),
		ev(evKey, btnTouch, 0),
		ev(evSyn, synReport, 0),
	)
}

func createVirtualTouchscreen() (*os.File, error) {
	f, err := os.OpenFile("/dev/uinput", os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open uinput: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
		}
	}()

	ioctl := func(req, arg uintptr) error {
		if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), req, arg); errno != 0 {
			return errno
		}
		return nil
	}

	for _, ev := range []uintptr{evSyn, evKey, evAbs} {
		if err := ioctl(uiSetEvbit, ev); err != nil {
			return nil, fmt.Errorf("set evbit %d: %w", ev, err)
		}
	}
	if err := ioctl(uiSetKeybit, btnTouch); err != nil {
		return nil, fmt.Errorf("set keybit: %w", err)
	}
	if err := ioctl(uiSetPropbit, inputPropDirect); err != nil {
		return nil, fmt.Errorf("set propbit: %w", err)
	}
	for _, ax := range []uintptr{absMtSlot, absMtTouchMajor, absMtPositionX, absMtPositionY, absMtTrackingID, absMtPressure} {
		if err := ioctl(uiSetAbsbit, ax); err != nil {
			return nil, fmt.Errorf("set absbit %d: %w", ax, err)
		}
	}

	// struct uinput_user_dev
	const (
		nameLen   = 80
		idOff     = nameLen
		absmaxOff = nameLen + 8 + 4
	)
	dev := make([]byte, absmaxOff+4*absCnt*4)
	copy(dev[0:nameLen], "thing-virtual-touch")
	binary.LittleEndian.PutUint16(dev[idOff:], 0x06)
	binary.LittleEndian.PutUint16(dev[idOff+2:], 0x16c0)
	binary.LittleEndian.PutUint16(dev[idOff+4:], 0x05df)
	binary.LittleEndian.PutUint16(dev[idOff+6:], 1)
	setAbsMax := func(code, val int) {
		binary.LittleEndian.PutUint32(dev[absmaxOff+code*4:], uint32(val))
	}
	setAbsMax(absMtPositionX, 4095)
	setAbsMax(absMtPositionY, 4095)
	setAbsMax(absMtSlot, 9)
	setAbsMax(absMtTrackingID, 65535)
	setAbsMax(absMtPressure, 255)
	setAbsMax(absMtTouchMajor, 255)

	if _, err := f.Write(dev); err != nil {
		return nil, fmt.Errorf("write uinput_user_dev: %w", err)
	}
	if err := ioctl(uiDevCreate, 0); err != nil {
		return nil, fmt.Errorf("UI_DEV_CREATE: %w", err)
	}

	ok = true
	return f, nil
}

func (app *App) newAppPlayer(ctx context.Context, creds any) (_ *AppPlayer, err error) {
	appPlayer := &AppPlayer{
		app:             app,
		stop:            make(chan struct{}, 1),
		logout:          app.logoutCh,
		countryCode:     new(string),
		playbackReadyCh: make(chan struct{}),
		queueResolvedCh: make(chan struct{}, 1),
		clusterCh:       make(chan *connectpb.Cluster, 1),
	}

	appPlayer.prefetchTimer = time.NewTimer(math.MaxInt64)
	appPlayer.prefetchTimer.Stop()

	if appPlayer.sess, err = session.NewSessionFromOptions(ctx, &session.Options{
		Log:         app.log,
		DeviceType:  app.deviceType,
		DeviceId:    app.deviceId,
		ClientToken: app.clientToken,
		Resolver:    app.resolver,
		Client:      app.client,
		AppState:    app.state,
		Credentials: creds,
		AuthURLCallback: func(url string) {
			app.SetAuthState(true, url)
		},
	}); err != nil {
		return nil, err
	}

	app.SetAuthState(false, "")
	appPlayer.initState()

	// observer mode

	return appPlayer, nil
}

func (app *App) runSpotifyToken(ctx context.Context, username, token string) error {
	return app.withCredentials(ctx, session.SpotifyTokenCredentials{Username: username, Token: token})
}

func (app *App) runInteractive(ctx context.Context) error {
	return app.withCredentials(ctx, session.InteractiveCredentials{})
}

// setOnlineState flips the waitOnline barrier
func (app *App) setOnlineState(online bool) {
	app.onlineMu.Lock()
	defer app.onlineMu.Unlock()

	if app.isOnline == online {
		return
	}
	if app.isOnline && !online {
		app.netDrops++ // was online, now offline: an internet drop this session
	}
	app.isOnline = online
	if online {
		close(app.onlineCh)
		app.onlineCh = make(chan struct{})
	}
}

// waitOnline blocks until online, timeout, or ctx cancel
func (app *App) waitOnline(ctx context.Context, timeout time.Duration) error {
	app.onlineMu.Lock()
	if app.isOnline {
		app.onlineMu.Unlock()
		return nil
	}
	ch := app.onlineCh
	app.onlineMu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("waitOnline timed out")
	case <-ch:
		return nil
	}
}

// sessionRetryBackoff: 2s, 4s, 8s, 16s, 30s, 30s, ... capped at 30s
func sessionRetryBackoff(attempt int) time.Duration {
	const cap = 30 * time.Second
	// int is 32-bit on the armv6 build target, so 1<<31 overflows to a negative
	if attempt >= 5 {
		return cap
	}
	if d := time.Duration(1<<attempt) * time.Second; d < cap {
		return d
	}
	return cap
}

// newAppPlayerWithRetry retries session creation with exponential backoff
func (app *App) newAppPlayerWithRetry(ctx context.Context, creds any) (*AppPlayer, error) {
	for attempt := 0; ; attempt++ {
		// park here until network is reachable so we don't burn a 30s DNS timeout
		if err := app.waitOnline(ctx, 60*time.Second); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			app.log.WithError(err).Debug("session retry: waitOnline gave up, attempting anyway")
		}

		appPlayer, err := app.newAppPlayer(ctx, creds)
		if err == nil {
			if err = appPlayer.sess.Dealer().Connect(ctx); err != nil {
				appPlayer.Close()
				err = fmt.Errorf("failed connecting to dealer: %w", err)
			} else {
				app.log.Debugf("connected to dealer")
			}
		}
		if err == nil {
			return appPlayer, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		backoff := sessionRetryBackoff(attempt)
		app.log.WithError(err).Warnf("session attempt %d failed; retrying in %s", attempt+1, backoff)

		select {
		case <-app.retryNowCh:
		default:
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-app.retryNowCh:
			app.log.Infof("session retry: network came online, retrying immediately (skipping %s backoff)", backoff)
		case <-time.After(backoff):
		}
	}
}

func (app *App) withCredentials(ctx context.Context, creds any) (err error) {
	if len(app.state.Credentials.Data) > 0 {
		// stored creds
		app.SetAuthState(false, "")
		appPlayer, err := app.newAppPlayerWithRetry(ctx, session.StoredCredentials{
			Username: app.state.Credentials.Username,
			Data:     app.state.Credentials.Data,
		})
		if err != nil {
			return err
		}

		appPlayer.Run(ctx, app.server.Receive())
		return nil
	}

	appPlayer, err := app.newAppPlayerWithRetry(ctx, creds)
	if err != nil {
		return err
	}

	app.state.Lock()
	app.state.Credentials.Username = appPlayer.sess.Username()
	app.state.Credentials.Data = appPlayer.sess.StoredCredentials()
	app.state.Unlock()

	if err = app.persistState(); err != nil {
		return err
	}

	app.log.Debugf("stored credentials for %s", librespot.ObfuscateUsername(appPlayer.sess.Username()))
	appPlayer.Run(ctx, app.server.Receive())
	return nil
}
