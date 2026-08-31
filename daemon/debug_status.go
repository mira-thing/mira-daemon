package daemon

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

const daemonLogPath = "/var/log/go-librespot/current"

// gathers the diagnostic for the debug screen
func (app *App) DebugStatus() DebugStatusPayload {
	free, total := readMeminfoMB()
	clockTime, clockOK := clockInfo()
	p := DebugStatusPayload{
		FirmwareVersion:    firmwareVersion(),
		DaemonVersion:      librespot.VersionNumberString(),
		UptimeSecs:         readUptimeSecs(),
		DaemonUptimeSecs:   int64(time.Since(app.startedAt).Seconds()),
		ClockTime:          clockTime,
		ClockOK:            clockOK,
		ClockLastStep:      app.clockSteps.Last(),
		RAMFreeMB:          free,
		RAMTotalMB:         total,
		DiskFreeMB:         diskFreeMB("/var"),
		TempC:              readTempC(),
		Load1:              readLoadAvg1(),
		WSClients:          app.server.WSClients(),
		DNSServers:         dnsServerCount(),
		USBBounces:         usbBounces(),
		Spotify:            app.spotifyState(),
		VoiceEnabled:       app.cfg.Voice.Enabled,
		VoiceReady:         app.voice != nil && app.voice.sherpa != nil && app.voice.sherpa.isReady(),
		CheckinLastSuccess: app.checkinStatus.LastSuccess(),
		CheckinLastError:   app.checkinStatus.LastError(),
		RecentProblems:     RecentProblems(6),
		PreviousProblems:   PreviousProblems(4),
	}

	app.onlineMu.Lock()
	p.Online = app.isOnline
	p.InternetDrops = app.netDrops
	app.onlineMu.Unlock()

	p.NetworkPath, p.IP = networkPathAndIP()

	if app.bt == nil {
		p.PhoneVolume = "no bluetooth"
		return p
	}

	p.TetherHealth = app.bt.TetherRouteState()

	if devs, err := app.bt.GetDevices(); err == nil {
		for _, d := range devs {
			if !d.Connected {
				continue
			}
			switch {
			case d.Name != "":
				p.BluetoothDevice = d.Name
			case d.Alias != "":
				p.BluetoothDevice = d.Alias
			default:
				p.BluetoothDevice = d.Address
			}
			break
		}
	}

	state, lastErr, present := app.bt.Iap2Status()
	if !present {
		p.PhoneVolume = "unavailable (build issue)"
	} else {
		p.PhoneVolume = state
	}
	p.PhoneVolumeErr = lastErr

	p.AndroidVolumeSent = app.bt.HIDVolumeSent()

	reg, sub, dead := app.bt.HIDVolumeStatus()
	switch {
	case sub:
		p.AndroidVolume = "ready"
	case dead:
		p.AndroidVolume = "phone not accepting volume keys (toggle phone Bluetooth to fix)"
	case reg:
		p.AndroidVolume = "registered (phone not subscribed)"
	default:
		p.AndroidVolume = "off (" + app.bt.HIDVolumeAdvState() + ")"
	}

	return p
}

func (app *App) spotifyState() string {
	app.state.Lock()
	user := app.state.Credentials.Username
	app.state.Unlock()
	if app.server != nil && app.server.PlayerReady() {
		if user != "" {
			return "signed in as " + user
		}
		return "signed in"
	}
	if user != "" {
		return "connecting"
	}
	return "waiting for sign-in"
}

func firmwareVersion() string {
	b, err := os.ReadFile("/etc/mira/version.txt")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(b))
}

// reports which interface actually carries internet
func networkPathAndIP() (path, ip string) {
	if name := defaultRouteIface(); name != "" {
		if p, v4 := ifaceLabelAndIP(name); v4 != "" {
			return p, v4
		}
	}
	for _, name := range []string{"bnep0", "usb1", "usb0"} {
		if p, v4 := ifaceLabelAndIP(name); v4 != "" {
			return p, v4
		}
	}
	return "none", ""
}

func defaultRouteIface() string {
	b, err := os.ReadFile("/proc/net/route")
	if err != nil {
		return ""
	}
	best, bestMetric := "", int(^uint(0)>>1)
	for i, line := range strings.Split(string(b), "\n") {
		if i == 0 {
			continue // header
		}
		f := strings.Fields(line)
		if len(f) < 7 || f[1] != "00000000" {
			continue
		}
		metric, _ := strconv.Atoi(f[6])
		if metric < bestMetric {
			best, bestMetric = f[0], metric
		}
	}
	return best
}

func ifaceLabelAndIP(name string) (label, ip string) {
	iface, err := net.InterfaceByName(name)
	if err != nil || iface.Flags&net.FlagUp == 0 {
		return "", ""
	}
	addrs, _ := iface.Addrs()
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		v4 := ipn.IP.To4()
		if v4 == nil || v4.IsLinkLocalUnicast() {
			continue
		}
		switch {
		case name == "bnep0":
			return "Bluetooth", v4.String()
		case strings.HasPrefix(name, "usb"):
			return "USB", v4.String()
		default:
			return name, v4.String()
		}
	}
	return "", ""
}

// returns the hottest thermal zone in C
func readTempC() int {
	max := 0
	for i := 0; i < 8; i++ {
		b, err := os.ReadFile("/sys/class/thermal/thermal_zone" + strconv.Itoa(i) + "/temp")
		if err != nil {
			break
		}
		milli, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil || milli <= 0 {
			continue
		}
		if c := milli / 1000; c > max {
			max = c
		}
	}
	return max
}

func readLoadAvg1() string {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return ""
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return ""
	}
	return f[0]
}

func readUptimeSecs() int64 {
	b, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return 0
	}
	secs, _ := strconv.ParseFloat(f[0], 64)
	return int64(secs)
}

func readMeminfoMB() (freeMB, totalMB int) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		kb, _ := strconv.Atoi(fields[1]) // value is in kB
		switch fields[0] {
		case "MemTotal:":
			totalMB = kb / 1024
		case "MemAvailable:":
			freeMB = kb / 1024
		}
	}
	return freeMB, totalMB
}

func diskFreeMB(path string) int {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0
	}
	return int(uint64(st.Bavail) * uint64(st.Bsize) / (1024 * 1024))
}

func clockInfo() (t string, ok bool) {
	now := time.Now()
	ok = now.After(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	return now.Format("2006-01-02 15:04"), ok
}

func dnsServerCount() int {
	b, err := os.ReadFile("/var/local/etc/resolv.conf")
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "nameserver ") {
			n++
		}
	}
	return n
}

func usbBounces() int {
	b, err := os.ReadFile("/run/dhclient-usb0.bounce")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(f[0])
	return n
}

func tailFile(path string, n int) []string {
	f, err := os.Open(path)
	if err != nil {
		return []string{"(no log at " + path + ")"}
	}
	defer f.Close()
	ring := make([]string, 0, n)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		ring = append(ring, sc.Text())
		if len(ring) > n {
			ring = ring[len(ring)-n:]
		}
	}
	return ring
}
