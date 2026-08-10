package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

func TestCheckinWaitJitter(t *testing.T) {
	t.Parallel()
	lo, hi := checkinInterval-checkinJitter, checkinInterval+checkinJitter
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		w := checkinWait()
		if w < lo || w > hi {
			t.Fatalf("wait %v outside [%v, %v]", w, lo, hi)
		}
		seen[w] = true
	}
	if len(seen) < 2 {
		t.Fatal("wait must vary between calls or the fleet stays phase locked")
	}
}

func TestParseCpuinfoSerial(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want string
	}{
		{"amlogic", "processor\t: 0\nmodel name\t: ARMv7\nSerial\t\t: 2b0e123400abcdef\nHardware\t: Amlogic\n", "2b0e123400abcdef"},
		{"no serial line", "processor\t: 0\nHardware\t: QEMU\n", ""},
		{"empty value", "Serial\t\t:\n", ""},
		{"serial substring key ignored", "SerialNumber\t: nope\nSerial : abc\n", "abc"},
		{"empty file", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseCpuinfoSerial([]byte(tt.body)); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestCheckinIdFromSerial(t *testing.T) {
	t.Parallel()
	if got := checkinIdFromSerial(""); got != "" {
		t.Fatalf("empty serial should give no id, got %q", got)
	}
	if got := checkinIdFromSerial("0000000000000000"); got != "" {
		t.Fatalf("all-zero serial should give no id, got %q", got)
	}
	id := checkinIdFromSerial("2b0e123400abcdef")
	if len(id) != 32 {
		t.Fatalf("id should be 32 hex chars, got %d (%q)", len(id), id)
	}
	if id != checkinIdFromSerial("2b0e123400abcdef") {
		t.Fatal("id must be deterministic")
	}
	if id == checkinIdFromSerial("2b0e123400abcde0") {
		t.Fatal("different serials must give different ids")
	}
}

func TestNormalizeVersion(t *testing.T) {
	t.Parallel()
	tests := []struct{ in, want string }{
		{"v1.0.0", "1.0.0"},
		{"1.0.0", "1.0.0"},
		{" v1.2.3 ", "1.2.3"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := normalizeVersion(tt.in); got != tt.want {
			t.Errorf("normalizeVersion(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestVersionLess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b string
		want bool
	}{
		{"1.0.0", "1.1.0", true},
		{"1.1.0", "1.0.0", false},
		{"1.0.0", "1.0.0", false},
		{"1.0.0", "1.0.1", true},
		{"1.9.0", "1.10.0", true},
		{"2.0.0", "1.9.9", false},
		{"1.0", "1.0.1", true},
		{"1.0.0", "1.0.1-rc1", true},
		{"unknown", "1.1.0", false},
		{"1.0.0", "garbage", false},
		{"", "1.0.0", false},
	}
	for _, tt := range tests {
		if got := versionLess(tt.a, tt.b); got != tt.want {
			t.Errorf("versionLess(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestCheckinConsentFromSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		blob string
		want string
	}{
		{"granted", `{"v":1,"checkinConsent":"granted"}`, "granted"},
		{"denied", `{"checkinConsent":"denied"}`, "denied"},
		{"absent", `{"v":1,"brightness":5}`, ""},
		{"junk value", `{"checkinConsent":"maybe"}`, ""},
		{"not json", `nope`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkinConsentFromSettings([]byte(tt.blob)); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

type nopStateStore struct{}

func (nopStateStore) Load() (*librespot.AppState, error) { return nil, nil }
func (nopStateStore) Save(*librespot.AppState) error     { return nil }
func (nopStateStore) Wipe() error                        { return nil }

func checkinTestApp(serverURL string) *App {
	return &App{
		log:        &librespot.NullLogger{},
		cfg:        &Config{Checkin: true, CheckinURL: serverURL},
		client:     &http.Client{},
		state:      &librespot.AppState{},
		stateStore: nopStateStore{},
	}
}

func TestDoCheckinIdentified(t *testing.T) {
	t.Parallel()
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		_, _ = w.Write([]byte(`{"utc_offset_min":-240,"latest_version":"1.1.0"}`))
	}))
	defer srv.Close()

	app := checkinTestApp(srv.URL)
	if err := app.doCheckin(context.Background(), "deadbeef", "1.0.0", "granted"); err != nil {
		t.Fatal(err)
	}
	if gotQuery.Get("id") != "deadbeef" || gotQuery.Get("version") != "1.0.0" {
		t.Fatalf("unexpected query: %v", gotQuery)
	}
	if gotQuery.Has("install") {
		t.Fatal("identified check-in must not carry install")
	}
	if off := app.utcOffsetMin(); off == nil || *off != -240 {
		t.Fatalf("offset not persisted: %v", off)
	}
	if app.latestVersion() != "1.1.0" {
		t.Fatalf("latest_version not persisted: %q", app.latestVersion())
	}
	if !app.state.CheckinInstallReported {
		t.Fatal("identified success should mark the install reported")
	}
	if !app.updateAvailable() {
		if !versionLess("1.0.0", "1.1.0") {
			t.Fatal("1.1.0 should read as newer than 1.0.0")
		}
	}
}

func TestDoCheckinAnonInstallOnce(t *testing.T) {
	t.Parallel()
	var queries []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		_, _ = w.Write([]byte(`{"utc_offset_min":60,"latest_version":"1.0.0"}`))
	}))
	defer srv.Close()

	app := checkinTestApp(srv.URL)
	// first denied ping: anonymous with install=1, no id
	if err := app.doCheckin(context.Background(), "deadbeef", "1.0.0", "denied"); err != nil {
		t.Fatal(err)
	}
	// second denied ping: anonymous, no install
	if err := app.doCheckin(context.Background(), "deadbeef", "1.0.0", "denied"); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(queries))
	}
	for i, q := range queries {
		if q.Has("id") {
			t.Fatalf("request %d: opted-out ping must never carry an id: %v", i, q)
		}
	}
	if queries[0].Get("install") != "1" {
		t.Fatalf("first opted-out ping must carry install=1: %v", queries[0])
	}
	if queries[1].Has("install") {
		t.Fatalf("second opted-out ping must not carry install: %v", queries[1])
	}
	if off := app.utcOffsetMin(); off == nil || *off != 60 {
		t.Fatalf("opted-out ping must still deliver the offset: %v", off)
	}
}

func TestDoCheckinUnansweredIsUncounted(t *testing.T) {
	t.Parallel()
	var queries []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		_, _ = w.Write([]byte(`{"utc_offset_min":60,"latest_version":"1.1.0"}`))
	}))
	defer srv.Close()

	app := checkinTestApp(srv.URL)
	if err := app.doCheckin(context.Background(), "deadbeef", "1.0.0", ""); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 1 {
		t.Fatalf("expected 1 request, got %d", len(queries))
	}
	if queries[0].Has("id") || queries[0].Has("install") {
		t.Fatalf("an unanswered card must send nothing countable: %v", queries[0])
	}
	if off := app.utcOffsetMin(); off == nil || *off != 60 {
		t.Fatalf("clock must work before the card is answered: %v", off)
	}
	if app.latestVersion() != "1.1.0" {
		t.Fatalf("updates must work before the card is answered: %q", app.latestVersion())
	}
	if app.state.CheckinInstallReported {
		t.Fatal("an uncounted ping must not burn the one-time install report")
	}

	// answering "no thanks" later still gets its single install ping
	if err := app.doCheckin(context.Background(), "deadbeef", "1.0.0", "denied"); err != nil {
		t.Fatal(err)
	}
	if queries[1].Get("install") != "1" {
		t.Fatalf("the deferred install ping must still fire: %v", queries[1])
	}
}

func TestDoCheckinMissingOffset(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	app := checkinTestApp(srv.URL)
	if err := app.doCheckin(context.Background(), "deadbeef", "1.0.0", "granted"); err == nil {
		t.Fatal("expected error when the response carries no offset")
	}
	if app.utcOffsetMin() != nil {
		t.Fatal("a response without an offset must not persist one")
	}
}

func TestDoCheckinServerError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	app := checkinTestApp(srv.URL)
	if err := app.doCheckin(context.Background(), "deadbeef", "1.0.0", "granted"); err == nil {
		t.Fatal("expected error on 500")
	}
	if app.utcOffsetMin() != nil {
		t.Fatal("failed request must not persist an offset")
	}
	if app.hasCheckedInEver() {
		t.Fatal("failed request must not count as a success")
	}
}
