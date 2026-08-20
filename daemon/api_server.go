package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	librespot "github.com/devgianlu/go-librespot"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
	"github.com/rs/cors"
)

const (
	// per-client websocket write deadline; a write that can't finish in this
	// window means the client is wedged and gets dropped
	wsWriteTimeout = 2 * time.Second
	// buffered events per client before Emit starts dropping. Every event we emit
	// is a full-state snapshot, so a dropped one is superseded by the next.
	wsClientBuffer      = 64
	wsHeartbeatInterval = 15 * time.Second
)

// returns (required, url, known). known=false while we haven't yet determined auth state
type AuthStatusFunc func() (required bool, url string, known bool)

type ApiServer interface {
	Emit(ev *ApiEvent)
	Receive() <-chan ApiRequest
	Close() error

	// /auth/status uses this, nil = endpoint returns 503
	SetAuthHandler(fn AuthStatusFunc)

	// /bluetooth/* uses this, nil = 503
	SetBluetoothHandler(bm BluetoothHandler)

	// while false, fast-fail 503 instead of hanging during OAuth pairing
	SetPlayerReady(ready bool)
	PlayerReady() bool
	// connected /events websocket clients (0 = the on-device UI lost its socket)
	WSClients() int

	OnPlayerReady(fn func())

	// /system/* uses this, nil = 503
	SetSystemHandler(h SystemHandler)

	// /debug/* uses this, nil = 503
	SetDebugHandler(h DebugHandler)

	// /settings uses this, nil = 503
	SetSettingsHandler(h SettingsHandler)

	// /voice/* uses this, nil = 503
	SetVoiceHandler(h VoiceHandler)

	Submit(ctx context.Context, t ApiRequestType, data any) (any, error)
}

// destructive whole-device actions
type SystemHandler interface {
	PerformReset()
	PerformRestart()
	PerformSuspend()
}

// produces the on device debug screens status snapshot + logs.
type DebugHandler interface {
	DebugStatus() DebugStatusPayload
	DebugBundle() []byte
	SendReport() (string, error)
}

// the diagnostic shown on the debug screen
type DebugStatusPayload struct {
	FirmwareVersion  string `json:"firmware_version"`
	DaemonVersion    string `json:"daemon_version"`
	UptimeSecs       int64  `json:"uptime_secs"`
	DaemonUptimeSecs int64  `json:"daemon_uptime_secs"`
	ClockTime        string `json:"clock_time"`
	ClockOK          bool   `json:"clock_ok"`
	ClockLastStep    string `json:"clock_last_step,omitempty"`
	RAMFreeMB        int    `json:"ram_free_mb"`
	RAMTotalMB       int    `json:"ram_total_mb"`
	DiskFreeMB       int    `json:"disk_free_mb"`
	TempC            int    `json:"temp_c"`
	Load1            string `json:"load_1m"`
	WSClients        int    `json:"ws_clients"`

	Online        bool   `json:"online"`
	NetworkPath   string `json:"network_path"`
	IP            string `json:"ip"`
	DNSServers    int    `json:"dns_servers"`
	USBBounces    int    `json:"usb_bounces"`
	InternetDrops int    `json:"internet_drops"`
	TetherHealth  string `json:"tether_health"`

	Spotify string `json:"spotify"`

	BluetoothDevice string `json:"bluetooth_device"`
	PhoneVolume     string `json:"phone_volume"`
	PhoneVolumeErr  string `json:"phone_volume_err"`
	AndroidVolume   string `json:"android_volume"`

	VoiceEnabled bool `json:"voice_enabled"`
	VoiceReady   bool `json:"voice_ready"`

	CheckinLastSuccess string `json:"checkin_last_success,omitempty"`
	CheckinLastError   string `json:"checkin_last_error,omitempty"`

	RecentProblems   []string `json:"recent_problems"`
	PreviousProblems []string `json:"previous_problems"`
}

// durable store for the frontends settings
type SettingsHandler interface {
	GetSettings() []byte
	PutSettings(body []byte) error
}

type ConcreteApiServer struct {
	log librespot.Logger

	allowOrigin string
	certFile    string
	keyFile     string

	close    atomic.Bool
	listener net.Listener

	requests chan ApiRequest

	clients     []*wsClient
	clientsLock sync.RWMutex

	authMu sync.RWMutex
	authFn AuthStatusFunc

	btMu sync.RWMutex
	btFn BluetoothHandler

	// read on every channel-bound HTTP request
	playerReady atomic.Bool

	readyMu  sync.Mutex
	readyFns []func()

	sysMu sync.RWMutex
	sysFn SystemHandler

	dbgMu sync.RWMutex
	dbgFn DebugHandler

	setMu sync.RWMutex
	setFn SettingsHandler

	voiceMu sync.RWMutex
	voiceFn VoiceHandler
}

var (
	ErrNoSession        = errors.New("no session")
	ErrBadRequest       = errors.New("bad request")
	ErrForbidden        = errors.New("forbidden")
	ErrNotFound         = errors.New("not found")
	ErrMethodNotAllowed = errors.New("method not allowed")
	ErrTooManyRequests  = errors.New("the app has exceeded its rate limits")
)

type ApiRequestType string

const (
	ApiRequestTypeRoot                ApiRequestType = "root"
	ApiRequestTypeWebApi              ApiRequestType = "web_api"
	ApiRequestTypeStatus              ApiRequestType = "status"
	ApiRequestTypeResume              ApiRequestType = "resume"
	ApiRequestTypePause               ApiRequestType = "pause"
	ApiRequestTypePlayPause           ApiRequestType = "playpause"
	ApiRequestTypeSeek                ApiRequestType = "seek"
	ApiRequestTypePrev                ApiRequestType = "prev"
	ApiRequestTypeNext                ApiRequestType = "next"
	ApiRequestTypePlay                ApiRequestType = "play"
	ApiRequestTypeSearch              ApiRequestType = "search"
	ApiRequestTypeCatalogPage         ApiRequestType = "catalog_page"
	ApiRequestTypeResumeLast          ApiRequestType = "resume_last"
	ApiRequestTypeGetVolume           ApiRequestType = "get_volume"
	ApiRequestTypeSetVolume           ApiRequestType = "set_volume"
	ApiRequestTypeSetRepeatingContext ApiRequestType = "repeating_context"
	ApiRequestTypeSetRepeatingTrack   ApiRequestType = "repeating_track"
	ApiRequestTypeSetShufflingContext ApiRequestType = "shuffling_context"
	ApiRequestTypeDjSignal            ApiRequestType = "dj_signal"
	ApiRequestTypeAddToQueue          ApiRequestType = "add_to_queue"
	ApiRequestTypeGetSaved            ApiRequestType = "get_saved"
	ApiRequestTypeSetSaved            ApiRequestType = "set_saved"
	ApiRequestTypeToken               ApiRequestType = "token"
	ApiRequestTypeObserverStatus      ApiRequestType = "observer_status"
	ApiRequestTypeConnectDevices      ApiRequestType = "connect_devices"
	ApiRequestTypeTransfer            ApiRequestType = "transfer"
	ApiRequestTypeLyrics              ApiRequestType = "lyrics"
)

type ApiEventType string

const (
	ApiEventTypePlaying              ApiEventType = "playing"
	ApiEventTypeNotPlaying           ApiEventType = "not_playing"
	ApiEventTypeWillPlay             ApiEventType = "will_play"
	ApiEventTypePaused               ApiEventType = "paused"
	ApiEventTypeActive               ApiEventType = "active"
	ApiEventTypeInactive             ApiEventType = "inactive"
	ApiEventTypeMetadata             ApiEventType = "metadata"
	ApiEventTypeVolume               ApiEventType = "volume"
	ApiEventTypeSeek                 ApiEventType = "seek"
	ApiEventTypeStopped              ApiEventType = "stopped"
	ApiEventTypeRepeatTrack          ApiEventType = "repeat_track"
	ApiEventTypeRepeatContext        ApiEventType = "repeat_context"
	ApiEventTypeShuffleContext       ApiEventType = "shuffle_context"
	ApiEventTypePlaybackReady        ApiEventType = "playback_ready"
	ApiEventTypeObserverTrackChanged ApiEventType = "observer_track_changed"
	ApiEventTypeObserverStateChanged ApiEventType = "observer_state_changed"
	ApiEventTypeObserverInactive     ApiEventType = "observer_inactive"
	ApiEventTypeConnectDevices       ApiEventType = "connect_devices"

	ApiEventTypeBluetoothPairing           ApiEventType = "bluetooth/pairing"
	ApiEventTypeBluetoothPairingCancelled  ApiEventType = "bluetooth/pairing/cancelled"
	ApiEventTypeBluetoothPaired            ApiEventType = "bluetooth/paired"
	ApiEventTypeBluetoothConnect           ApiEventType = "bluetooth/connect"
	ApiEventTypeBluetoothDisconnect        ApiEventType = "bluetooth/disconnect"
	ApiEventTypeBluetoothNetworkConnect    ApiEventType = "bluetooth/network/connect"
	ApiEventTypeBluetoothNetworkDisconnect ApiEventType = "bluetooth/network/disconnect"
	ApiEventTypeNetworkStatus              ApiEventType = "network_status"
	ApiEventTypeVoice                      ApiEventType = "voice"
	ApiEventTypeSetupProgress              ApiEventType = "setup_progress"
	ApiEventTypePing                       ApiEventType = "ping"
)

type ApiRequest struct {
	Type ApiRequestType
	Data any

	resp chan apiResponse
}

func (r *ApiRequest) Reply(data any, err error) {
	r.resp <- apiResponse{data, err}
}

// NewApiRequest builds an ApiRequest pre-wired with a reply channel, plus a
// wait function that blocks until the daemon calls Reply (or ctx is done).
func NewApiRequest(t ApiRequestType, data any) (req ApiRequest, wait func(context.Context) (any, error)) {
	ch := make(chan apiResponse, 1)
	req = ApiRequest{Type: t, Data: data, resp: ch}
	wait = func(ctx context.Context) (any, error) {
		select {
		case r := <-ch:
			return r.data, r.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return
}

type ApiRequestDataSeek struct {
	Position int64 `json:"position"`
	Relative bool  `json:"relative"`
}

type ApiRequestDataVolume struct {
	Volume   int32 `json:"volume"`
	Relative bool  `json:"relative"`
}

type ApiRequestDataWebApi struct {
	Method string
	Path   string
	Query  url.Values
}

type ApiRequestDataLyrics struct {
	TrackId    string
	TrackName  string
	ArtistName string
	AlbumName  string
	DurationMs int
	Episode    bool
	Richsync   bool
}

type ApiRequestDataPlay struct {
	Uri       string `json:"uri"`
	SkipToUri string `json:"skip_to_uri"`
	Paused    bool   `json:"paused"`
	Shuffle   *bool  `json:"shuffle,omitempty"`
}

type ApiRequestDataTransfer struct {
	DeviceId string `json:"device_id"`
}

type ApiRequestDataNext struct {
	Uri *string `json:"uri"`
}

type ApiRequestDataSaved struct {
	Uri   string `json:"uri"`
	Saved bool   `json:"saved"`
}

type apiResponse struct {
	data any
	err  error
}

type ApiResponseStatusTrack struct {
	Uri           string   `json:"uri"`
	Name          string   `json:"name"`
	ArtistNames   []string `json:"artist_names"`
	AlbumName     string   `json:"album_name"`
	AlbumCoverUrl *string  `json:"album_cover_url"`
	Position      int64    `json:"position"`
	Duration      int      `json:"duration"`
	ReleaseDate   string   `json:"release_date"`
	TrackNumber   int      `json:"track_number"`
	DiscNumber    int      `json:"disc_number"`
}

func getBestImageIdForSize(images []*metadatapb.Image, size string) []byte {
	if len(images) == 0 {
		return nil
	}

	imageSize := metadatapb.Image_Size(metadatapb.Image_Size_value[strings.ToUpper(size)])

	dist := func(a metadatapb.Image_Size) int {
		diff := int(a) - int(imageSize)
		if diff < 0 {
			return -diff
		}
		return diff
	}

	// Find an image with the exact requested size.
	// If no exact match, return the closest image to the requested size.
	var bestImage *metadatapb.Image
	for _, img := range images {
		if img.Size == nil {
			continue
		}

		if *img.Size == imageSize {
			return img.FileId
		}

		// Find the image with the closest size. This logic works because the
		// metadatapb.Image_Size enum values are ordered from smallest to largest.
		if bestImage == nil || dist(*img.Size) < dist(*bestImage.Size) {
			bestImage = img
		}
	}

	if bestImage != nil {
		return bestImage.FileId
	}

	// Fallback to the first image if none have size information.
	return images[0].FileId
}

func (p *AppPlayer) newApiResponseStatusTrack(media *librespot.Media, position int64) *ApiResponseStatusTrack {
	if media.IsTrack() {
		track := media.Track()

		var artists []string
		for _, a := range track.Artist {
			artists = append(artists, *a.Name)
		}

		albumCoverId := getBestImageIdForSize(track.Album.Cover, p.app.cfg.ImageSize)
		if albumCoverId == nil && track.Album.CoverGroup != nil {
			albumCoverId = getBestImageIdForSize(track.Album.CoverGroup.Image, p.app.cfg.ImageSize)
		}

		return &ApiResponseStatusTrack{
			Uri:           librespot.SpotifyIdFromGid(librespot.SpotifyIdTypeTrack, track.Gid).Uri(),
			Name:          *track.Name,
			ArtistNames:   artists,
			AlbumName:     *track.Album.Name,
			AlbumCoverUrl: p.prodInfo.ImageUrl(albumCoverId),
			Position:      position,
			Duration:      int(*track.Duration),
			ReleaseDate:   track.Album.Date.String(),
			TrackNumber:   int(*track.Number),
			DiscNumber:    int(*track.DiscNumber),
		}
	} else {
		episode := media.Episode()

		albumCoverId := getBestImageIdForSize(episode.CoverImage.Image, p.app.cfg.ImageSize)

		return &ApiResponseStatusTrack{
			Uri:           librespot.SpotifyIdFromGid(librespot.SpotifyIdTypeEpisode, episode.Gid).Uri(),
			Name:          *episode.Name,
			ArtistNames:   []string{*episode.Show.Name},
			AlbumName:     *episode.Show.Name,
			AlbumCoverUrl: p.prodInfo.ImageUrl(albumCoverId),
			Position:      position,
			Duration:      int(*episode.Duration),
			ReleaseDate:   "",
			TrackNumber:   0,
			DiscNumber:    0,
		}
	}
}

type ApiResponseStatus struct {
	Username       string                  `json:"username"`
	DeviceId       string                  `json:"device_id"`
	DeviceType     string                  `json:"device_type"`
	DeviceName     string                  `json:"device_name"`
	PlayOrigin     string                  `json:"play_origin"`
	Stopped        bool                    `json:"stopped"`
	Paused         bool                    `json:"paused"`
	Buffering      bool                    `json:"buffering"`
	Volume         uint32                  `json:"volume"`
	VolumeSteps    uint32                  `json:"volume_steps"`
	RepeatContext  bool                    `json:"repeat_context"`
	RepeatTrack    bool                    `json:"repeat_track"`
	ShuffleContext bool                    `json:"shuffle_context"`
	Track          *ApiResponseStatusTrack `json:"track"`
}

type ApiResponseRoot struct {
	PlaybackReady bool `json:"playback_ready"`
}

type ApiResponseVolume struct {
	Value uint32 `json:"value"`
	Max   uint32 `json:"max"`
}

type ApiResponseToken struct {
	Token string `json:"token"`
}

type ApiEvent struct {
	Type ApiEventType `json:"type"`
	Data any          `json:"data"`
}

type ApiEventDataMetadata ApiResponseStatusTrack

type ApiEventDataVolume ApiResponseVolume

type ApiEventDataPlaying struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	Resume     bool   `json:"resume"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataWillPlay struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataNotPlaying struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataPaused struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataStopped struct {
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataSeek struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	Position   int    `json:"position"`
	Duration   int    `json:"duration"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataRepeatTrack struct {
	Value bool `json:"value"`
}

type ApiEventDataRepeatContext struct {
	Value bool `json:"value"`
}

type ApiEventDataShuffleContext struct {
	Value bool `json:"value"`
}

func NewApiServer(log librespot.Logger, address string, port int, allowOrigin string, certFile string, keyFile string) (_ ApiServer, err error) {
	s := &ConcreteApiServer{log: log, allowOrigin: allowOrigin, certFile: certFile, keyFile: keyFile}
	s.requests = make(chan ApiRequest)

	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		return nil, fmt.Errorf("failed starting api listener: %w", err)
	}

	log.Infof("api server listening on %s", s.listener.Addr())

	go s.serve()
	return s, nil
}

type StubApiServer struct {
	log librespot.Logger
}

func NewStubApiServer(log librespot.Logger) (ApiServer, error) {
	return &StubApiServer{log: log}, nil
}

func (s *StubApiServer) Emit(ev *ApiEvent) {
	s.log.Tracef("voiding websocket event: %s", ev.Type)
}

func (s *StubApiServer) Receive() <-chan ApiRequest {
	return make(<-chan ApiRequest)
}

func (s *StubApiServer) Close() error {
	return nil
}

func (s *StubApiServer) SetAuthHandler(_ AuthStatusFunc) {}

func (s *StubApiServer) SetPlayerReady(_ bool) {}

func (s *StubApiServer) PlayerReady() bool { return false }

func (s *StubApiServer) WSClients() int { return 0 }

func (s *StubApiServer) OnPlayerReady(_ func()) {}

func (s *StubApiServer) SetSystemHandler(_ SystemHandler)     {}
func (s *StubApiServer) SetDebugHandler(_ DebugHandler)       {}
func (s *StubApiServer) SetSettingsHandler(_ SettingsHandler) {}

func (s *StubApiServer) SetBluetoothHandler(_ BluetoothHandler) {}

func (s *ConcreteApiServer) SetAuthHandler(fn AuthStatusFunc) {
	s.authMu.Lock()
	s.authFn = fn
	s.authMu.Unlock()
}

func (s *ConcreteApiServer) getAuthHandler() AuthStatusFunc {
	s.authMu.RLock()
	defer s.authMu.RUnlock()
	return s.authFn
}

func (s *ConcreteApiServer) PlayerReady() bool { return s.playerReady.Load() }

func (s *ConcreteApiServer) WSClients() int {
	s.clientsLock.RLock()
	defer s.clientsLock.RUnlock()
	return len(s.clients)
}

// while false, channel-bound endpoints short-circuit instead of blocking
func (s *ConcreteApiServer) SetPlayerReady(ready bool) {
	prev := s.playerReady.Swap(ready)
	if !ready || prev {
		return
	}
	s.readyMu.Lock()
	fns := s.readyFns
	s.readyFns = nil
	s.readyMu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

func (s *ConcreteApiServer) OnPlayerReady(fn func()) {
	s.readyMu.Lock()
	if s.playerReady.Load() {
		s.readyMu.Unlock()
		fn()
		return
	}
	s.readyFns = append(s.readyFns, fn)
	s.readyMu.Unlock()
}

func (s *ConcreteApiServer) SetBluetoothHandler(bm BluetoothHandler) {
	s.btMu.Lock()
	s.btFn = bm
	s.btMu.Unlock()
}

func (s *ConcreteApiServer) getBluetoothHandler() BluetoothHandler {
	s.btMu.RLock()
	defer s.btMu.RUnlock()
	return s.btFn
}

func (s *ConcreteApiServer) SetSystemHandler(h SystemHandler) {
	s.sysMu.Lock()
	s.sysFn = h
	s.sysMu.Unlock()
}

func (s *ConcreteApiServer) getSystemHandler() SystemHandler {
	s.sysMu.RLock()
	defer s.sysMu.RUnlock()
	return s.sysFn
}

func (s *ConcreteApiServer) SetDebugHandler(h DebugHandler) {
	s.dbgMu.Lock()
	s.dbgFn = h
	s.dbgMu.Unlock()
}

func (s *ConcreteApiServer) getDebugHandler() DebugHandler {
	s.dbgMu.RLock()
	defer s.dbgMu.RUnlock()
	return s.dbgFn
}

func (s *ConcreteApiServer) SetSettingsHandler(h SettingsHandler) {
	s.setMu.Lock()
	s.setFn = h
	s.setMu.Unlock()
}

func (s *ConcreteApiServer) getSettingsHandler() SettingsHandler {
	s.setMu.RLock()
	defer s.setMu.RUnlock()
	return s.setFn
}

// ambientLuxPath is the tmd2772 ambient-light reading (lux) exposed by the
// kernels iio subsystem
const ambientLuxPath = "/sys/bus/iio/devices/iio:device0/in_illuminance0_input"

func readAmbientLux() (int, bool) {
	b, err := os.ReadFile(ambientLuxPath)
	if err != nil {
		return 0, false
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, false
	}
	return v, true
}

func (s *ConcreteApiServer) handleRequest(req ApiRequest, w http.ResponseWriter) {
	// pre-session, nobody is reading the channel yet, fail fast
	if !s.playerReady.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	req.resp = make(chan apiResponse, 1)
	s.requests <- req
	resp := <-req.resp

	if resp.err != nil {
		switch {
		case errors.Is(resp.err, ErrNoSession):
			w.WriteHeader(http.StatusNoContent)
			return
		case errors.Is(resp.err, ErrForbidden):
			w.WriteHeader(http.StatusForbidden)
			return
		case errors.Is(resp.err, ErrNotFound):
			w.WriteHeader(http.StatusNotFound)
			return
		case errors.Is(resp.err, ErrMethodNotAllowed):
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		case errors.Is(resp.err, ErrTooManyRequests):
			w.WriteHeader(http.StatusTooManyRequests)
			return
		case errors.Is(resp.err, ErrBadRequest):
			w.WriteHeader(http.StatusBadRequest)
			return
		default:
			s.log.WithError(resp.err).Errorf("failed handling request %s", req.Type)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	switch respData := resp.data.(type) {
	case []byte:
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(respData)
	default:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(respData)
	}
}

func jsonDecode(r *http.Request, v any) error {
	defer func() { _ = r.Body.Close() }()

	data, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	} else if len(data) == 0 {
		return nil
	}

	return json.Unmarshal(data, v)
}

func (s *ConcreteApiServer) heartbeatLoop() {
	t := time.NewTicker(wsHeartbeatInterval)
	defer t.Stop()
	for range t.C {
		if s.close.Load() {
			return
		}
		s.Emit(&ApiEvent{Type: ApiEventTypePing})
	}
}

func (s *ConcreteApiServer) serve() {
	go s.heartbeatLoop()

	m := http.NewServeMux()
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeRoot}, w)
	})
	m.Handle("/web-api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleRequest(ApiRequest{
			Type: ApiRequestTypeWebApi,
			Data: ApiRequestDataWebApi{
				Method: r.Method,
				Path:   strings.TrimPrefix(r.URL.Path, "/web-api/"),
				Query:  r.URL.Query(),
			},
		}, w)
	}))
	m.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeStatus}, w)
	})
	m.HandleFunc("/debug/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		h := s.getDebugHandler()
		if h == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		_ = json.NewEncoder(w).Encode(h.DebugStatus())
	})
	m.HandleFunc("/debug/bundle", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		h := s.getDebugHandler()
		if h == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Disposition", `attachment; filename="mira-support-bundle.tar.gz"`)
		_, _ = w.Write(h.DebugBundle())
	})
	m.HandleFunc("/debug/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		h := s.getDebugHandler()
		if h == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		id, err := h.SendReport()
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})
	// the frontend forwards its uncaught errors here so they land in the
	// recent-problems ring; the UI is otherwise a black box in the field
	m.HandleFunc("/debug/ui-error", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Message string `json:"message"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil || body.Message == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(body.Message) > 300 {
			body.Message = body.Message[:300]
		}
		s.log.Warnf("ui: %s", body.Message)
		w.WriteHeader(http.StatusNoContent)
	})
	m.HandleFunc("/observer/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// pre-session fast path so the frontend can route to auth/idle
		if !s.playerReady.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"active":  false,
				"message": "starting up",
			})
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeObserverStatus}, w)
	})
	m.HandleFunc("/connect/devices", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// no cluster yet so no devices
		if !s.playerReady.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
			_ = json.NewEncoder(w).Encode(map[string]any{"devices": []any{}})
			return
		}
		s.handleRequest(ApiRequest{Type: ApiRequestTypeConnectDevices}, w)
	})
	m.HandleFunc("/connect/transfer", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var data ApiRequestDataTransfer
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if data.DeviceId == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.handleRequest(ApiRequest{Type: ApiRequestTypeTransfer, Data: data}, w)
	})
	// /auth/status must answer before any AppPlayer exists
	m.HandleFunc("/auth/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		fn := s.getAuthHandler()
		if fn == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		required, url, known := fn()
		resp := map[string]any{
			"required": required,
			"loading":  !known,
		}
		if required && url != "" {
			resp["url"] = url
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		_ = json.NewEncoder(w).Encode(resp)
	})
	m.HandleFunc("/lyrics/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// extract track ID from path: /lyrics/{trackId}
		trackId := strings.TrimPrefix(r.URL.Path, "/lyrics/")
		if trackId == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// get track metadata from query params
		trackName := r.URL.Query().Get("track")
		artistName := r.URL.Query().Get("artist")
		albumName := r.URL.Query().Get("album")
		durationMs := 0
		if d := r.URL.Query().Get("duration"); d != "" {
			if v, err := strconv.Atoi(d); err == nil {
				durationMs = v
			}
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeLyrics, Data: ApiRequestDataLyrics{
			TrackId:    trackId,
			TrackName:  trackName,
			ArtistName: artistName,
			AlbumName:  albumName,
			DurationMs: durationMs,
			Episode:    r.URL.Query().Get("episode") == "1",
			Richsync:   r.URL.Query().Get("richsync") == "1",
		}}, w)
	})
	m.HandleFunc("/player/play", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data ApiRequestDataPlay
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(data.Uri) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypePlay, Data: data}, w)
	})
	m.HandleFunc("/player/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var data ApiRequestDataSearch
		if err := jsonDecode(r, &data); err != nil || strings.TrimSpace(data.Query) == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.handleRequest(ApiRequest{Type: ApiRequestTypeSearch, Data: data}, w)
	})
	// /voice/trigger drives the in-daemon voice flow without the mic
	m.HandleFunc("/voice/trigger", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var data struct {
			Transcript string `json:"transcript"`
			Clip       string `json:"clip"`
		}
		_ = jsonDecode(r, &data) // body is optional
		s.voiceMu.RLock()
		fn := s.voiceFn
		s.voiceMu.RUnlock()
		if fn == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		text, err := fn.TriggerVoice(r.Context(), data.Transcript, data.Clip)
		if err != nil {
			s.log.WithError(err).Warn("voice trigger failed")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"text": text})
	})
	m.HandleFunc("/player/resume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeResume}, w)
	})
	// resume playback on the last active device
	m.HandleFunc("/player/resume_last", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeResumeLast}, w)
	})
	m.HandleFunc("/player/pause", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypePause}, w)
	})
	m.HandleFunc("/player/playpause", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypePlayPause}, w)
	})
	m.HandleFunc("/player/next", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data ApiRequestDataNext
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeNext, Data: data}, w)
	})
	m.HandleFunc("/player/prev", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypePrev}, w)
	})
	m.HandleFunc("/player/seek", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data ApiRequestDataSeek
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if !data.Relative && data.Position < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeSeek, Data: data}, w)
	})
	m.HandleFunc("/player/volume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			s.handleRequest(ApiRequest{Type: ApiRequestTypeGetVolume}, w)
		} else if r.Method == "POST" {
			var data ApiRequestDataVolume
			if err := jsonDecode(r, &data); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if !data.Relative && data.Volume < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			s.handleRequest(ApiRequest{Type: ApiRequestTypeSetVolume, Data: data}, w)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	m.HandleFunc("/player/repeat_context", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Repeat bool `json:"repeat_context"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeSetRepeatingContext, Data: data.Repeat}, w)
	})
	m.HandleFunc("/player/repeat_track", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Repeat bool `json:"repeat_track"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeSetRepeatingTrack, Data: data.Repeat}, w)
	})
	m.HandleFunc("/player/shuffle_context", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Shuffle bool `json:"shuffle_context"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeSetShufflingContext, Data: data.Shuffle}, w)
	})
	// tell the active device to switch to a new DJ set. Momentary, so no body
	m.HandleFunc("/player/dj_signal", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeDjSignal}, w)
	})
	m.HandleFunc("/player/add_to_queue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Uri string `json:"uri"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(data.Uri) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeAddToQueue, Data: data.Uri}, w)
	})
	// adds or removes the track from the users liked Songs.
	m.HandleFunc("/player/saved", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			uri := r.URL.Query().Get("uri")
			if uri == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			s.handleRequest(ApiRequest{Type: ApiRequestTypeGetSaved, Data: ApiRequestDataSaved{Uri: uri}}, w)
		case "POST":
			var data ApiRequestDataSaved
			if err := jsonDecode(r, &data); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if data.Uri == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			s.handleRequest(ApiRequest{Type: ApiRequestTypeSetSaved, Data: data}, w)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	m.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeToken}, w)
	})
	registerBluetoothRoutes(s.log, m, s.getBluetoothHandler)

	// factory reset, 200 + flush first so the frontend gets a clean response before reboot
	m.HandleFunc("POST /system/reset", func(w http.ResponseWriter, r *http.Request) {
		h := s.getSystemHandler()
		if h == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		s.log.Warn("system: factory reset requested via HTTP")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go h.PerformReset()
	})

	// restart (reboot, no wipe)
	m.HandleFunc("POST /system/restart", func(w http.ResponseWriter, r *http.Request) {
		h := s.getSystemHandler()
		if h == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		s.log.Warn("system: restart requested via HTTP")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go h.PerformRestart()
	})

	// suspend (sleep to ram)
	m.HandleFunc("POST /system/suspend", func(w http.ResponseWriter, r *http.Request) {
		h := s.getSystemHandler()
		if h == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		s.log.Info("system: suspend requested via HTTP")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go h.PerformSuspend()
	})

	// ambient light
	m.HandleFunc("GET /system/ambient", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if lux, ok := readAmbientLux(); ok {
			fmt.Fprintf(w, `{"lux":%d}`, lux)
		} else {
			_, _ = w.Write([]byte(`{"lux":null}`))
		}
	})

	// settings
	m.HandleFunc("GET /settings", func(w http.ResponseWriter, r *http.Request) {
		h := s.getSettingsHandler()
		if h == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		b := h.GetSettings()
		if len(b) == 0 {
			b = []byte("{}")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	})

	m.HandleFunc("PUT /settings", func(w http.ResponseWriter, r *http.Request) {
		h := s.getSettingsHandler()
		if h == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64*1024))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !json.Valid(body) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := h.PutSettings(body); err != nil {
			s.log.WithError(err).Warn("failed to persist settings")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})

	m.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		opts := &websocket.AcceptOptions{}
		if len(s.allowOrigin) > 0 {
			allow := s.allowOrigin
			allow = strings.TrimPrefix(allow, "http://")
			allow = strings.TrimPrefix(allow, "https://")
			allow = strings.TrimSuffix(allow, "/")
			opts.OriginPatterns = []string{allow}
		}

		conn, err := websocket.Accept(w, r, opts)
		if err != nil {
			s.log.WithError(err).Error("failed accepting websocket connection")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		client := &wsClient{conn: conn, send: make(chan *ApiEvent, wsClientBuffer)}

		s.clientsLock.Lock()
		s.clients = append(s.clients, client)
		s.clientsLock.Unlock()

		s.log.Debugf("new websocket client")

		// dedicated writer
		// the ONLY place we write to this socket
		go func() {
			for ev := range client.send {
				ctx, cancel := context.WithTimeout(context.Background(), wsWriteTimeout)
				err := wsjson.Write(ctx, client.conn, ev)
				cancel()
				if err != nil {
					s.log.WithError(err).Debug("websocket write failed; dropping client")
					s.removeClient(client)
					return
				}
			}
		}()

		// read loop
		for {
			_, _, err := client.conn.Read(context.Background())
			if s.close.Load() {
				return
			}
			if err != nil {
				s.removeClient(client)
				return
			}
		}
	})

	c := cors.New(cors.Options{
		AllowedOrigins: []string{s.allowOrigin},
		// rs/cors defaults to GET/POST/HEAD only
		AllowedMethods:      []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodHead},
		AllowPrivateNetwork: true,
		AllowCredentials:    true,
	})

	var err error
	if len(s.certFile) > 0 && len(s.keyFile) > 0 {
		err = http.ServeTLS(s.listener, c.Handler(m), s.certFile, s.keyFile)
	} else {
		err = http.Serve(s.listener, c.Handler(m))
	}

	if s.close.Load() {
		return
	} else if err != nil {
		s.log.WithError(err).Error("failed serving api")
		_ = s.Close()
	}
}

// wsClient is one connected /events subscriber
type wsClient struct {
	conn      *websocket.Conn
	send      chan *ApiEvent
	closeOnce sync.Once
}

// removeClient tears a client down exactly once
func (s *ConcreteApiServer) removeClient(c *wsClient) {
	c.closeOnce.Do(func() {
		s.clientsLock.Lock()
		for i, cc := range s.clients {
			if cc == c {
				s.clients = append(s.clients[:i], s.clients[i+1:]...)
				break
			}
		}
		s.clientsLock.Unlock()

		// close AFTER unlinking
		close(c.send)
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
	})
}

func (s *ConcreteApiServer) Emit(ev *ApiEvent) {
	s.clientsLock.RLock()
	defer s.clientsLock.RUnlock()

	s.log.Tracef("emitting websocket event: %s", ev.Type)

	for _, client := range s.clients {
		select {
		case client.send <- ev:
		default:
			// buffer full, the client is wedged
			s.log.Tracef("websocket send buffer full, dropping %s for a slow client", ev.Type)
		}
	}
}

func (s *ConcreteApiServer) Receive() <-chan ApiRequest {
	return s.requests
}

func (s *ConcreteApiServer) Close() error {
	s.close.Store(true)

	// detach all clients under the lock,
	s.clientsLock.Lock()
	clients := s.clients
	s.clients = nil
	s.clientsLock.Unlock()

	for _, client := range clients {
		client.closeOnce.Do(func() {
			close(client.send)
			_ = client.conn.Close(websocket.StatusGoingAway, "")
		})
	}

	// close the listener
	_ = s.listener.Close()
	return nil
}
