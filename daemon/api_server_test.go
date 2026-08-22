package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

// HTTP-handler contract tests, the real contract that the frontend's msw mocks reflect

// brings up a server on an OS-assigned port, closes at test end
func newTestApiServer(t *testing.T) (ApiServer, string) {
	t.Helper()
	srv, err := NewApiServer(&librespot.NullLogger{}, "127.0.0.1", 0, "*", "", "")
	if err != nil {
		t.Fatalf("NewApiServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// We access the underlying listener via the concrete type to read the
	// OS-assigned port. Same-package test, so unexported fields are fine.
	addr := srv.(*ConcreteApiServer).listener.Addr().String()
	return srv, "http://" + addr
}

// testClient is a shared http.Client with a short timeout - keeps a hung
// server from making the test suite hang for minutes.
var testClient = &http.Client{Timeout: 5 * time.Second}

// reads one ApiRequest, replies with (data, err), yields the request for assertions
func drainOne(t *testing.T, srv ApiServer, data any, err error) <-chan ApiRequest {
	t.Helper()
	ch := make(chan ApiRequest, 1)
	go func() {
		select {
		case req := <-srv.Receive():
			ch <- req
			req.Reply(data, err)
		case <-time.After(2 * time.Second):
			// Test will time out anyway via testClient; closing the channel
			// signals "no request seen" so receivers don't block forever.
			close(ch)
		}
	}()
	return ch
}

// /observer/status

func TestObserverStatus_FastPathWhenPlayerNotReady(t *testing.T) {
	t.Parallel()

	// pre-session, observer/status intercepts and returns synthetic "starting up"
	_, base := newTestApiServer(t)

	resp, err := testClient.Get(base + "/observer/status")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code: got %d want 200 (fast-path should serve synthetic JSON, not 503)", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got, want := body["active"], false; got != want {
		t.Errorf("active: got %v want %v", got, want)
	}
	if got, want := body["message"], "starting up"; got != want {
		t.Errorf("message: got %q want %q", got, want)
	}
}

func TestObserverStatus_DispatchesToChannelWhenPlayerReady(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)

	// Fake consumer drains exactly one request and replies with a stub
	// observer status. The HTTP response body is then the JSON-encoded
	// stub, proving the channel round-trip wired up correctly.
	captured := drainOne(t, srv, map[string]any{
		"active":     true,
		"track_name": "Test Song",
	}, nil)

	resp, err := testClient.Get(base + "/observer/status")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code: got %d want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got, want := body["track_name"], "Test Song"; got != want {
		t.Errorf("body forwarded from channel reply incorrectly: got %q want %q", got, want)
	}

	req, ok := <-captured
	if !ok {
		t.Fatal("consumer goroutine never received a request")
	}
	if got, want := req.Type, ApiRequestTypeObserverStatus; got != want {
		t.Errorf("ApiRequest.Type: got %q want %q", got, want)
	}
}

func TestObserverStatus_WrongMethodReturns405(t *testing.T) {
	t.Parallel()

	_, base := newTestApiServer(t)

	resp, err := testClient.Post(base+"/observer/status", "application/json", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST to /observer/status: got %d want 405", resp.StatusCode)
	}
}

func TestNonObserverChannelEndpointReturns503WhenPlayerNotReady(t *testing.T) {
	t.Parallel()

	// other channel-bound endpoints just 503 pre-session, no fast-path
	_, base := newTestApiServer(t)

	resp, err := testClient.Get(base + "/lyrics/abc?track=x&artist=y")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("channel endpoint with playerReady=false: got %d want 503", resp.StatusCode)
	}
}

// /lyrics/{id}

func TestLyrics_ValidResultReturns200WithJSON(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)
	captured := drainOne(t, srv, &LyricsResult{
		SyncType: "LINE_SYNCED",
		Lines:    []LyricsLine{{StartTimeMs: "0", Words: "Hello"}},
	}, nil)

	resp, err := testClient.Get(base + "/lyrics/abc?track=Song&artist=Artist")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	var body LyricsResult
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.SyncType != "LINE_SYNCED" || len(body.Lines) != 1 {
		t.Errorf("response body mismatch: %+v", body)
	}

	// Verify the request reached the handler with the right shape.
	req := <-captured
	if got, want := req.Type, ApiRequestTypeLyrics; got != want {
		t.Errorf("ApiRequest.Type: got %q want %q", got, want)
	}
	data, ok := req.Data.(ApiRequestDataLyrics)
	if !ok {
		t.Fatalf("ApiRequest.Data type: got %T want ApiRequestDataLyrics", req.Data)
	}
	if data.TrackId != "abc" || data.TrackName != "Song" || data.ArtistName != "Artist" {
		t.Errorf("ApiRequestDataLyrics: got %+v want trackId=abc, name=Song, artist=Artist", data)
	}
}

func TestLyrics_ErrNotFoundMapsTo404(t *testing.T) {
	t.Parallel()

	// 404 = "looked and found nothing" (silent empty state), 500 = lookup failed
	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)
	drainOne(t, srv, nil, ErrNotFound)

	resp, err := testClient.Get(base + "/lyrics/abc?track=x&artist=y")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("ErrNotFound should map to 404, got %d", resp.StatusCode)
	}
}

func TestLyrics_GenericErrorMapsTo500(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)
	drainOne(t, srv, nil, errors.New("primary source is on fire"))

	resp, err := testClient.Get(base + "/lyrics/abc?track=x&artist=y")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("generic error should map to 500, got %d", resp.StatusCode)
	}
}

// /auth/status

func TestAuthStatus_NoHandlerRegisteredReturns503(t *testing.T) {
	t.Parallel()

	// /auth/status bypasses the request channel, needs its own handler
	_, base := newTestApiServer(t)

	resp, err := testClient.Get(base + "/auth/status")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("no auth handler: got %d want 503", resp.StatusCode)
	}
}

func TestAuthStatus_NotRequiredKnownYieldsLoadingFalse(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetAuthHandler(func() (required bool, url string, known bool) {
		return false, "", true
	})

	resp, err := testClient.Get(base + "/auth/status")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, want := body["required"], false; got != want {
		t.Errorf("required: got %v want %v", got, want)
	}
	if got, want := body["loading"], false; got != want {
		// known=true loading should be false
		t.Errorf("loading: got %v want %v", got, want)
	}
	// `url` is omitted when not required
	if _, exists := body["url"]; exists {
		t.Errorf("url should be omitted when required=false; got %v", body["url"])
	}
}

func TestAuthStatus_RequiredWithURLExposesQRTarget(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	const want = "https://accounts.spotify.com/?code=ABCD"
	srv.SetAuthHandler(func() (required bool, url string, known bool) {
		return true, want, true
	})

	resp, err := testClient.Get(base + "/auth/status")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["required"] != true {
		t.Errorf("required: got %v want true", body["required"])
	}
	if body["url"] != want {
		t.Errorf("url: got %v want %q", body["url"], want)
	}
}

// /system/reset

type fakeSystemHandler struct{ called atomic.Bool }

func (f *fakeSystemHandler) PerformReset()   { f.called.Store(true) }
func (f *fakeSystemHandler) PerformRestart() { f.called.Store(true) }
func (f *fakeSystemHandler) PerformSuspend() { f.called.Store(true) }

func TestSystemReset_POSTCallsPerformReset(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	h := &fakeSystemHandler{}
	srv.SetSystemHandler(h)

	resp, err := testClient.Post(base+"/system/reset", "", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /system/reset: got %d want 200", resp.StatusCode)
	}

	// PerformReset is called from a goroutine (so the daemon can respond
	// 200 before it kills itself)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if h.called.Load() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("PerformReset was not called within 1s of the POST response")
}

func TestSystemReset_NoHandlerReturns503(t *testing.T) {
	t.Parallel()

	// No handler set to 503
	_, base := newTestApiServer(t)

	resp, err := testClient.Post(base+"/system/reset", "", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("no system handler: got %d want 503", resp.StatusCode)
	}
}

// /player/*

func TestPlayerPlayPause_POSTDispatchesPlayPauseRequest(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)
	captured := drainOne(t, srv, nil, nil)

	resp, err := testClient.Post(base+"/player/playpause", "", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	req := <-captured
	if got, want := req.Type, ApiRequestTypePlayPause; got != want {
		t.Errorf("Type: got %q want %q", got, want)
	}
}

func TestPlayerSeek_DecodesAbsolutePositionFromBody(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)
	captured := drainOne(t, srv, nil, nil)

	body := strings.NewReader(`{"position": 42000, "relative": false}`)
	resp, err := testClient.Post(base+"/player/seek", "application/json", body)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	req := <-captured
	if got, want := req.Type, ApiRequestTypeSeek; got != want {
		t.Errorf("Type: got %q want %q", got, want)
	}
	data, ok := req.Data.(ApiRequestDataSeek)
	if !ok {
		t.Fatalf("Data type: got %T want ApiRequestDataSeek", req.Data)
	}
	if data.Position != 42_000 {
		t.Errorf("Position: got %d want 42000", data.Position)
	}
	if data.Relative {
		t.Errorf("Relative: got true want false")
	}
}

func TestPlayerSeek_NegativeAbsolutePositionRejectedAsBadRequest(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)

	body := strings.NewReader(`{"position": -100, "relative": false}`)
	resp, err := testClient.Post(base+"/player/seek", "application/json", body)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("negative absolute position: got %d want 400", resp.StatusCode)
	}
}

func TestPlayerShuffleContext_BodyShapeDispatchesBool(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)
	captured := drainOne(t, srv, nil, nil)

	body := strings.NewReader(`{"shuffle_context": true}`)
	resp, err := testClient.Post(base+"/player/shuffle_context", "application/json", body)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	req := <-captured
	if got, want := req.Type, ApiRequestTypeSetShufflingContext; got != want {
		t.Errorf("Type: got %q want %q", got, want)
	}
	if got, want := req.Data.(bool), true; got != want {
		t.Errorf("Data: got %v want %v", got, want)
	}
}

func TestPlayerDjSignal_DispatchesWithoutBody(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)
	captured := drainOne(t, srv, nil, nil)

	// momentary action, so the route must dispatch even with no request body
	resp, err := testClient.Post(base+"/player/dj_signal", "application/json", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	req := <-captured
	if got, want := req.Type, ApiRequestTypeDjSignal; got != want {
		t.Errorf("Type: got %q want %q", got, want)
	}
	if req.Data != nil {
		t.Errorf("Data: got %v want nil", req.Data)
	}
}

func TestPlayerPlay_EmptyUriRejected(t *testing.T) {
	t.Parallel()

	// gate on empty URI before dispatch
	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)

	body := strings.NewReader(`{}`)
	resp, err := testClient.Post(base+"/player/play", "application/json", body)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty uri: got %d want 400", resp.StatusCode)
	}
}

func TestPlayerEndpoints_WrongMethodReturns405(t *testing.T) {
	t.Parallel()

	// GET on a // POST-only endpoint must return 405 across the board.
	_, base := newTestApiServer(t)

	for _, path := range []string{
		"/player/playpause",
		"/player/pause",
		"/player/resume",
		"/player/next",
		"/player/prev",
		"/player/seek",
		"/player/shuffle_context",
		"/player/repeat_context",
		"/player/repeat_track",
		"/player/dj_signal",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			resp, err := testClient.Get(base + path)
			if err != nil {
				t.Fatalf("Get %s: %v", path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusMethodNotAllowed {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("GET %s: got %d want 405 (body=%s)", path, resp.StatusCode, bytes.TrimSpace(body))
			}
		})
	}
}

func TestPlayerRepeatContext_ParsesBoolBody(t *testing.T) {
	t.Parallel()

	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)
	captured := drainOne(t, srv, nil, nil)

	body := strings.NewReader(`{"repeat_context": true}`)
	resp, err := testClient.Post(base+"/player/repeat_context", "application/json", body)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	req := <-captured
	if got, want := req.Type, ApiRequestTypeSetRepeatingContext; got != want {
		t.Errorf("Type: got %q want %q", got, want)
	}
	if got := req.Data.(bool); !got {
		t.Errorf("Data: got %v want true", got)
	}
}

func TestOnPlayerReady_FiresOnTransitionAndImmediatelyWhenReady(t *testing.T) {
	t.Parallel()
	srv, _ := newTestApiServer(t)

	var a, b, c atomic.Int32

	srv.OnPlayerReady(func() { a.Add(1) })
	srv.OnPlayerReady(func() { b.Add(1) })
	if a.Load() != 0 || b.Load() != 0 {
		t.Fatalf("subscribers fired before ready: a=%d b=%d", a.Load(), b.Load())
	}

	srv.SetPlayerReady(true)
	if a.Load() != 1 || b.Load() != 1 {
		t.Fatalf("subscribers not fired on transition: a=%d b=%d", a.Load(), b.Load())
	}

	srv.SetPlayerReady(false)
	srv.SetPlayerReady(true)
	if a.Load() != 1 || b.Load() != 1 {
		t.Fatalf("subscribers re-fired on second transition: a=%d b=%d", a.Load(), b.Load())
	}

	srv.OnPlayerReady(func() { c.Add(1) })
	if c.Load() != 1 {
		t.Fatalf("late subscriber not fired immediately: c=%d", c.Load())
	}
}
