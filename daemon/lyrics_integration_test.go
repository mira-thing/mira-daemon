package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

// test helpers
func newTestSecondaryProviderHTTP() *secondaryLyricProvider {
	return &secondaryLyricProvider{
		log:    &librespot.NullLogger{},
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func newTestTertiaryProviderHTTP() *tertiaryLyricProvider {
	return &tertiaryLyricProvider{
		log:    &librespot.NullLogger{},
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func newTestOrchestrator(sources ...lyricSource) *LyricsProvider {
	return &LyricsProvider{
		log:     &librespot.NullLogger{},
		sources: sources,
		cache:   make(map[string]*LyricsResult),
	}
}

// token acquisition + caching + invalidation
func TestSecondaryGetToken_FetchesParsesAndCachesNewToken(t *testing.T) {
	t.Parallel()

	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"the-token","app_id":"web-desktop-app-v1.0"}}}`))
	}))
	t.Cleanup(srv.Close)

	s := newTestSecondaryProviderHTTP()
	s.tokenURL = srv.URL

	got, err := s.getToken(context.Background())
	if err != nil {
		t.Fatalf("getToken: %v", err)
	}
	if got != "the-token" {
		t.Errorf("token: got %q want %q", got, "the-token")
	}
	if atomic.LoadInt32(&requests) != 1 {
		t.Errorf("expected exactly 1 token fetch, got %d", requests)
	}
	if s.tok != "the-token" {
		t.Errorf("token not cached on provider; got %q", s.tok)
	}
	// exp must be in the future
	if s.exp.IsZero() || time.Until(s.exp) <= 0 {
		t.Errorf("exp should be in the future, got %v (now=%v)", s.exp, time.Now())
	}
}

func TestSecondaryGetToken_ReusesCachedTokenWithinExpiry(t *testing.T) {
	t.Parallel()

	// cache hit
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"the-token"}}}`))
	}))
	t.Cleanup(srv.Close)

	s := newTestSecondaryProviderHTTP()
	s.tokenURL = srv.URL

	if _, err := s.getToken(context.Background()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := s.getToken(context.Background()); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("expected 1 token fetch (cached on 2nd call), got %d", got)
	}
}

func TestSecondaryGetToken_ExpiredTokenTriggersRefresh(t *testing.T) {
	t.Parallel()

	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requests, 1)
		_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"token-` + string(rune('0'+n)) + `"}}}`))
	}))
	t.Cleanup(srv.Close)

	s := newTestSecondaryProviderHTTP()
	s.tokenURL = srv.URL

	first, err := s.getToken(context.Background())
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if first != "token-1" {
		t.Errorf("first token: got %q want %q", first, "token-1")
	}

	// force expiry
	s.mu.Lock()
	s.exp = time.Now().Add(-time.Second)
	s.mu.Unlock()

	second, err := s.getToken(context.Background())
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if second == first {
		t.Errorf("expired token reused: both calls returned %q", first)
	}
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Errorf("expected 2 token fetches, got %d", got)
	}
}

func TestSecondaryGetToken_Non200StatusCodeReturnsError(t *testing.T) {
	t.Parallel()

	// body wraps a non-200 status code
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"message":{"header":{"status_code":401},"body":{}}}`))
	}))
	t.Cleanup(srv.Close)

	s := newTestSecondaryProviderHTTP()
	s.tokenURL = srv.URL

	if _, err := s.getToken(context.Background()); err == nil {
		t.Error("status_code 401 should produce an error, got nil")
	}
}

func TestSecondaryGetToken_InvalidateForcesRefetch(t *testing.T) {
	t.Parallel()

	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"t"}}}`))
	}))
	t.Cleanup(srv.Close)

	s := newTestSecondaryProviderHTTP()
	s.tokenURL = srv.URL

	if _, err := s.getToken(context.Background()); err != nil {
		t.Fatalf("first: %v", err)
	}
	s.invalidateToken()
	// invalidateToken sets a cooldown
	s.mu.Lock()
	s.cooldownUntil = time.Time{}
	s.mu.Unlock()
	if _, err := s.getToken(context.Background()); err != nil {
		t.Fatalf("second: %v", err)
	}

	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Errorf("expected 2 fetches across invalidate, got %d", got)
	}
}

// end-to-end

// returns the deeply-nested secondary JSON
func happySecondarySubtitleResponse() []byte {
	return []byte(`{"message":{"header":{"status_code":200},"body":{"macro_calls":{"track.subtitles.get":{"message":{"header":{"status_code":200},"body":{"subtitle_list":[{"subtitle":{"subtitle_body":"[{\"text\":\"Hello\",\"time\":{\"total\":0}},{\"text\":\"World\",\"time\":{\"total\":1.5}}]"}}]}}}}}}}`)
}

// returns the body=[]
func noSyncedLyricsResponse() []byte {
	return []byte(`{"message":{"header":{"status_code":200},"body":{"macro_calls":{"track.subtitles.get":{"message":{"header":{"status_code":200},"body":[]}}}}}}`)
}

func TestFetchLyrics_SecondaryHappyPathReturnsSyncedLyrics(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "token.get") {
			_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"t"}}}`))
			return
		}
		_, _ = w.Write(happySecondarySubtitleResponse())
	}))
	t.Cleanup(srv.Close)

	sec := newTestSecondaryProviderHTTP()
	sec.tokenURL = srv.URL + "/token.get"
	sec.subtitleURL = srv.URL + "/macro.subtitles.get"
	ter := newTestTertiaryProviderHTTP()
	ter.url = "http://localhost:1/nope"
	lp := newTestOrchestrator(sec, ter)

	result, err := lp.FetchLyrics(
		context.Background(),
		"track-abc",
		"Hello",
		"Test",
		"",
		60_000,
		false,
	)
	if err != nil {
		t.Fatalf("FetchLyrics: %v", err)
	}
	if result == nil {
		t.Fatal("got nil result")
	}
	if result.SyncType != "LINE_SYNCED" {
		t.Errorf("SyncType: got %q want LINE_SYNCED", result.SyncType)
	}
	if len(result.Lines) != 2 {
		t.Errorf("expected 2 lines, got %d", len(result.Lines))
	}
	cached, ok := lp.cache[lyricsCacheKey("track-abc", "Hello", "Test")]
	if !ok || cached != result {
		t.Errorf("FetchLyrics did not cache the result under its lookup key")
	}
}

func TestFetchLyrics_SecondaryFailsFallsBackToLRCLIB(t *testing.T) {
	t.Parallel()

	// secondary returns "no synced subtitles" our third source has synced lyrics, returns the 3rd source
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "token.get") {
			_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"t"}}}`))
			return
		}
		_, _ = w.Write(noSyncedLyricsResponse())
	}))
	t.Cleanup(srv.Close)

	lrc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"instrumental": false,
			"syncedLyrics": "[00:00.000]LRC line one\n[00:05.000]LRC line two",
			"plainLyrics": ""
		}`))
	}))
	t.Cleanup(lrc.Close)

	sec := newTestSecondaryProviderHTTP()
	sec.tokenURL = srv.URL + "/token.get"
	sec.subtitleURL = srv.URL + "/macro.subtitles.get"
	ter := newTestTertiaryProviderHTTP()
	ter.url = lrc.URL
	lp := newTestOrchestrator(sec, ter)

	result, err := lp.FetchLyrics(
		context.Background(),
		"track-fallback",
		"Hello",
		"Test",
		"",
		60_000,
		false,
	)
	if err != nil {
		t.Fatalf("FetchLyrics: %v", err)
	}
	if result.SyncType != "LINE_SYNCED" {
		t.Errorf("SyncType: got %q want LINE_SYNCED (LRCLIB synced)", result.SyncType)
	}
	if len(result.Lines) != 2 {
		t.Errorf("expected 2 lines, got %d", len(result.Lines))
	}
	if result.Lines[0].Words != "LRC line one" {
		t.Errorf("expected LRCLIB content; got Lines[0]=%q", result.Lines[0].Words)
	}
}

func TestFetchLyrics_AllSourcesFailReturnsErrNoLyrics(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// always return the no-lyrics
		if strings.Contains(r.URL.Path, "token.get") {
			_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"t"}}}`))
			return
		}
		_, _ = w.Write(noSyncedLyricsResponse())
	}))
	t.Cleanup(srv.Close)

	lrc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404) // LRCLIB also has nothing
	}))
	t.Cleanup(lrc.Close)

	sec := newTestSecondaryProviderHTTP()
	sec.tokenURL = srv.URL + "/token.get"
	sec.subtitleURL = srv.URL + "/macro.subtitles.get"
	ter := newTestTertiaryProviderHTTP()
	ter.url = lrc.URL
	lp := newTestOrchestrator(sec, ter)

	_, err := lp.FetchLyrics(context.Background(), "track-nothing", "X", "Y", "", 60_000, false)
	if err == nil {
		t.Fatal("expected ErrNoLyrics, got nil")
	}
	if err != ErrNoLyrics {
		t.Errorf("got err %q want ErrNoLyrics", err)
	}
}

func TestFetchLyrics_EmptyTrackNameReturnsErrorImmediately(t *testing.T) {
	t.Parallel()

	// no track name = no useful query
	lp := newTestOrchestrator(newTestSecondaryProviderHTTP(), newTestTertiaryProviderHTTP())

	_, err := lp.FetchLyrics(context.Background(), "track-x", "", "Artist", "", 60_000, false)
	if err == nil {
		t.Error("empty trackName should error, got nil")
	}
}

func TestFetchLyrics_CacheHitSkipsUpstream(t *testing.T) {
	t.Parallel()

	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		if strings.Contains(r.URL.Path, "token.get") {
			_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"t"}}}`))
			return
		}
		_, _ = w.Write(happySecondarySubtitleResponse())
	}))
	t.Cleanup(srv.Close)

	sec := newTestSecondaryProviderHTTP()
	sec.tokenURL = srv.URL + "/token.get"
	sec.subtitleURL = srv.URL + "/macro.subtitles.get"
	ter := newTestTertiaryProviderHTTP()
	ter.url = "http://localhost:1/nope"
	lp := newTestOrchestrator(sec, ter)

	if _, err := lp.FetchLyrics(context.Background(), "k", "Hi", "X", "", 60_000, false); err != nil {
		t.Fatalf("first FetchLyrics: %v", err)
	}
	requestsAfterFirst := atomic.LoadInt32(&requests)
	if requestsAfterFirst == 0 {
		t.Fatal("first call should have hit the network")
	}

	// should be a cache hit
	if _, err := lp.FetchLyrics(context.Background(), "k", "Hi", "X", "", 60_000, false); err != nil {
		t.Fatalf("second FetchLyrics: %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != requestsAfterFirst {
		t.Errorf("second call hit network: requests went from %d to %d", requestsAfterFirst, got)
	}
}

// cache eviction

func TestEvictOldestLocked_BoundsCacheSize(t *testing.T) {
	t.Parallel()

	// eviction shrinks the cache enough
	cases := []struct {
		name           string
		startSize      int
		expectedRemain int
	}{
		// Traced: iter1 deletes (0 < 4/2=2). iter2 break (1 >= 3/2=1)
		// One deletion total 3 remain.
		{"size_4_deletes_1", 4, 3},
		// iter1 deletes (0 < 2). iter2 deletes (1 < 4/2=2). iter3 break (2 >= 3/2=1)
		{"size_5_deletes_2", 5, 3},
		// iter1-3 delete. iter4 break (3 >= 7/2=3)
		{"size_10_deletes_3", 10, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lp := newTestOrchestrator()
			for i := 0; i < tc.startSize; i++ {
				lp.cache[string(rune('a'+i))] = &LyricsResult{SyncType: "LINE_SYNCED"}
			}

			lp.mu.Lock()
			lp.evictOldestLocked()
			lp.mu.Unlock()

			if got := len(lp.cache); got != tc.expectedRemain {
				t.Errorf("start=%d: cache size after eviction got %d want %d",
					tc.startSize, got, tc.expectedRemain)
			}
		})
	}
}

func TestEvictOldestLocked_EmptyCacheIsSafe(t *testing.T) {
	t.Parallel()

	// calling eviction on an empty cache must not panic
	lp := newTestOrchestrator()

	lp.mu.Lock()
	lp.evictOldestLocked()
	lp.mu.Unlock()

	if len(lp.cache) != 0 {
		t.Errorf("expected empty cache, got %d entries", len(lp.cache))
	}
}

func TestClearCache_RemovesAllEntries(t *testing.T) {
	t.Parallel()

	lp := newTestOrchestrator()
	for _, k := range []string{"a", "b", "c"} {
		lp.cache[k] = &LyricsResult{SyncType: "UNSYNCED"}
	}

	lp.ClearCache()

	if len(lp.cache) != 0 {
		t.Errorf("ClearCache: expected empty, got %d entries", len(lp.cache))
	}
}

func TestFetchLyrics_ConcurrentCallsForSameTrackDoNotDeadlock(t *testing.T) {
	t.Parallel()

	// race-detector sanity, 10 concurrent FetchLyrics for the same id
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "token.get") {
			_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"t"}}}`))
			return
		}
		_, _ = w.Write(happySecondarySubtitleResponse())
	}))
	t.Cleanup(srv.Close)

	sec := newTestSecondaryProviderHTTP()
	sec.tokenURL = srv.URL + "/token.get"
	sec.subtitleURL = srv.URL + "/macro.subtitles.get"
	ter := newTestTertiaryProviderHTTP()
	ter.url = "http://localhost:1/nope"
	lp := newTestOrchestrator(sec, ter)

	const N = 10
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = lp.FetchLyrics(context.Background(), "concurrent-key", "X", "Y", "", 60_000, false)
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("FetchLyrics goroutines did not all finish within 5s - possible deadlock")
	}

	// the cache populated despite the race.
	lp.mu.RLock()
	defer lp.mu.RUnlock()
	if _, ok := lp.cache[lyricsCacheKey("concurrent-key", "X", "Y")]; !ok {
		t.Error("cache entry not populated after concurrent calls")
	}
}

var _ = json.Marshal
