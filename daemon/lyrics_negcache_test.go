package daemon

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// A track that every source reports as having no lyrics should still be cached
func TestFetchLyrics_NoLyricsCachedSkipsRefetch(t *testing.T) {
	t.Parallel()

	var secReqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&secReqs, 1)
		if strings.Contains(r.URL.Path, "token.get") {
			_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"t"}}}`))
			return
		}
		_, _ = w.Write(noSyncedLyricsResponse()) // 200, but no lyrics
	}))
	t.Cleanup(srv.Close)

	var lrcReqs int32
	lrc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&lrcReqs, 1)
		w.WriteHeader(404) // lrclib: definitively nothing
	}))
	t.Cleanup(lrc.Close)

	sec := newTestSecondaryProviderHTTP()
	sec.tokenURL = srv.URL + "/token.get"
	sec.subtitleURL = srv.URL + "/macro.subtitles.get"
	ter := newTestTertiaryProviderHTTP()
	ter.url = lrc.URL
	lp := newTestOrchestrator(sec, ter)

	// first fetch: hits upstream, finds nothing, caches the negative
	if _, err := lp.FetchLyrics(context.Background(), "track-instrumental", "X", "Y", "", 60_000, false); !errors.Is(err, ErrNoLyrics) {
		t.Fatalf("first fetch: got %v, want ErrNoLyrics", err)
	}
	sec1, lrc1 := atomic.LoadInt32(&secReqs), atomic.LoadInt32(&lrcReqs)
	if sec1 == 0 || lrc1 == 0 {
		t.Fatal("first fetch should have hit upstream")
	}

	// second fetch: served from the negative cache
	if _, err := lp.FetchLyrics(context.Background(), "track-instrumental", "X", "Y", "", 60_000, false); !errors.Is(err, ErrNoLyrics) {
		t.Fatalf("second fetch: got %v, want ErrNoLyrics", err)
	}
	if got := atomic.LoadInt32(&secReqs); got != sec1 {
		t.Errorf("secondary re-queried on negative cache hit: %d -> %d", sec1, got)
	}
	if got := atomic.LoadInt32(&lrcReqs); got != lrc1 {
		t.Errorf("lrclib re-queried on negative cache hit: %d -> %d", lrc1, got)
	}

	// the cache holds an explicit nil (negative) entry
	lp.mu.RLock()
	v, ok := lp.cache[lyricsCacheKey("track-instrumental", "X", "Y")]
	lp.mu.RUnlock()
	if !ok || v != nil {
		t.Errorf("expected a nil negative cache entry, got ok=%v v=%v", ok, v)
	}
}

// if a source fails transiently (here, a connection error from
// lrclib), the result must NOT be cached
func TestFetchLyrics_TransientFailureNotCached(t *testing.T) {
	t.Parallel()

	var secReqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&secReqs, 1)
		if strings.Contains(r.URL.Path, "token.get") {
			_, _ = w.Write([]byte(`{"message":{"header":{"status_code":200},"body":{"user_token":"t"}}}`))
			return
		}
		_, _ = w.Write(noSyncedLyricsResponse()) // secondary: definitive no lyrics
	}))
	t.Cleanup(srv.Close)

	sec := newTestSecondaryProviderHTTP()
	sec.tokenURL = srv.URL + "/token.get"
	sec.subtitleURL = srv.URL + "/macro.subtitles.get"

	// tertiary points at a dead port -> connection error (transient, not ErrNoLyrics)
	ter := newTestTertiaryProviderHTTP()
	ter.url = "http://localhost:1/nope"
	lp := newTestOrchestrator(sec, ter)

	if _, err := lp.FetchLyrics(context.Background(), "track-maybe", "X", "Y", "", 60_000, false); !errors.Is(err, ErrNoLyrics) {
		t.Fatalf("first fetch: got %v, want ErrNoLyrics", err)
	}
	sec1 := atomic.LoadInt32(&secReqs)

	// a transient failure must leave the cache empty
	lp.mu.RLock()
	_, cached := lp.cache[lyricsCacheKey("track-maybe", "X", "Y")]
	lp.mu.RUnlock()
	if cached {
		t.Fatal("transient failure was cached as a negative; lyrics could appear on retry")
	}

	// so the second fetch re-queries upstream
	if _, err := lp.FetchLyrics(context.Background(), "track-maybe", "X", "Y", "", 60_000, false); !errors.Is(err, ErrNoLyrics) {
		t.Fatalf("second fetch: got %v, want ErrNoLyrics", err)
	}
	if got := atomic.LoadInt32(&secReqs); got <= sec1 {
		t.Errorf("expected upstream re-query after a transient failure; secondary reqs stayed at %d", got)
	}
}

// The DJ narration shares the track id of the song it introduces, so a negative cached for the
// narration used to be served to the song.
func TestFetchLyrics_NarrationNegativeDoesNotPoisonTheSong(t *testing.T) {
	t.Parallel()

	// the id shared by the narration and the song it introduces
	const sharedId = "2VQ5Zw5bDevO8iRx1EQ2gr"

	var lrcReqs int32
	lrc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&lrcReqs, 1)
		// nothing is written about what the DJ says
		if r.URL.Query().Get("artist_name") == "DJ X" {
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write([]byte(`{
			"instrumental": false,
			"syncedLyrics": "[00:00.000]real line one\n[00:05.000]real line two",
			"plainLyrics": ""
		}`))
	}))
	t.Cleanup(lrc.Close)

	ter := newTestTertiaryProviderHTTP()
	ter.url = lrc.URL
	lp := newTestOrchestrator(ter)

	// finds nothing, caches a negative under its own key
	if _, err := lp.FetchLyrics(context.Background(), sharedId, "Up next", "DJ X", "", 3_056, false); !errors.Is(err, ErrNoLyrics) {
		t.Fatalf("narration lookup: got %v, want ErrNoLyrics", err)
	}
	afterNarration := atomic.LoadInt32(&lrcReqs)
	if afterNarration == 0 {
		t.Fatal("narration lookup should have reached upstream")
	}

	// same id, different lookup, so the negative must not answer it
	result, err := lp.FetchLyrics(context.Background(), sharedId, "Tall Pines", "Ed Prosek", "", 194_210, false)
	if err != nil {
		t.Fatalf("song lookup after narration: got %v, want lyrics", err)
	}
	if len(result.Lines) != 2 {
		t.Errorf("expected the song's 2 lines, got %d", len(result.Lines))
	}
	if got := atomic.LoadInt32(&lrcReqs); got == afterNarration {
		t.Error("song lookup was served from the narration's cache entry instead of querying upstream")
	}

	// and each is cached under its own key
	lp.mu.RLock()
	defer lp.mu.RUnlock()
	if v, ok := lp.cache[lyricsCacheKey(sharedId, "Up next", "DJ X")]; !ok || v != nil {
		t.Errorf("narration: want a negative entry, got ok=%v v=%v", ok, v)
	}
	if v, ok := lp.cache[lyricsCacheKey(sharedId, "Tall Pines", "Ed Prosek")]; !ok || v == nil {
		t.Errorf("song: want a positive entry, got ok=%v v=%v", ok, v)
	}
}
