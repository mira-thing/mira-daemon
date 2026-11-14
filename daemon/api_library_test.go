package daemon

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParseLibraryPaging(t *testing.T) {
	cases := []struct {
		query          string
		offset, limit  int
		wantBadRequest bool
	}{
		{"", 0, 50, false},
		{"limit=10&offset=20", 20, 10, false},
		{"limit=0", 0, 1, false},
		{"limit=-5", 0, 1, false},
		{"limit=500", 0, 100, false},
		{"limit=100", 0, 100, false},
		{"limit=abc", 0, 0, true},
		{"limit=1.5", 0, 0, true},
		{"offset=-1", 0, 0, true},
		{"offset=x", 0, 0, true},
		{"offset=100001", 0, 0, true},
		{"offset=100000", 100000, 50, false},
	}
	for _, c := range cases {
		q, _ := url.ParseQuery(c.query)
		off, lim, err := parseLibraryPaging(q)
		if c.wantBadRequest {
			if !errors.Is(err, ErrBadRequest) {
				t.Errorf("%q: err = %v, want ErrBadRequest", c.query, err)
			}
			continue
		}
		if err != nil || off != c.offset || lim != c.limit {
			t.Errorf("%q: got (%d, %d, %v) want (%d, %d)", c.query, off, lim, err, c.offset, c.limit)
		}
	}
}

func TestValidLibraryPlaylistUri(t *testing.T) {
	valid := []string{
		"spotify:playlist:37i9dQZF1EYkqdzj48dyYq",
		"spotify:collection:tracks",
		"spotify:user:testuser:collection",
		"spotify:user:test.user_1:collection",
	}
	invalid := []string{
		"",
		"spotify:playlist:",
		"spotify:playlist:37i9dQZF1EYkqdzj48dyY", // 21 chars
		"spotify:playlist:37i9dQZF1EYkqdzj48dyYqq", // 23 chars
		"spotify:playlist:37i9dQZF1EYkqdzj48dy-q",
		"spotify:playlist:37i9dQZF1EYkqdzj48dyYq\n",
		"spotify:playlist:37i9dQZF1EYkqdzj48dyYq:extra",
		" spotify:playlist:37i9dQZF1EYkqdzj48dyYq",
		"spotify:album:37i9dQZF1EYkqdzj48dyYq",
		"spotify:collection",
		"spotify:collection:your-episodes",
		"spotify:user::collection",
		"spotify:user:a:b:collection",
		"spotify:user:testuser:collection:tracks",
		"spotify:user:testuser:playlist:37i9dQZF1EYkqdzj48dyYq",
		"https://open.spotify.com/playlist/37i9dQZF1EYkqdzj48dyYq",
	}
	for _, u := range valid {
		if !validLibraryPlaylistUri(u) {
			t.Errorf("%q rejected", u)
		}
	}
	for _, u := range invalid {
		if validLibraryPlaylistUri(u) {
			t.Errorf("%q accepted", u)
		}
	}
}

func TestNextOffset(t *testing.T) {
	ptr := func(n int) *int { return &n }
	cases := []struct {
		offset int
		pg     pfPage
		want   *int
	}{
		{0, pfPage{Total: 150, Raw: 50}, ptr(50)},
		{120, pfPage{Total: 150, Raw: 30}, nil},
		{0, pfPage{Total: 0, Raw: 0}, nil},
		{500, pfPage{Total: 150, Raw: 0}, nil},
		// a short page (dead items Pathfinder still counts) advances by what was returned
		{100, pfPage{Total: 150, Raw: 40}, ptr(140)},
	}
	for _, c := range cases {
		got := nextOffset(c.offset, c.pg)
		if (got == nil) != (c.want == nil) || (got != nil && *got != *c.want) {
			t.Errorf("nextOffset(%d, %+v) = %v, want %v", c.offset, c.pg, got, c.want)
		}
	}
}

func TestLibraryQuery_ErrorMapping(t *testing.T) {
	p := &AppPlayer{app: &App{}}
	cases := []struct {
		name   string
		errs   []error
		want   error
		forced []bool
	}{
		{"ok", []error{nil}, nil, []bool{false}},
		{"429", []error{&pathfinderError{Status: 429}}, ErrTooManyRequests, []bool{false}},
		{"404", []error{&pathfinderError{Status: 404}}, ErrNotFound, []bool{false}},
		{"403 then ok", []error{&pathfinderError{Status: 403}, nil}, nil, []bool{false, true}},
		{"401 twice", []error{&pathfinderError{Status: 401}, &pathfinderError{Status: 401}}, ErrForbidden, []bool{false, true}},
		{"403 then 429", []error{&pathfinderError{Status: 403}, &pathfinderError{Status: 429}}, ErrTooManyRequests, []bool{false, true}},
	}
	for _, c := range cases {
		p.libraryRemintAt.Store(0)
		var forced []bool
		err := p.libraryQuery(func(force bool) error {
			forced = append(forced, force)
			return c.errs[len(forced)-1]
		})
		if !errors.Is(err, c.want) || (c.want == nil && err != nil) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
		if len(forced) != len(c.forced) {
			t.Errorf("%s: calls = %v, want %v", c.name, forced, c.forced)
			continue
		}
		for i := range forced {
			if forced[i] != c.forced[i] {
				t.Errorf("%s: calls = %v, want %v", c.name, forced, c.forced)
			}
		}
	}

	// a second 403 inside the remint interval is not retried
	calls := 0
	if err := p.libraryQuery(func(bool) error { calls++; return &pathfinderError{Status: 403} }); !errors.Is(err, ErrForbidden) || calls != 1 {
		t.Errorf("rate-limited 403: err = %v, calls = %d", err, calls)
	}

	// unmapped failures pass through so handleRequest logs them as 500s
	plain := errors.New("boom")
	if err := p.libraryQuery(func(bool) error { return plain }); err != plain {
		t.Errorf("plain error = %v", err)
	}
	pq := &pathfinderError{Status: 200, PersistedQuery: true, msg: "PersistedQueryNotFound"}
	if err := p.libraryQuery(func(bool) error { return pq }); err != pq {
		t.Errorf("persisted-query error = %v", err)
	}
	bad := &pathfinderError{Status: 400}
	if err := p.libraryQuery(func(bool) error { return bad }); err != bad {
		t.Errorf("400 error = %v", err)
	}
}

func TestIsOwnCollectionUri(t *testing.T) {
	for uri, want := range map[string]bool{
		"spotify:user:testuser:collection":    true,
		"spotify:user:TestUser:collection":    true,
		"spotify:user:test%40user:collection": false,
		"spotify:user:otheruser:collection":   false,
	} {
		if got := isOwnCollectionUri(uri, "testuser"); got != want {
			t.Errorf("%q: got %v", uri, got)
		}
	}
	if !isOwnCollectionUri("spotify:user:a%40b:collection", "a@b") {
		t.Error("percent-escaped name rejected")
	}
}

// HTTP contract

func TestLibraryRoutes_ValidationRejectsWithoutDispatch(t *testing.T) {
	t.Parallel()
	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)

	for _, path := range []string{
		"/library/playlist",
		"/library/playlist?uri=spotify:album:37i9dQZF1EYkqdzj48dyYq",
		"/library/playlist?uri=" + url.QueryEscape(`spotify:playlist:37i9dQZF1EYkqdzj48dyYq","x":"`),
		"/library/playlist?uri=spotify:playlist:37i9dQZF1EYkqdzj48dyYq&offset=-1",
		"/library/playlists?limit=abc",
		"/library/playlists?offset=-3",
	} {
		resp, err := testClient.Get(base + path)
		if err != nil {
			t.Fatalf("Get %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: got %d want 400", path, resp.StatusCode)
		}
	}
	select {
	case req := <-srv.Receive():
		t.Errorf("rejected request reached the player: %+v", req)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLibraryRoutes_WrongMethodReturns405(t *testing.T) {
	t.Parallel()
	_, base := newTestApiServer(t)
	for _, path := range []string{"/library/playlists", "/library/playlist?uri=spotify:collection:tracks"} {
		resp, err := testClient.Post(base+path, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("Post %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("POST %s: got %d want 405", path, resp.StatusCode)
		}
	}
}

func TestLibraryPlaylists_DispatchesClampedPaging(t *testing.T) {
	t.Parallel()
	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)

	next := 120
	count := 1234
	reply := &ApiResponseLibraryPlaylists{
		Items: []ApiResponseLibraryPlaylist{{Uri: likedCollectionUri, Name: "Liked Songs", ImageUrl: "https://example.test/l", TrackCount: &count}},
		Total: 150, NextOffset: &next,
	}
	seen := drainOne(t, srv, reply, nil)

	resp, err := testClient.Get(base + "/library/playlists?offset=20&limit=1000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	req := <-seen
	if req.Type != ApiRequestTypeLibraryPlaylists {
		t.Errorf("type = %s", req.Type)
	}
	if d, _ := req.Data.(ApiRequestDataLibraryPage); d != (ApiRequestDataLibraryPage{Offset: 20, Limit: 100}) {
		t.Errorf("data = %+v, want offset 20 limit 100", req.Data)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	items, _ := body["items"].([]any)
	if body["total"] != float64(150) || body["next_offset"] != float64(120) || len(items) != 1 {
		t.Errorf("body = %v", body)
	}
	first, _ := items[0].(map[string]any)
	for _, k := range []string{"uri", "name", "owner", "image_url", "track_count"} {
		if _, ok := first[k]; !ok {
			t.Errorf("item missing %q: %v", k, first)
		}
	}
}

func TestLibraryPlaylist_DispatchesUriAndDefaults(t *testing.T) {
	t.Parallel()
	srv, base := newTestApiServer(t)
	srv.SetPlayerReady(true)

	seen := drainOne(t, srv, &ApiResponseLibraryPlaylistPage{Tracks: []libraryTrack{}}, nil)
	resp, err := testClient.Get(base + "/library/playlist?uri=spotify:playlist:37i9dQZF1EYkqdzj48dyYq")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	req := <-seen
	want := ApiRequestDataLibraryPage{Uri: "spotify:playlist:37i9dQZF1EYkqdzj48dyYq", Offset: 0, Limit: 50}
	if d, _ := req.Data.(ApiRequestDataLibraryPage); req.Type != ApiRequestTypeLibraryPlaylist || d != want {
		t.Errorf("req = %s %+v, want %+v", req.Type, req.Data, want)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if tr, ok := body["tracks"].([]any); !ok || len(tr) != 0 {
		t.Errorf("tracks should be an empty array, got %v", body["tracks"])
	}
	if _, ok := body["next_offset"]; !ok || body["next_offset"] != nil {
		t.Errorf("next_offset should be null, got %v", body["next_offset"])
	}
}

func TestLibraryPlaylist_ErrorsMapToStatus(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		err  error
		want int
	}{
		{ErrTooManyRequests, http.StatusTooManyRequests},
		{ErrNotFound, http.StatusNotFound},
		{ErrBadRequest, http.StatusBadRequest},
		{ErrForbidden, http.StatusForbidden},
	} {
		srv, base := newTestApiServer(t)
		srv.SetPlayerReady(true)
		drainOne(t, srv, nil, c.err)
		resp, err := testClient.Get(base + "/library/playlist?uri=spotify:collection:tracks")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%v: got %d want %d", c.err, resp.StatusCode, c.want)
		}
	}
}

func TestLibraryRoutes_503WhenPlayerNotReady(t *testing.T) {
	t.Parallel()
	_, base := newTestApiServer(t)
	resp, err := testClient.Get(base + "/library/playlists")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("got %d want 503", resp.StatusCode)
	}
}
