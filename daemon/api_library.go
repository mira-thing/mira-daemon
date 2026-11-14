package daemon

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// /library/playlists and /library/playlist: the user's playlists and one playlist's tracks

const (
	libraryDefaultLimit = 50
	libraryMaxLimit     = 100
	libraryMaxOffset    = 100_000
)

// Spotify ids are 22 base62 characters
var playlistUriRe = regexp.MustCompile(`^spotify:playlist:[0-9A-Za-z]{22}$`)

type ApiRequestDataLibraryPage struct {
	Uri    string
	Offset int
	Limit  int
}

type ApiResponseLibraryPlaylist struct {
	Uri        string `json:"uri"`
	Name       string `json:"name"`
	Owner      string `json:"owner"`
	ImageUrl   string `json:"image_url"`
	TrackCount *int   `json:"track_count"`
}

type ApiResponseLibraryPlaylists struct {
	Items      []ApiResponseLibraryPlaylist `json:"items"`
	Total      int                          `json:"total"`
	NextOffset *int                         `json:"next_offset"`
}

type ApiResponseLibraryPlaylistPage struct {
	ApiResponseLibraryPlaylist
	Tracks     []libraryTrack `json:"tracks"`
	Total      int            `json:"total"`
	NextOffset *int           `json:"next_offset"`
}

// parseLibraryPaging reads offset/limit: limit defaults to 50 and clamps to [1, 100],
// anything non-numeric or a negative/huge offset is ErrBadRequest.
func parseLibraryPaging(q url.Values) (offset, limit int, err error) {
	limit = libraryDefaultLimit
	if v := q.Get("limit"); v != "" {
		n, aerr := strconv.Atoi(v)
		if aerr != nil {
			return 0, 0, ErrBadRequest
		}
		limit = min(max(n, 1), libraryMaxLimit)
	}
	if v := q.Get("offset"); v != "" {
		n, aerr := strconv.Atoi(v)
		if aerr != nil || n < 0 || n > libraryMaxOffset {
			return 0, 0, ErrBadRequest
		}
		offset = n
	}
	return offset, limit, nil
}

// collectionOwner returns <name> from spotify:user:<name>:collection
func collectionOwner(uri string) (string, bool) {
	name, ok := strings.CutPrefix(uri, "spotify:user:")
	if !ok {
		return "", false
	}
	name, ok = strings.CutSuffix(name, ":collection")
	return name, ok && name != "" && !strings.ContainsAny(name, ": \t\r\n")
}

// isLibraryCollectionUri accepts the liked-songs alias and any user's collection URI;
// likedSongsPage then requires that user to be the session's.
func isLibraryCollectionUri(uri string) bool {
	_, ok := collectionOwner(uri)
	return uri == likedCollectionUri || ok
}

// isOwnCollectionUri reports whether uri is username's collection; cluster URIs may carry
// the name percent-escaped or in another case.
func isOwnCollectionUri(uri, username string) bool {
	name, ok := collectionOwner(uri)
	if !ok {
		return false
	}
	if un, err := url.PathUnescape(name); err == nil {
		name = un
	}
	return strings.EqualFold(name, username)
}

func validLibraryPlaylistUri(uri string) bool {
	return playlistUriRe.MatchString(uri) || isLibraryCollectionUri(uri)
}

// nextOffset is null once the page reaches total, or when Pathfinder returned nothing to advance past
func nextOffset(offset int, pg pfPage) *int {
	n := offset + pg.Raw
	if pg.Raw == 0 || n >= pg.Total {
		return nil
	}
	return &n
}

func (s *ConcreteApiServer) registerLibraryRoutes(m *http.ServeMux) {
	m.HandleFunc("/library/playlists", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		offset, limit, err := parseLibraryPaging(r.URL.Query())
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.handleRequest(ApiRequest{Type: ApiRequestTypeLibraryPlaylists, Data: ApiRequestDataLibraryPage{Offset: offset, Limit: limit}}, w)
	})
	m.HandleFunc("/library/playlist", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		uri := q.Get("uri")
		if !validLibraryPlaylistUri(uri) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		offset, limit, err := parseLibraryPaging(q)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.handleRequest(ApiRequest{Type: ApiRequestTypeLibraryPlaylist, Data: ApiRequestDataLibraryPage{Uri: uri, Offset: offset, Limit: limit}}, w)
	})
}

// libraryRemintInterval bounds forced remints, which any LAN client could otherwise trigger per request
const libraryRemintInterval = time.Minute

// libraryQuery runs one Pathfinder fetch for a /library route, retrying with reminted tokens
// on 401/403 (rate-limited), and maps failures onto the errors handleRequest knows.
func (p *AppPlayer) libraryQuery(fetch func(force bool) error) error {
	err := fetch(false)
	var pe *pathfinderError
	if errors.As(err, &pe) && (pe.Status == 401 || pe.Status == 403) && p.claimLibraryRemint() {
		err = fetch(true)
	}
	if err == nil || !errors.As(err, &pe) {
		return err
	}
	switch {
	case pe.PersistedQuery, pe.Status == 400:
		// Pathfinder answers a rotated persisted-query hash with either one
		p.onPersistedDrift()
	case pe.Status == 401 || pe.Status == 403:
		return ErrForbidden
	case pe.Status == 404:
		return ErrNotFound
	case pe.Status == 429:
		return ErrTooManyRequests
	}
	return err
}

func (p *AppPlayer) claimLibraryRemint() bool {
	now := time.Now().UnixNano()
	last := p.libraryRemintAt.Load()
	return now-last >= int64(libraryRemintInterval) && p.libraryRemintAt.CompareAndSwap(last, now)
}

func (p *AppPlayer) libraryPlaylists(ctx context.Context, d ApiRequestDataLibraryPage) (*ApiResponseLibraryPlaylists, error) {
	var (
		entries []libraryEntry
		pg      pfPage
	)
	err := p.libraryQuery(func(force bool) (err error) {
		entries, pg, err = p.libraryV3Entries(ctx, "Playlists", d.Offset, d.Limit, force)
		return err
	})
	if err != nil {
		return nil, err
	}
	resp := &ApiResponseLibraryPlaylists{Items: []ApiResponseLibraryPlaylist{}, Total: pg.Total, NextOffset: nextOffset(d.Offset, pg)}
	for _, e := range entries {
		if !validLibraryPlaylistUri(e.Uri) {
			continue
		}
		resp.Items = append(resp.Items, ApiResponseLibraryPlaylist{
			Uri:        e.Uri,
			Name:       e.Name,
			Owner:      e.Owner,
			ImageUrl:   e.ImageUrl,
			TrackCount: e.TrackCount,
		})
	}
	return resp, nil
}

// libraryPlaylist trusts d.Uri: the /library/playlist handler, its only producer, validated it.
func (p *AppPlayer) libraryPlaylist(ctx context.Context, d ApiRequestDataLibraryPage) (*ApiResponseLibraryPlaylistPage, error) {
	if isLibraryCollectionUri(d.Uri) {
		return p.likedSongsPage(ctx, d)
	}

	var (
		h      playlistHeader
		tracks []libraryTrack
		pg     pfPage
	)
	err := p.libraryQuery(func(force bool) (err error) {
		h, tracks, pg, err = p.fetchPlaylist(ctx, d.Uri, d.Offset, d.Limit, force)
		return err
	})
	if err != nil {
		return nil, err
	}
	if h.Typename != "Playlist" {
		return nil, ErrNotFound
	}
	meta := ApiResponseLibraryPlaylist{Uri: d.Uri, Name: h.Name, Owner: h.Owner, ImageUrl: h.ImageUrl}
	return newPlaylistPage(meta, tracks, pg, d.Offset), nil
}

func (p *AppPlayer) likedSongsPage(ctx context.Context, d ApiRequestDataLibraryPage) (*ApiResponseLibraryPlaylistPage, error) {
	if d.Uri != likedCollectionUri && !isOwnCollectionUri(d.Uri, p.sess.Username()) {
		return nil, ErrBadRequest
	}
	var (
		tracks []libraryTrack
		pg     pfPage
	)
	err := p.libraryQuery(func(force bool) (err error) {
		tracks, pg, err = p.likedTracks(ctx, d.Offset, d.Limit, force)
		return err
	})
	if err != nil {
		return nil, err
	}
	meta := ApiResponseLibraryPlaylist{Uri: likedCollectionUri, Name: likedSongsName}
	return newPlaylistPage(meta, tracks, pg, d.Offset), nil
}

const likedSongsName = "Liked Songs"

func newPlaylistPage(meta ApiResponseLibraryPlaylist, tracks []libraryTrack, pg pfPage, offset int) *ApiResponseLibraryPlaylistPage {
	total := pg.Total
	meta.TrackCount = &total
	if tracks == nil {
		tracks = []libraryTrack{}
	}
	return &ApiResponseLibraryPlaylistPage{
		ApiResponseLibraryPlaylist: meta,
		Tracks:                     tracks,
		Total:                      pg.Total,
		NextOffset:                 nextOffset(offset, pg),
	}
}
