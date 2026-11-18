package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// voice API

type ApiEventDataVoice struct {
	State string `json:"state"`
	Text  string `json:"text,omitempty"`
}

// ApiRequestDataSearch is the body of /player/search
type ApiRequestDataSearch struct {
	Query string `json:"query"`
	TopN  bool   `json:"top_n,omitempty"`
}

// selects which Pathfinder catalog a CatalogPage request runs
type catalogKind string

const (
	catalogKindLiked          catalogKind = "liked"
	catalogKindLibraryList    catalogKind = "library_list"
	catalogKindPlaylistTracks catalogKind = "playlist_tracks"
	catalogKindTopArtists     catalogKind = "top_artists"
)

// catalog fetch
type ApiRequestDataCatalogPage struct {
	Kind   catalogKind
	Filter string
	Uri    string
	Offset int
	Limit  int
	Force  bool
}

type catalogPageResult struct {
	Items []catalogItem
	Total int
}

type playerCatalogFetcher struct {
	submit func(ctx context.Context, t ApiRequestType, data any) (any, error)
}

func (f *playerCatalogFetcher) page(ctx context.Context, d ApiRequestDataCatalogPage) ([]catalogItem, int, error) {
	res, err := f.submit(ctx, ApiRequestTypeCatalogPage, d)
	if err != nil {
		return nil, 0, err
	}
	r, _ := res.(*catalogPageResult)
	if r == nil {
		return nil, 0, nil
	}
	return r.Items, r.Total, nil
}

func (f *playerCatalogFetcher) likedTracks(ctx context.Context, offset, limit int, force bool) ([]catalogItem, int, error) {
	return f.page(ctx, ApiRequestDataCatalogPage{Kind: catalogKindLiked, Offset: offset, Limit: limit, Force: force})
}
func (f *playerCatalogFetcher) libraryList(ctx context.Context, filter string, offset, limit int, force bool) ([]catalogItem, int, error) {
	return f.page(ctx, ApiRequestDataCatalogPage{Kind: catalogKindLibraryList, Filter: filter, Offset: offset, Limit: limit, Force: force})
}
func (f *playerCatalogFetcher) playlistTracks(ctx context.Context, uri string, offset, limit int, force bool) ([]catalogItem, int, error) {
	return f.page(ctx, ApiRequestDataCatalogPage{Kind: catalogKindPlaylistTracks, Uri: uri, Offset: offset, Limit: limit, Force: force})
}
func (f *playerCatalogFetcher) topArtists(ctx context.Context, offset, limit int, force bool) ([]catalogItem, int, error) {
	return f.page(ctx, ApiRequestDataCatalogPage{Kind: catalogKindTopArtists, Offset: offset, Limit: limit, Force: force})
}

// VoiceHandler runs the voice command flow (wake/clip/transcript -> search -> play).
type VoiceHandler interface {
	TriggerVoice(ctx context.Context, transcript, clipPath string) (string, error)
}

func (s *ConcreteApiServer) SetVoiceHandler(h VoiceHandler) {
	s.voiceMu.Lock()
	s.voiceFn = h
	s.voiceMu.Unlock()
}

func (s *ConcreteApiServer) Submit(ctx context.Context, t ApiRequestType, data any) (any, error) {
	if !s.playerReady.Load() {
		return nil, ErrNoSession
	}
	req, wait := NewApiRequest(t, data)
	select {
	case s.requests <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return wait(ctx)
}

func (s *StubApiServer) SetVoiceHandler(_ VoiceHandler) {}

func (s *StubApiServer) Submit(_ context.Context, _ ApiRequestType, _ any) (any, error) {
	return nil, ErrNoSession
}

// searchTrackResult is the top track for a search term
type searchTrackResult struct {
	Uri    string `json:"uri"`
	Name   string `json:"name"`
	Artist string `json:"artist"`
}

// query hashs, if they fail we rotate them
const pfSearchDesktopHash = "21b3fe49546912ba782db5c47e9ef5a7dbd20329520ba0c7d0fcfadee671d24e"
const pfFetchLibraryTracksHash = "087278b20b743578a6262c2b0b4bcd20d879c503cc359a2285baf083ef944240"

// persistant query hashes
const (
	pfLibraryV3Hash      = "9f4da031f81274d572cfedaf6fc57a737c84b43d572952200b2c36aaa8fec1c6"
	pfFetchPlaylistHash  = "bb67e0af06e8d6f52b531f97468ee4acd44cd0f82b988e15c2ea47b1148efc77"
	pfUserTopContentHash = "49ee15704de4a7fdeac65a02db20604aa11e46f02e809c55d9a89f6db9754356"
)

type catalogItem struct {
	Name   string
	Artist string
	Uri    string
}

func (p *AppPlayer) pfBody(op string, vars map[string]any) []byte {
	b, _ := json.Marshal(map[string]any{
		"operationName": op,
		"variables":     vars,
		"extensions": map[string]any{
			"persistedQuery": map[string]any{"version": 1, "sha256Hash": p.hashOf(op)},
		},
	})
	return b
}

func (p *AppPlayer) libraryTracksPage(ctx context.Context, offset, limit int, force bool) (items []catalogItem, total int, err error) {
	tracks, pg, err := p.likedTracks(ctx, offset, limit, force)
	if err != nil {
		return nil, 0, err
	}
	return tracksToCatalog(tracks), pg.Total, nil
}

func (p *AppPlayer) likedTracks(ctx context.Context, offset, limit int, force bool) ([]libraryTrack, pfPage, error) {
	body := p.pfBody("fetchLibraryTracks", map[string]any{"offset": offset, "limit": limit})
	data, err := p.pathfinderQueryEx(ctx, body, force)
	if err != nil {
		return nil, pfPage{}, err
	}
	return parseLibraryTracks(data)
}

// fetches one page of the libraryV3 list filtered to a single entity type
func (p *AppPlayer) libraryV3Page(ctx context.Context, filter string, offset, limit int, force bool) (items []catalogItem, total int, err error) {
	entries, pg, err := p.libraryV3Entries(ctx, filter, offset, limit, force)
	if err != nil {
		return nil, 0, err
	}
	for _, e := range entries {
		items = append(items, e.catalog())
	}
	return items, pg.Total, nil
}

func (p *AppPlayer) libraryV3Entries(ctx context.Context, filter string, offset, limit int, force bool) ([]libraryEntry, pfPage, error) {
	body := p.pfBody("libraryV3", map[string]any{
		"filters":                      []string{filter},
		"order":                        nil,
		"textFilter":                   "",
		"features":                     []string{"LIKED_SONGS", "YOUR_EPISODES_V2", "PRERELEASES", "EVENTS"},
		"limit":                        limit,
		"offset":                       offset,
		"flatten":                      true,
		"expandedFolders":              []string{},
		"folderUri":                    nil,
		"includeFoldersWhenFlattening": false,
	})
	data, err := p.pathfinderQueryEx(ctx, body, force)
	if err != nil {
		return nil, pfPage{}, err
	}
	return parseLibraryV3(data)
}

// fetches one page of a playlists tracks
func (p *AppPlayer) playlistTracksPage(ctx context.Context, uri string, offset, limit int, force bool) (items []catalogItem, total int, err error) {
	_, tracks, pg, err := p.fetchPlaylist(ctx, uri, offset, limit, force)
	if err != nil {
		return nil, 0, err
	}
	return tracksToCatalog(tracks), pg.Total, nil
}

func (p *AppPlayer) fetchPlaylist(ctx context.Context, uri string, offset, limit int, force bool) (playlistHeader, []libraryTrack, pfPage, error) {
	body := p.pfBody("fetchPlaylist", map[string]any{
		"uri": uri, "offset": offset, "limit": limit,
		"enableWatchFeedEntrypoint": false,
	})
	data, err := p.pathfinderQueryEx(ctx, body, force)
	if err != nil {
		return playlistHeader{}, nil, pfPage{}, err
	}
	return parsePlaylist(data)
}

// routes a CatalogPage request to the right entity fetcher
func (p *AppPlayer) catalogPage(ctx context.Context, d ApiRequestDataCatalogPage) (*catalogPageResult, error) {
	limit := d.Limit
	if limit <= 0 {
		limit = 50
	}
	var (
		items []catalogItem
		total int
		err   error
	)
	switch d.Kind {
	case catalogKindLiked:
		items, total, err = p.libraryTracksPage(ctx, d.Offset, limit, d.Force)
	case catalogKindLibraryList:
		items, total, err = p.libraryV3Page(ctx, d.Filter, d.Offset, limit, d.Force)
	case catalogKindPlaylistTracks:
		items, total, err = p.playlistTracksPage(ctx, d.Uri, d.Offset, limit, d.Force)
	case catalogKindTopArtists:
		items, total, err = p.topArtistsPage(ctx, d.Offset, limit, d.Force)
	default:
		return nil, fmt.Errorf("unknown catalog kind: %s", d.Kind)
	}
	if err != nil {
		return nil, err
	}
	return &catalogPageResult{Items: items, Total: total}, nil
}

// topArtistsPage fetches one page of the users top artists
func (p *AppPlayer) topArtistsPage(ctx context.Context, offset, limit int, force bool) (items []catalogItem, total int, err error) {
	body := p.pfBody("userTopContent", map[string]any{
		"includeTopArtists": true,
		"includeTopTracks":  false,
		"topArtistsInput":   map[string]any{"offset": offset, "limit": limit, "sortBy": "AFFINITY"},
		"topTracksInput":    map[string]any{"offset": 0, "limit": 1, "sortBy": "AFFINITY"},
	})
	data, err := p.pathfinderQueryEx(ctx, body, force)
	if err != nil {
		return nil, 0, err
	}
	var r struct {
		Data struct {
			Me struct {
				Profile struct {
					TopArtists struct {
						TotalCount int `json:"totalCount"`
						Items      []struct {
							Data struct {
								Uri     string `json:"uri"`
								Profile struct {
									Name string `json:"name"`
								} `json:"profile"`
								Name string `json:"name"`
							} `json:"data"`
						} `json:"items"`
					} `json:"topArtists"`
				} `json:"profile"`
			} `json:"me"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, 0, err
	}
	ta := r.Data.Me.Profile.TopArtists
	for _, it := range ta.Items {
		name := it.Data.Profile.Name
		if name == "" {
			name = it.Data.Name
		}
		if name == "" || it.Data.Uri == "" {
			continue
		}
		items = append(items, catalogItem{Name: name, Uri: it.Data.Uri})
	}
	return items, ta.TotalCount, nil
}

func (p *AppPlayer) searchTrack(ctx context.Context, query string) (*searchTrackResult, error) {
	tracks, err := p.searchTracks(ctx, query)
	if err != nil {
		return nil, err
	}
	return &tracks[0], nil
}

// searchTracks runs searchDesktop and returns up to 10 tracks from spotify
// we then rerank them to try and get a proper match rather than the spotify suggested popular rankings
func (p *AppPlayer) searchTracks(ctx context.Context, query string) ([]searchTrackResult, error) {
	if strings.TrimSpace(query) == "" {
		return nil, ErrBadRequest
	}

	body := p.pfBody("searchDesktop", map[string]any{
		"searchTerm":                     query,
		"offset":                         0,
		"limit":                          10,
		"numberOfTopResults":             10,
		"includeAudiobooks":              false,
		"includeArtistHasConcertsField":  false,
		"includePreReleases":             false,
		"includeAuthors":                 false,
		"includeEpisodeContentRatingsV2": false,
	})

	data, err := p.pathfinderQuery(ctx, body)
	if err != nil {
		return nil, err
	}

	var r struct {
		Data struct {
			SearchV2 struct {
				TracksV2 struct {
					Items []struct {
						Item struct {
							Data struct {
								Name    string `json:"name"`
								Uri     string `json:"uri"`
								Artists struct {
									Items []struct {
										Profile struct {
											Name string `json:"name"`
										} `json:"profile"`
									} `json:"items"`
								} `json:"artists"`
							} `json:"data"`
						} `json:"item"`
					} `json:"items"`
				} `json:"tracksV2"`
			} `json:"searchV2"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}

	var out []searchTrackResult
	for _, it := range r.Data.SearchV2.TracksV2.Items {
		d := it.Item.Data
		if d.Uri == "" {
			continue
		}
		var arts []string
		for _, a := range d.Artists.Items {
			if a.Profile.Name != "" {
				arts = append(arts, a.Profile.Name)
			}
		}
		out = append(out, searchTrackResult{Uri: d.Uri, Name: d.Name, Artist: strings.Join(arts, ", ")})
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}
