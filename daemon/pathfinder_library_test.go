package daemon

import (
	"reflect"
	"testing"
)

// Fixtures are trimmed from recorded Pathfinder responses, with the user's data replaced.

const fixtureLibraryV3Playlists = `{"data":{"me":{"libraryV3":{
  "__typename":"LibraryPage","pagingInfo":{"limit":4,"offset":0},"totalCount":150,
  "items":[
    {"item":{"__typename":"LibraryPseudoPlaylistResponseWrapper","_uri":"spotify:collection:tracks",
      "data":{"__typename":"PseudoPlaylist","count":1234,"name":"Liked Songs","uri":"spotify:collection:tracks",
        "image":{"sources":[
          {"height":64,"url":"https://misc.scdn.co/liked-songs/liked-songs-64.png","width":64},
          {"height":300,"url":"https://misc.scdn.co/liked-songs/liked-songs-300.png","width":300},
          {"height":640,"url":"https://misc.scdn.co/liked-songs/liked-songs-640.png","width":640}]}}}},
    {"item":{"__typename":"PlaylistResponseWrapper","_uri":"spotify:playlist:37i9dQZF1EYkqdzj48dyYq",
      "data":{"__typename":"Playlist","name":"DJ","uri":"spotify:playlist:37i9dQZF1EYkqdzj48dyYq",
        "images":{"items":[{"sources":[{"height":null,"url":"https://example.test/dj-300.jpg","width":null}]}]},
        "ownerV2":{"data":{"__typename":"User","name":"Spotify","username":"spotify"}}}}},
    {"item":{"__typename":"PlaylistResponseWrapper","_uri":"spotify:playlist:37i9dQZF1EAAAAAAAAAAAA",
      "data":{"__typename":"NotFound"}}},
    {"item":{"__typename":"PlaylistResponseWrapper","_uri":"spotify:playlist:0aBcDeFgHiJkLmNoPqRsTu",
      "data":{"__typename":"Playlist","name":"Road Trip",
        "images":{"items":[{"sources":[
          {"height":640,"url":"https://example.test/rt-640","width":640},
          {"height":300,"url":"https://example.test/rt-300","width":300},
          {"height":60,"url":"https://example.test/rt-60","width":60}]}]},
        "ownerV2":{"data":{"__typename":"User","name":"Test User","username":"testuser"}}}}}
  ]}}}}`

const fixtureFetchPlaylist = `{"data":{"playlistV2":{
  "__typename":"Playlist","uri":"spotify:playlist:0aBcDeFgHiJkLmNoPqRsTu","name":"Road Trip",
  "ownerV2":{"data":{"__typename":"User","name":"Test User","username":"testuser"}},
  "images":{"items":[{"sources":[{"height":300,"url":"https://example.test/rt-300","width":300}]}]},
  "content":{"__typename":"PlaylistItemsPage","pagingInfo":{"limit":3,"offset":0},"totalCount":500,"items":[
    {"uid":"0000000000000a01","itemV2":{"__typename":"TrackResponseWrapper","data":{"__typename":"Track",
      "uri":"spotify:track:1aaaaaaaaaaaaaaaaaaaaa","name":"First Song",
      "artists":{"items":[{"profile":{"name":"Artist A"}},{"profile":{"name":"Guest"}}]},
      "albumOfTrack":{"name":"First Song","coverArt":{"sources":[
        {"height":300,"url":"https://example.test/a-300","width":300},
        {"height":64,"url":"https://example.test/a-64","width":64},
        {"height":640,"url":"https://example.test/a-640","width":640}]}},
      "trackDuration":{"totalMilliseconds":200000},
      "playability":{"playable":true,"reason":"PLAYABLE"}}}},
    {"uid":"0000000000000a02","itemV2":{"__typename":"TrackResponseWrapper","_uri":"spotify:track:2bbbbbbbbbbbbbbbbbbbbb",
      "data":{"__typename":"Track","name":"Second Song",
      "artists":{"items":[{"profile":{"name":"Artist B"}}]},
      "albumOfTrack":{"name":"Album B","coverArt":{"sources":[]}},
      "trackDuration":{"totalMilliseconds":210000},
      "playability":{"playable":false,"reason":"NOT_AVAILABLE"}}}},
    {"uid":"0000000000000a03","itemV2":{"__typename":"UnknownTypeWrapper","data":{"__typename":"NotFound"}}}
  ]}}}}`

const fixtureFetchLibraryTracks = `{"data":{"me":{"library":{"tracks":{
  "__typename":"UserLibraryTracksPage","pagingInfo":{"limit":2,"offset":0},"totalCount":1234,"items":[
    {"__typename":"UserLibraryTrackResponse","track":{"_uri":"spotify:track:3ccccccccccccccccccccc",
      "data":{"__typename":"Track","name":"Liked Song",
        "artists":{"items":[{"profile":{"name":"Artist C"}}]},
        "albumOfTrack":{"name":"Album C","coverArt":{"sources":[
          {"height":300,"url":"https://example.test/b-300","width":300},
          {"height":64,"url":"https://example.test/b-64","width":64}]}},
        "duration":{"totalMilliseconds":220000},
        "playability":{"playable":true}}}},
    {"__typename":"UserLibraryTrackResponse","track":{"_uri":"spotify:track:gone","data":{"__typename":"NotFound"}}}
  ]}}}}}`

func TestParseLibraryV3_PlaylistsAndLikedSongs(t *testing.T) {
	entries, pg, err := parseLibraryV3([]byte(fixtureLibraryV3Playlists))
	if err != nil {
		t.Fatalf("parseLibraryV3: %v", err)
	}
	if pg != (pfPage{Total: 150, Raw: 4}) {
		t.Errorf("page = %+v, want total 150 raw 4", pg)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (NotFound skipped)", len(entries))
	}

	liked := entries[0]
	if liked.Uri != likedCollectionUri || liked.Name != "Liked Songs" || liked.TrackCount == nil || *liked.TrackCount != 1234 {
		t.Errorf("liked = %+v", liked)
	}
	if liked.ImageUrl != "https://misc.scdn.co/liked-songs/liked-songs-300.png" {
		t.Errorf("liked image = %q", liked.ImageUrl)
	}

	dj := entries[1]
	if dj.Owner != "Spotify" || dj.ImageUrl != "https://example.test/dj-300.jpg" || dj.TrackCount != nil {
		t.Errorf("dj = %+v (unsized image should still win, no count in payload)", dj)
	}

	rt := entries[2]
	if rt.Uri != "spotify:playlist:0aBcDeFgHiJkLmNoPqRsTu" {
		t.Errorf("uri fallback to _uri: got %q", rt.Uri)
	}
	if rt.ImageUrl != "https://example.test/rt-300" || rt.Owner != "Test User" {
		t.Errorf("rt = %+v", rt)
	}
}

func TestParsePlaylist_HeaderAndTracks(t *testing.T) {
	h, tracks, pg, err := parsePlaylist([]byte(fixtureFetchPlaylist))
	if err != nil {
		t.Fatalf("parsePlaylist: %v", err)
	}
	wantH := playlistHeader{
		Typename: "Playlist", Name: "Road Trip", Owner: "Test User", ImageUrl: "https://example.test/rt-300",
	}
	if h != wantH {
		t.Errorf("header = %+v, want %+v", h, wantH)
	}
	if pg != (pfPage{Total: 500, Raw: 3}) {
		t.Errorf("page = %+v", pg)
	}
	want := []libraryTrack{
		{Uri: "spotify:track:1aaaaaaaaaaaaaaaaaaaaa", Uid: "0000000000000a01", Name: "First Song",
			Artist: "Artist A, Guest", Album: "First Song", ImageUrl: "https://example.test/a-64",
			DurationMs: 200000, Playable: true, firstArtist: "Artist A"},
		{Uri: "spotify:track:2bbbbbbbbbbbbbbbbbbbbb", Uid: "0000000000000a02", Name: "Second Song",
			Artist: "Artist B", Album: "Album B", DurationMs: 210000, firstArtist: "Artist B"},
	}
	if !reflect.DeepEqual(tracks, want) {
		t.Errorf("tracks =\n%+v\nwant\n%+v", tracks, want)
	}
}

func TestParsePlaylist_GenericErrorIsNotAPlaylist(t *testing.T) {
	h, tracks, pg, err := parsePlaylist([]byte(`{"data":{"playlistV2":{"__typename":"GenericError","message":"Failed to fetch playlist"}}}`))
	if err != nil {
		t.Fatalf("parsePlaylist: %v", err)
	}
	if h.Typename != "GenericError" || len(tracks) != 0 || pg != (pfPage{}) {
		t.Errorf("got %+v %v %+v", h, tracks, pg)
	}
}

func TestParseLibraryTracks_LikedSongs(t *testing.T) {
	tracks, pg, err := parseLibraryTracks([]byte(fixtureFetchLibraryTracks))
	if err != nil {
		t.Fatalf("parseLibraryTracks: %v", err)
	}
	if pg != (pfPage{Total: 1234, Raw: 2}) {
		t.Errorf("page = %+v", pg)
	}
	want := []libraryTrack{{
		Uri: "spotify:track:3ccccccccccccccccccccc", Name: "Liked Song", Artist: "Artist C",
		Album: "Album C", ImageUrl: "https://example.test/b-64", DurationMs: 220000, Playable: true,
		firstArtist: "Artist C",
	}}
	if !reflect.DeepEqual(tracks, want) {
		t.Errorf("tracks = %+v", tracks)
	}
}

// the voice catalog persists catalogItem, so its projection must stay {Name, first artist, Uri}
func TestCatalogProjectionUnchanged(t *testing.T) {
	_, tracks, _, _ := parsePlaylist([]byte(fixtureFetchPlaylist))
	got := tracksToCatalog(tracks)
	want := []catalogItem{
		{Name: "First Song", Artist: "Artist A", Uri: "spotify:track:1aaaaaaaaaaaaaaaaaaaaa"},
		{Name: "Second Song", Artist: "Artist B", Uri: "spotify:track:2bbbbbbbbbbbbbbbbbbbbb"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("playlist catalog = %+v", got)
	}

	liked, _, _ := parseLibraryTracks([]byte(fixtureFetchLibraryTracks))
	if got := tracksToCatalog(liked); !reflect.DeepEqual(got, []catalogItem{{Name: "Liked Song", Artist: "Artist C", Uri: "spotify:track:3ccccccccccccccccccccc"}}) {
		t.Errorf("liked catalog = %+v", got)
	}

	entries, _, _ := parseLibraryV3([]byte(fixtureLibraryV3Playlists))
	var lib []catalogItem
	for _, e := range entries {
		lib = append(lib, e.catalog())
	}
	wantLib := []catalogItem{
		{Name: "Liked Songs", Uri: "spotify:collection:tracks"},
		{Name: "DJ", Uri: "spotify:playlist:37i9dQZF1EYkqdzj48dyYq"},
		{Name: "Road Trip", Uri: "spotify:playlist:0aBcDeFgHiJkLmNoPqRsTu"},
	}
	if !reflect.DeepEqual(lib, wantLib) {
		t.Errorf("library catalog = %+v", lib)
	}

	if tracksToCatalog(nil) != nil {
		t.Errorf("empty page should stay nil")
	}
}

func TestPickImage(t *testing.T) {
	src := func(url string, w int) pfImageSource { return pfImageSource{Url: url, Width: w, Height: w} }
	cases := []struct {
		name   string
		srcs   []pfImageSource
		target int
		want   string
	}{
		{"empty", nil, 200, ""},
		{"smallest at least target", []pfImageSource{src("640", 640), src("300", 300), src("64", 64)}, 200, "300"},
		{"exact", []pfImageSource{src("640", 640), src("64", 64)}, 64, "64"},
		{"all too small takes largest", []pfImageSource{src("60", 60), src("120", 120)}, 200, "120"},
		{"sized beats unsized", []pfImageSource{src("x", 0), src("64", 64)}, 200, "64"},
		{"unsized only", []pfImageSource{src("", 0), src("x", 0)}, 200, "x"},
	}
	for _, c := range cases {
		if got := pickImage(c.srcs, c.target); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
