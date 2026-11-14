package daemon

import (
	"encoding/json"
	"strings"
)

// Pathfinder library/playlist payload parsing, shared by the voice catalog sync and /library/*.

type pfImageSource struct {
	Url    string `json:"url"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

type pfImage struct {
	Sources []pfImageSource `json:"sources"`
}

type pfImages struct {
	Items []pfImage `json:"items"`
}

func (im pfImages) first() []pfImageSource {
	if len(im.Items) == 0 {
		return nil
	}
	return im.Items[0].Sources
}

type pfArtists struct {
	Items []struct {
		Profile struct {
			Name string `json:"name"`
		} `json:"profile"`
	} `json:"items"`
}

func (a pfArtists) names() []string {
	var out []string
	for _, it := range a.Items {
		if it.Profile.Name != "" {
			out = append(out, it.Profile.Name)
		}
	}
	return out
}

func (a pfArtists) first() string {
	if len(a.Items) == 0 {
		return ""
	}
	return a.Items[0].Profile.Name
}

type pfOwner struct {
	Data struct {
		Name     string `json:"name"`
		Username string `json:"username"`
	} `json:"data"`
}

// the track shape shared by fetchPlaylist items and fetchLibraryTracks
type pfTrackData struct {
	Name         string    `json:"name"`
	Uri          string    `json:"uri"`
	Artists      pfArtists `json:"artists"`
	AlbumOfTrack struct {
		Name     string  `json:"name"`
		CoverArt pfImage `json:"coverArt"`
	} `json:"albumOfTrack"`
	TrackDuration struct {
		TotalMilliseconds int `json:"totalMilliseconds"`
	} `json:"trackDuration"`
	Duration struct {
		TotalMilliseconds int `json:"totalMilliseconds"`
	} `json:"duration"`
	Playability struct {
		Playable bool `json:"playable"`
	} `json:"playability"`
}

const (
	playlistTileImagePx = 200
	trackRowImagePx     = 64
)

// pickImage returns the smallest source at least target px wide, else the largest;
// unsized sources only win when nothing is sized.
func pickImage(srcs []pfImageSource, target int) string {
	var best, largest *pfImageSource
	for i := range srcs {
		s := &srcs[i]
		if s.Url == "" || s.Width <= 0 {
			continue
		}
		if s.Width >= target && (best == nil || s.Width < best.Width) {
			best = s
		}
		if largest == nil || s.Width > largest.Width {
			largest = s
		}
	}
	switch {
	case best != nil:
		return best.Url
	case largest != nil:
		return largest.Url
	}
	for _, s := range srcs {
		if s.Url != "" {
			return s.Url
		}
	}
	return ""
}

// libraryTrack is one row of a playlist or of Liked Songs
type libraryTrack struct {
	Uri        string `json:"uri"`
	Uid        string `json:"uid,omitempty"`
	Name       string `json:"name"`
	Artist     string `json:"artist"`
	Album      string `json:"album"`
	ImageUrl   string `json:"image_url"`
	DurationMs int    `json:"duration_ms"`
	Playable   bool   `json:"playable"`

	firstArtist string
}

func newLibraryTrack(d pfTrackData, fallbackUri, uid string) libraryTrack {
	uri := d.Uri
	if uri == "" {
		uri = fallbackUri
	}
	dur := d.TrackDuration.TotalMilliseconds
	if dur == 0 {
		dur = d.Duration.TotalMilliseconds
	}
	return libraryTrack{
		Uri:         uri,
		Uid:         uid,
		Name:        d.Name,
		Artist:      strings.Join(d.Artists.names(), ", "),
		Album:       d.AlbumOfTrack.Name,
		ImageUrl:    pickImage(d.AlbumOfTrack.CoverArt.Sources, trackRowImagePx),
		DurationMs:  dur,
		Playable:    d.Playability.Playable,
		firstArtist: d.Artists.first(),
	}
}

func (t libraryTrack) catalog() catalogItem {
	return catalogItem{Name: t.Name, Artist: t.firstArtist, Uri: t.Uri}
}

func tracksToCatalog(ts []libraryTrack) []catalogItem {
	var out []catalogItem
	for _, t := range ts {
		out = append(out, t.catalog())
	}
	return out
}

// pfPage is the paging state of one Pathfinder list response. Raw counts the
// items Pathfinder returned, including the ones parsing skipped.
type pfPage struct {
	Total int
	Raw   int
}

func parseLibraryTracks(data []byte) ([]libraryTrack, pfPage, error) {
	var r struct {
		Data struct {
			Me struct {
				Library struct {
					Tracks struct {
						TotalCount int `json:"totalCount"`
						Items      []struct {
							Track struct {
								Uri  string      `json:"_uri"`
								Data pfTrackData `json:"data"`
							} `json:"track"`
						} `json:"items"`
					} `json:"tracks"`
				} `json:"library"`
			} `json:"me"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, pfPage{}, err
	}
	tr := r.Data.Me.Library.Tracks
	var out []libraryTrack
	for _, it := range tr.Items {
		if it.Track.Data.Name == "" {
			continue
		}
		out = append(out, newLibraryTrack(it.Track.Data, it.Track.Uri, ""))
	}
	return out, pfPage{Total: tr.TotalCount, Raw: len(tr.Items)}, nil
}

// libraryEntry is one libraryV3 item: a playlist, album, artist, or the Liked Songs pseudo-playlist
type libraryEntry struct {
	Uri        string
	Name       string
	Owner      string
	ImageUrl   string
	TrackCount *int

	firstArtist string
}

func (e libraryEntry) catalog() catalogItem {
	return catalogItem{Name: e.Name, Artist: e.firstArtist, Uri: e.Uri}
}

func parseLibraryV3(data []byte) ([]libraryEntry, pfPage, error) {
	var r struct {
		Data struct {
			Me struct {
				LibraryV3 struct {
					TotalCount int `json:"totalCount"`
					Items      []struct {
						Item struct {
							Data struct {
								Typename string    `json:"__typename"`
								Uri      string    `json:"uri"`
								Name     string    `json:"name"`
								Artists  pfArtists `json:"artists"`
								OwnerV2  pfOwner   `json:"ownerV2"`
								Images   pfImages  `json:"images"`
								Image    pfImage   `json:"image"`
								Count    *int      `json:"count"`
							} `json:"data"`
							URI string `json:"_uri"`
						} `json:"item"`
					} `json:"items"`
				} `json:"libraryV3"`
			} `json:"me"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, pfPage{}, err
	}
	lib := r.Data.Me.LibraryV3
	var out []libraryEntry
	for _, it := range lib.Items {
		d := it.Item.Data
		uri := d.Uri
		if uri == "" {
			uri = it.Item.URI
		}
		if d.Name == "" || uri == "" {
			continue
		}
		img := pickImage(d.Images.first(), playlistTileImagePx)
		if img == "" {
			img = pickImage(d.Image.Sources, playlistTileImagePx)
		}
		out = append(out, libraryEntry{
			Uri:         uri,
			Name:        d.Name,
			Owner:       d.OwnerV2.Data.Name,
			ImageUrl:    img,
			TrackCount:  d.Count,
			firstArtist: d.Artists.first(),
		})
	}
	return out, pfPage{Total: lib.TotalCount, Raw: len(lib.Items)}, nil
}

// playlistHeader is the playlist-level metadata of a fetchPlaylist response
type playlistHeader struct {
	Typename string
	Name     string
	Owner    string
	ImageUrl string
}

func parsePlaylist(data []byte) (playlistHeader, []libraryTrack, pfPage, error) {
	var r struct {
		Data struct {
			PlaylistV2 struct {
				Typename string   `json:"__typename"`
				Name     string   `json:"name"`
				OwnerV2  pfOwner  `json:"ownerV2"`
				Images   pfImages `json:"images"`
				Content  struct {
					TotalCount int `json:"totalCount"`
					Items      []struct {
						Uid    string `json:"uid"`
						ItemV2 struct {
							Uri  string      `json:"_uri"`
							Data pfTrackData `json:"data"`
						} `json:"itemV2"`
					} `json:"items"`
				} `json:"content"`
			} `json:"playlistV2"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return playlistHeader{}, nil, pfPage{}, err
	}
	pl := r.Data.PlaylistV2
	c := pl.Content
	var out []libraryTrack
	for _, it := range c.Items {
		if it.ItemV2.Data.Name == "" {
			continue
		}
		out = append(out, newLibraryTrack(it.ItemV2.Data, it.ItemV2.Uri, it.Uid))
	}
	h := playlistHeader{
		Typename: pl.Typename,
		Name:     pl.Name,
		Owner:    pl.OwnerV2.Data.Name,
		ImageUrl: pickImage(pl.Images.first(), playlistTileImagePx),
	}
	return h, out, pfPage{Total: c.TotalCount, Raw: len(c.Items)}, nil
}
