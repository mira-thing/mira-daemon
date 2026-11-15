package daemon

import (
	"testing"

	librespot "github.com/devgianlu/go-librespot"
	extmetadatapb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func testImage(size metadatapb.Image_Size, id byte) *metadatapb.Image {
	return &metadatapb.Image{Size: size.Enum(), FileId: []byte{0xab, id}}
}

func trackV4Response(t *testing.T, tracks map[string]*metadatapb.Track) *extmetadatapb.BatchedExtensionResponse {
	t.Helper()
	arr := &extmetadatapb.EntityExtensionDataArray{ExtensionKind: extmetadatapb.ExtensionKind_TRACK_V4}
	for uri, tr := range tracks {
		a, err := anypb.New(tr)
		if err != nil {
			t.Fatal(err)
		}
		arr.ExtensionData = append(arr.ExtensionData, &extmetadatapb.EntityExtensionData{
			Header:        &extmetadatapb.EntityExtensionDataHeader{StatusCode: 200},
			EntityUri:     uri,
			ExtensionData: a,
		})
	}
	return &extmetadatapb.BatchedExtensionResponse{ExtendedMetadata: []*extmetadatapb.EntityExtensionDataArray{arr}}
}

func TestQueueResolverStoreParsesCover(t *testing.T) {
	t.Parallel()

	const withCover, coverGroupOnly, noCover = "spotify:track:a", "spotify:track:b", "spotify:track:c"
	r := newQueueResolver(&librespot.NullLogger{}, nil, make(chan struct{}, 1))
	n := r.store(trackV4Response(t, map[string]*metadatapb.Track{
		withCover: {
			Name:   proto.String("Example Track"),
			Artist: []*metadatapb.Artist{{Name: proto.String("Example Artist")}},
			Album: &metadatapb.Album{
				Name:  proto.String("Example Album"),
				Cover: []*metadatapb.Image{testImage(metadatapb.Image_LARGE, 1), testImage(metadatapb.Image_SMALL, 2), testImage(metadatapb.Image_DEFAULT, 3)},
			},
		},
		coverGroupOnly: {
			Name: proto.String("Other Track"),
			Album: &metadatapb.Album{
				Name:       proto.String("Other Album"),
				CoverGroup: &metadatapb.ImageGroup{Image: []*metadatapb.Image{testImage(metadatapb.Image_DEFAULT, 4), testImage(metadatapb.Image_SMALL, 5)}},
			},
		},
		noCover:               {Name: proto.String("Bare Track"), Album: &metadatapb.Album{Name: proto.String("Bare Album")}},
		"spotify:track:empty": {},
	}))
	if n != 3 {
		t.Fatalf("stored %d, want 3", n)
	}
	want := map[string]resolvedQueueTrack{
		withCover:      {name: "Example Track", artist: "Example Artist", album: "Example Album", image: "https://i.scdn.co/image/ab02"},
		coverGroupOnly: {name: "Other Track", album: "Other Album", image: "https://i.scdn.co/image/ab05"},
		noCover:        {name: "Bare Track", album: "Bare Album"},
	}
	for uri, w := range want {
		if got := r.cache[uri]; got != w {
			t.Errorf("%s: got %+v, want %+v", uri, got, w)
		}
	}
	// an empty answer stays uncached so the next update retries it
	if _, ok := r.cache["spotify:track:empty"]; ok {
		t.Error("empty track cached")
	}
}

func TestQueueResolverApplyCacheFillsImage(t *testing.T) {
	t.Parallel()

	r := newQueueResolver(&librespot.NullLogger{}, nil, make(chan struct{}, 1))
	r.cache["spotify:track:a"] = resolvedQueueTrack{name: "N", artist: "A", album: "B", image: "https://i.scdn.co/image/ab02"}

	tracks := []QueueTrack{{Uri: "spotify:track:a", Name: "N", Artist: "A", Album: "B"}}
	if need := r.applyCache(tracks); len(need) != 0 {
		t.Errorf("needsResolve = %v, want none", need)
	}
	if tracks[0].ImageUrl != "https://i.scdn.co/image/ab02" {
		t.Errorf("image_url = %q", tracks[0].ImageUrl)
	}

	// an image the cluster already supplied wins over the cache
	tracks = []QueueTrack{{Uri: "spotify:track:a", ImageUrl: "https://i.scdn.co/image/ff"}}
	r.applyCache(tracks)
	if tracks[0].ImageUrl != "https://i.scdn.co/image/ff" || tracks[0].Name != "N" {
		t.Errorf("got %+v", tracks[0])
	}
}

func TestQueueResolverApplyCacheImageGap(t *testing.T) {
	t.Parallel()

	r := newQueueResolver(&librespot.NullLogger{}, nil, make(chan struct{}, 1))
	// name/artist/album present but no cover counts as incomplete
	tracks := []QueueTrack{{Uri: "spotify:track:a", Name: "N", Artist: "A", Album: "B"}}
	need := r.applyCache(tracks)
	if len(need) != 1 || need[0] != "spotify:track:a" {
		t.Fatalf("needsResolve = %v", need)
	}
	if again := r.applyCache(tracks); len(again) != 0 {
		t.Errorf("in-flight uri rescheduled: %v", again)
	}

	// TRACK_V4 can't resolve these, so they are never scheduled
	other := []QueueTrack{{Uri: "spotify:episode:e", Name: "N"}, {Uri: "spotify:local:a:b:c:1", Name: "N"}}
	if need := r.applyCache(other); len(need) != 0 {
		t.Errorf("non-track uris scheduled: %v", need)
	}

	// a cached negative result stops the refetch loop
	delete(r.pending, "spotify:track:a")
	r.cache["spotify:track:a"] = resolvedQueueTrack{name: "N", artist: "A", album: "B"}
	if again := r.applyCache(tracks); len(again) != 0 {
		t.Errorf("cached track without cover rescheduled: %v", again)
	}
	if tracks[0].ImageUrl != "" {
		t.Errorf("image_url = %q, want empty", tracks[0].ImageUrl)
	}
}
