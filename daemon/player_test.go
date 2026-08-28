package daemon

import (
	"encoding/json"
	"testing"
)

// convertSpotifyImageUrl normalizes the image_url field to a usable https URL
func TestConvertSpotifyImageUrl(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			// standard shape from PlayerState.Track.Metadata["image_url"]
			name: "spotify_image_prefix",
			in:   "spotify:image:ab67616d00001e02deadbeef",
			want: "https://i.scdn.co/image/ab67616d00001e02deadbeef",
		},
		{
			// already-absolute https, pass through
			name: "already_https",
			in:   "https://i.scdn.co/image/abc",
			want: "https://i.scdn.co/image/abc",
		},
		{
			name: "already_http",
			in:   "http://example.com/img.png",
			want: "http://example.com/img.png",
		},
		{
			// default branch, prepend https:// so at least it's a valid URL
			name: "default_prepends_https",
			in:   "bare-string",
			want: "https://bare-string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := convertSpotifyImageUrl(tt.in); got != tt.want {
				t.Errorf("convertSpotifyImageUrl(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// buildPlayCommand: the DJ session swaps the resolve url, everything else is the generic envelope
func TestBuildPlayCommand(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	const wantOrigin = `"play_origin":{"feature_identifier":"your_library",` +
		`"feature_version":"go-librespot","referrer_identifier":"your_library"},` +
		`"logging_params":{"page_instance_ids":[],"interaction_ids":[],"command_id":""}}`

	tests := []struct {
		name string
		in   ApiRequestDataPlay
		want string
	}{
		{
			name: "dj_uri_resolves_through_the_lexicon_provider",
			in:   ApiRequestDataPlay{Uri: djContextUri},
			want: `{"endpoint":"play","context":{` +
				`"uri":"spotify:playlist:37i9dQZF1EYkqdzj48dyYq",` +
				`"entity_uri":"spotify:playlist:37i9dQZF1EYkqdzj48dyYq",` +
				`"url":"hm://lexicon-session-provider/context-resolve/v2/session` +
				`?contextUri=spotify:playlist:37i9dQZF1EYkqdzj48dyYq","metadata":{}},` +
				`"options":{"license":"tft","skip_to":{},"player_options_override":{}},` +
				wantOrigin,
		},
		{
			// voice sends shuffle=false on every play, so the DJ context sees it too
			name: "dj_uri_carries_shuffle_like_any_other",
			in:   ApiRequestDataPlay{Uri: djContextUri, Shuffle: boolPtr(false)},
			want: `{"endpoint":"play","context":{` +
				`"uri":"spotify:playlist:37i9dQZF1EYkqdzj48dyYq",` +
				`"entity_uri":"spotify:playlist:37i9dQZF1EYkqdzj48dyYq",` +
				`"url":"hm://lexicon-session-provider/context-resolve/v2/session` +
				`?contextUri=spotify:playlist:37i9dQZF1EYkqdzj48dyYq","metadata":{}},` +
				`"options":{"license":"tft","skip_to":{},` +
				`"player_options_override":{"shuffling_context":false}},` +
				wantOrigin,
		},
		{
			// regression guard: entity_uri and the DJ url must not reach an ordinary context
			name: "ordinary_playlist_unchanged",
			in:   ApiRequestDataPlay{Uri: "spotify:playlist:abc"},
			want: `{"endpoint":"play","context":{"uri":"spotify:playlist:abc",` +
				`"url":"context://spotify:playlist:abc","metadata":{}},` +
				`"options":{"license":"tft","skip_to":{},"player_options_override":{}},` +
				wantOrigin,
		},
		{
			name: "ordinary_uri_keeps_skip_and_shuffle",
			in: ApiRequestDataPlay{
				Uri:       "spotify:album:abc",
				SkipToUri: "spotify:track:xyz",
				Shuffle:   boolPtr(true),
			},
			want: `{"endpoint":"play","context":{"uri":"spotify:album:abc",` +
				`"url":"context://spotify:album:abc","metadata":{}},` +
				`"options":{"license":"tft","skip_to":{"track_uri":"spotify:track:xyz"},` +
				`"player_options_override":{"shuffling_context":true}},` +
				wantOrigin,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := buildPlayCommand(tt.in)
			// randomCommandId makes the envelope unstable, so blank it and pin the rest
			if cmd.LoggingParams != nil {
				cmd.LoggingParams.CommandId = ""
			}
			got, err := json.Marshal(cmd)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("buildPlayCommand(%+v)\n got %s\nwant %s", tt.in, got, tt.want)
			}
		})
	}
}
