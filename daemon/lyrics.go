package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

// LyricsWord is one word segment with its own start time
type LyricsWord struct {
	StartTimeMs string `json:"startTimeMs"`
	Word        string `json:"word"`
}

type LyricsLine struct {
	StartTimeMs string       `json:"startTimeMs"`
	Words       string       `json:"words"`
	Syllables   []LyricsWord `json:"syllables,omitempty"` // word-level timing
}

type LyricsResult struct {
	SyncType string       `json:"syncType"` // LINE_SYNCED or UNSYNCED
	Lines    []LyricsLine `json:"lines"`
}

// 404 means a track w no lyrics
var ErrNoLyrics = errors.New("no lyrics available")

// everything any source might need to look a track up
type lyricsQuery struct {
	trackId    string
	trackName  string
	artistName string
	albumName  string
	durationMs int
}

// lyricSource is one tier in the prefer-synced lookup chain.
type lyricSource interface {
	name() string
	fetch(ctx context.Context, q lyricsQuery) (*LyricsResult, error)
}

type LyricsProvider struct {
	log       librespot.Logger
	sources   []lyricSource
	episode   *episodeTextProvider
	secondary *secondaryLyricProvider

	mu    sync.RWMutex
	cache map[string]*LyricsResult // keyed by lyricsCacheKey, not by track id alone
}

func NewLyricsProvider(logger librespot.Logger, getAccessToken func(ctx context.Context, force bool) (string, error)) *LyricsProvider {
	secondary := newSecondaryLyricProvider(logger)
	return &LyricsProvider{
		log: logger,
		sources: []lyricSource{
			newPrimaryLyricProvider(logger, getAccessToken),
			secondary,
			newTertiaryLyricProvider(logger),
		},
		episode:   newEpisodeTextProvider(logger, getAccessToken),
		secondary: secondary,
		cache:     make(map[string]*LyricsResult),
	}
}

// hasWords reports whether a result carries word by word timing.
func hasWords(r *LyricsResult) bool {
	for i := range r.Lines {
		if len(r.Lines[i].Syllables) > 0 {
			return true
		}
	}
	return false
}

// identifies a lookup rather than a track: ids are not unique per lookup, since the DJ
// narration shares the id of the song it introduces
func lyricsCacheKey(trackId, trackName, artistName string) string {
	norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
	// unit separator: should not appear in an id, a title or an artist name
	return trackId + "\x1f" + norm(trackName) + "\x1f" + norm(artistName)
}

// FetchLyrics returns the best lyrics for a track
func (lp *LyricsProvider) FetchLyrics(ctx context.Context, trackId, trackName, artistName, albumName string, durationMs int, wantRichsync bool) (*LyricsResult, error) {
	if trackName == "" {
		return nil, fmt.Errorf("track name is required")
	}

	key := lyricsCacheKey(trackId, trackName, artistName)

	lp.mu.RLock()
	cached, ok := lp.cache[key]
	lp.mu.RUnlock()
	if ok {
		// a nil entry is a negative cache
		if cached == nil {
			return nil, ErrNoLyrics
		}
		// a positive hit serves directly, unless we want word-by-word timing
		if !wantRichsync || hasWords(cached) {
			return cached, nil
		}
	}

	q := lyricsQuery{
		trackId:    trackId,
		trackName:  trackName,
		artistName: artistName,
		albumName:  albumName,
		durationMs: durationMs,
	}

	if wantRichsync && lp.secondary != nil {
		if res, err := lp.secondary.fetchRichsync(ctx, q); err != nil {
			lp.log.Debugf("lyrics: richsync unavailable: %v", err)
			if cached != nil {
				return cached, nil
			}
		} else if res != nil {
			lp.store(key, res)
			lp.log.Debugf("lyrics: richsync (word-level) for %q by %q (%d lines)", trackName, artistName, len(res.Lines))
			return res, nil
		}
	}

	var result, firstUnsynced *LyricsResult
	var sawTransientError bool
	for _, src := range lp.sources {
		res, err := src.fetch(ctx, q)
		if err != nil {
			// ErrNoLyrics means the source has nothing for this track
			if !errors.Is(err, ErrNoLyrics) {
				sawTransientError = true
			}
			lp.log.Debugf("lyrics: %s failed: %v", src.name(), err)
			continue
		}
		if res == nil {
			continue
		}
		if res.SyncType == "LINE_SYNCED" {
			result = res
			break
		}
		if firstUnsynced == nil {
			firstUnsynced = res
		}
	}

	if result == nil {
		result = firstUnsynced
	}
	if result == nil {
		// cache the negative only when every source returns no lyrics
		if !sawTransientError && artistName != "" {
			lp.mu.Lock()
			lp.storeLocked(key, nil)
			lp.mu.Unlock()
			lp.log.Debugf("lyrics: confirmed none for %q by %q, caching negative", trackName, artistName)
		}
		return nil, ErrNoLyrics
	}

	lp.store(key, result)

	lp.log.Debugf("lyrics found for %q by %q (%s, %d lines)", trackName, artistName, result.SyncType, len(result.Lines))
	return result, nil
}

func (lp *LyricsProvider) store(key string, result *LyricsResult) {
	lp.mu.Lock()
	lp.storeLocked(key, result)
	lp.mu.Unlock()
}

// returns the synced text track for podcasts, these are not cached
func (lp *LyricsProvider) FetchEpisodeText(ctx context.Context, episodeId string) (*LyricsResult, error) {
	if i := strings.LastIndex(episodeId, ":"); i >= 0 {
		episodeId = episodeId[i+1:]
	}
	if episodeId == "" {
		return nil, fmt.Errorf("episode id is required")
	}

	if lp.episode == nil {
		return nil, ErrNoLyrics
	}

	result, err := lp.episode.fetch(ctx, episodeId)
	if err != nil {
		return nil, err
	}
	lp.log.Debugf("episode text found for %s (%d lines)", episodeId, len(result.Lines))
	return result, nil
}

// storeLocked records a cache entry, keyed by lyricsCacheKey
func (lp *LyricsProvider) storeLocked(key string, result *LyricsResult) {
	lp.cache[key] = result
	if len(lp.cache) > 200 {
		lp.evictOldestLocked()
	}
}

func (lp *LyricsProvider) evictOldestLocked() {
	// drop half when over the limit
	count := 0
	for k := range lp.cache {
		if count >= len(lp.cache)/2 {
			break
		}
		delete(lp.cache, k)
		count++
	}
}

func (lp *LyricsProvider) ClearCache() {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	lp.cache = make(map[string]*LyricsResult)
}

// primary source

// errPrimaryUnauthorized signals a 401/403 so we force a token refresh
var errPrimaryUnauthorized = errors.New("primary unauthorized")

type primaryLyricProvider struct {
	log            librespot.Logger
	client         *http.Client
	url            string
	query          string
	platform       string
	getAccessToken func(ctx context.Context, force bool) (string, error)
}

func newPrimaryLyricProvider(logger librespot.Logger, getAccessToken func(ctx context.Context, force bool) (string, error)) *primaryLyricProvider {
	return &primaryLyricProvider{
		log:            logger,
		client:         &http.Client{Timeout: 10 * time.Second},
		url:            os.Getenv("THING_LP_PRIMARY_URL"),
		query:          os.Getenv("THING_LP_PRIMARY_QUERY"),
		platform:       os.Getenv("THING_LP_PRIMARY_PLATFORM"),
		getAccessToken: getAccessToken,
	}
}

func (p *primaryLyricProvider) name() string { return "primary" }

func (p *primaryLyricProvider) fetch(ctx context.Context, q lyricsQuery) (*LyricsResult, error) {
	if p.url == "" {
		return nil, fmt.Errorf("primary disabled (no env config): %w", ErrNoLyrics)
	}
	if p.getAccessToken == nil {
		return nil, fmt.Errorf("primary: no access token source: %w", ErrNoLyrics)
	}

	// trackId may arrive as a bare id or in a "<type>:<id>" form
	id := q.trackId
	if i := strings.LastIndex(id, ":"); i >= 0 {
		id = id[i+1:]
	}
	if id == "" {
		return nil, fmt.Errorf("primary: empty track id: %w", ErrNoLyrics)
	}

	result, err := p.request(ctx, id, false)
	if errors.Is(err, errPrimaryUnauthorized) {
		p.log.Debugf("lyrics: primary 401/403, retrying with a fresh token")
		result, err = p.request(ctx, id, true)
	}
	return result, err
}

func (p *primaryLyricProvider) request(ctx context.Context, trackId string, force bool) (*LyricsResult, error) {
	token, err := p.getAccessToken(ctx, force)
	if err != nil {
		return nil, fmt.Errorf("primary access token: %w", err)
	}

	reqURL := p.url + trackId
	if p.query != "" {
		reqURL += "?" + p.query
	}

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating primary request: %w", err)
	}
	// a browser-like User-Agent
	req.Header.Set("Authorization", "Bearer "+token)
	if p.platform != "" {
		req.Header.Set("App-Platform", p.platform)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("primary request failed: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		return nil, errPrimaryUnauthorized
	case resp.StatusCode == 404:
		// no lyrics for this track in the primary source
		return nil, fmt.Errorf("primary: no lyrics for track: %w", ErrNoLyrics)
	case resp.StatusCode != 200:
		return nil, fmt.Errorf("primary status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading primary response: %w", err)
	}
	return parsePrimary(body)
}

func parsePrimary(body []byte) (*LyricsResult, error) {
	var resp struct {
		Lyrics struct {
			SyncType string `json:"syncType"`
			Lines    []struct {
				StartTimeMs string `json:"startTimeMs"`
				Words       string `json:"words"`
			} `json:"lines"`
		} `json:"lyrics"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing primary response: %w", err)
	}

	if len(resp.Lyrics.Lines) == 0 {
		return nil, fmt.Errorf("primary: no lines: %w", ErrNoLyrics)
	}

	syncType := resp.Lyrics.SyncType
	if syncType == "" {
		syncType = "UNSYNCED"
	}

	// startTimeMs already arrives as a millisecond string
	lines := make([]LyricsLine, 0, len(resp.Lyrics.Lines))
	for _, l := range resp.Lyrics.Lines {
		lines = append(lines, LyricsLine{
			StartTimeMs: l.StartTimeMs,
			Words:       l.Words,
		})
	}

	return &LyricsResult{SyncType: syncType, Lines: lines}, nil
}

// episode text source

type episodeTextProvider struct {
	log            librespot.Logger
	client         *http.Client
	url            string
	query          string
	platform       string
	getAccessToken func(ctx context.Context, force bool) (string, error)
}

func newEpisodeTextProvider(logger librespot.Logger, getAccessToken func(ctx context.Context, force bool) (string, error)) *episodeTextProvider {
	return &episodeTextProvider{
		log:            logger,
		client:         &http.Client{Timeout: 15 * time.Second},
		url:            os.Getenv("THING_LP_EPISODE_URL"),
		query:          os.Getenv("THING_LP_EPISODE_QUERY"),
		platform:       os.Getenv("THING_LP_EPISODE_PLATFORM"),
		getAccessToken: getAccessToken,
	}
}

func (p *episodeTextProvider) fetch(ctx context.Context, episodeId string) (*LyricsResult, error) {
	if p.url == "" {
		return nil, fmt.Errorf("episode text disabled (no env config): %w", ErrNoLyrics)
	}
	if p.getAccessToken == nil {
		return nil, fmt.Errorf("episode text: no access token source: %w", ErrNoLyrics)
	}

	result, err := p.request(ctx, episodeId, false)
	if errors.Is(err, errPrimaryUnauthorized) {
		p.log.Debugf("episode text: 401/403, retrying with a fresh token")
		result, err = p.request(ctx, episodeId, true)
	}
	return result, err
}

func (p *episodeTextProvider) request(ctx context.Context, episodeId string, force bool) (*LyricsResult, error) {
	token, err := p.getAccessToken(ctx, force)
	if err != nil {
		return nil, fmt.Errorf("episode text access token: %w", err)
	}

	reqURL := p.url + episodeId
	if p.query != "" {
		reqURL += "?" + p.query
	}

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating episode text request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if p.platform != "" {
		req.Header.Set("App-Platform", p.platform)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("episode text request failed: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		return nil, errPrimaryUnauthorized
	case resp.StatusCode == 404:
		return nil, fmt.Errorf("episode text: none for episode: %w", ErrNoLyrics)
	case resp.StatusCode != 200:
		return nil, fmt.Errorf("episode text status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading episode text response: %w", err)
	}
	return parseEpisodeText(body)
}

func parseEpisodeText(body []byte) (*LyricsResult, error) {
	var resp struct {
		Section []struct {
			StartMs int64 `json:"startMs"`
			Title   *struct {
				Title string `json:"title"`
			} `json:"title"`
			Text *struct {
				Sentence struct {
					StartMs int64  `json:"startMs"`
					Text    string `json:"text"`
				} `json:"sentence"`
			} `json:"text"`
			Fallback *struct {
				Sentence struct {
					StartMs int64  `json:"startMs"`
					Text    string `json:"text"`
				} `json:"sentence"`
			} `json:"fallback"`
		} `json:"section"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing episode text response: %w", err)
	}

	lines := make([]LyricsLine, 0, len(resp.Section))
	var pendingSpeaker string
	for _, s := range resp.Section {
		if s.Title != nil && strings.TrimSpace(s.Title.Title) != "" {
			pendingSpeaker = strings.TrimSpace(s.Title.Title)
			continue
		}

		var sentence string
		var startMs int64
		spoken := false
		switch {
		case s.Text != nil && strings.TrimSpace(s.Text.Sentence.Text) != "":
			sentence = s.Text.Sentence.Text
			startMs = s.Text.Sentence.StartMs
			spoken = true
		case s.Fallback != nil && strings.TrimSpace(s.Fallback.Sentence.Text) != "":
			// music/intro caption
			sentence = s.Fallback.Sentence.Text
			startMs = s.Fallback.Sentence.StartMs
		default:
			continue
		}
		if startMs == 0 {
			startMs = s.StartMs
		}
		if spoken && pendingSpeaker != "" {
			sentence = pendingSpeaker + ": " + sentence
			pendingSpeaker = ""
		}
		lines = append(lines, LyricsLine{
			StartTimeMs: strconv.FormatInt(startMs, 10),
			Words:       sentence,
		})
	}

	if len(lines) == 0 {
		return nil, fmt.Errorf("episode text: no lines: %w", ErrNoLyrics)
	}

	return &LyricsResult{SyncType: "LINE_SYNCED", Lines: lines}, nil
}

// secondary source

// how long to skip the secondary source after a rate-limit
const secondaryCooldown = 60 * time.Second

type secondaryLyricProvider struct {
	log    librespot.Logger
	client *http.Client

	tokenURL    string
	subtitleURL string
	appID       string
	origin      string
	referer     string
	subtitleFmt string

	mu            sync.Mutex
	tok           string
	exp           time.Time
	cooldownUntil time.Time
}

func newSecondaryLyricProvider(logger librespot.Logger) *secondaryLyricProvider {
	jar, jarErr := cookiejar.New(nil)
	if jarErr != nil {
		logger.Warnf("lyrics: cookie jar init failed (secondary path may degrade): %v", jarErr)
	}
	return &secondaryLyricProvider{
		log: logger,
		client: &http.Client{
			Timeout: 10 * time.Second,
			Jar:     jar,
		},
		tokenURL:    os.Getenv("THING_LP_TOKEN_URL"),
		subtitleURL: os.Getenv("THING_LP_SUBTITLE_URL"),
		appID:       os.Getenv("THING_LP_APP_ID"),
		origin:      os.Getenv("THING_LP_ORIGIN"),
		referer:     os.Getenv("THING_LP_REFERER"),
		subtitleFmt: os.Getenv("THING_LP_SUBTITLE_FORMAT"),
	}
}

func (s *secondaryLyricProvider) name() string { return "secondary" }

func (s *secondaryLyricProvider) addHeaders(req *http.Request) {
	// browser header
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	if s.origin != "" {
		req.Header.Set("Origin", s.origin)
	}
	if s.referer != "" {
		req.Header.Set("Referer", s.referer)
	}
}

func (s *secondaryLyricProvider) getToken(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// tokens last about 10 min, refresh at 8
	if s.tok != "" && time.Now().Before(s.exp) {
		return s.tok, nil
	}

	params := url.Values{
		"app_id": {s.appID},
	}
	reqURL := s.tokenURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating token request: %w", err)
	}
	s.addHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading token response: %w", err)
	}

	var tokenResp struct {
		Message struct {
			Header struct {
				StatusCode int `json:"status_code"`
			} `json:"header"`
			Body struct {
				UserToken string `json:"user_token"`
			} `json:"body"`
		} `json:"message"`
	}

	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("parsing token response: %w", err)
	}

	if tokenResp.Message.Header.StatusCode != 200 || tokenResp.Message.Body.UserToken == "" {
		return "", fmt.Errorf("secondary returned status %d or empty token", tokenResp.Message.Header.StatusCode)
	}

	s.tok = tokenResp.Message.Body.UserToken
	s.exp = time.Now().Add(8 * time.Minute)

	s.log.Debugf("lyrics: acquired new secondary token")
	return s.tok, nil
}

func (s *secondaryLyricProvider) invalidateToken() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tok = ""
	s.exp = time.Time{}
	// back off when we are rate limited
	s.cooldownUntil = time.Now().Add(secondaryCooldown)
}

func (s *secondaryLyricProvider) fetch(ctx context.Context, q lyricsQuery) (*LyricsResult, error) {
	if s.tokenURL == "" || s.subtitleURL == "" {
		return nil, fmt.Errorf("secondary disabled (no env config): %w", ErrNoLyrics)
	}

	s.mu.Lock()
	cooling := time.Now().Before(s.cooldownUntil)
	s.mu.Unlock()
	if cooling {
		return nil, errors.New("secondary cooling down after auth/rate-limit error")
	}

	token, err := s.getToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting secondary token: %w", err)
	}

	params := url.Values{
		"format":            {"json"},
		"namespace":         {"lyrics_richsynched"},
		"subtitle_format":   {s.subtitleFmt},
		"app_id":            {s.appID},
		"usertoken":         {token},
		"q_track":           {q.trackName},
		"q_artist":          {q.artistName},
		"q_artists":         {q.artistName},
		"f_subtitle_length": {""},
	}
	if q.albumName != "" {
		params.Set("q_album", q.albumName)
	}
	if q.durationMs > 0 {
		params.Set("q_duration", strconv.Itoa(q.durationMs/1000))
	}

	reqURL := s.subtitleURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating subtitle request: %w", err)
	}
	s.addHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("subtitle request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading subtitle response: %w", err)
	}

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		s.invalidateToken()
		return nil, fmt.Errorf("secondary auth error (status %d), token invalidated", resp.StatusCode)
	}

	return s.parseResponse(body)
}

func (s *secondaryLyricProvider) parseResponse(body []byte) (*LyricsResult, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	msgRaw, ok := raw["message"]
	if !ok {
		return nil, fmt.Errorf("no message in response")
	}

	var msg struct {
		Header struct {
			StatusCode int `json:"status_code"`
		} `json:"header"`
		Body struct {
			MacroCalls map[string]json.RawMessage `json:"macro_calls"`
		} `json:"body"`
	}
	if err := json.Unmarshal(msgRaw, &msg); err != nil {
		return nil, fmt.Errorf("parsing message: %w", err)
	}

	if msg.Header.StatusCode != 200 {
		// provider signals a dead/rate-limited token via the in-body status_code
		if msg.Header.StatusCode == 401 || msg.Header.StatusCode == 403 {
			s.invalidateToken()
		}
		return nil, fmt.Errorf("secondary status %d", msg.Header.StatusCode)
	}

	// try synced first
	if subtitlesRaw, ok := msg.Body.MacroCalls["track.subtitles.get"]; ok {
		result, err := s.parseSubtitles(subtitlesRaw)
		if err == nil && result != nil {
			return result, nil
		}
		s.log.Debugf("lyrics: synced subtitles parse failed, trying plain lyrics: %v", err)
	}

	if lyricsRaw, ok := msg.Body.MacroCalls["track.lyrics.get"]; ok {
		result, err := s.parsePlain(lyricsRaw)
		if err == nil && result != nil {
			return result, nil
		}
	}

	return nil, fmt.Errorf("no lyrics data in secondary response: %w", ErrNoLyrics)
}

func (s *secondaryLyricProvider) parseSubtitles(raw json.RawMessage) (*LyricsResult, error) {
	// body comes as either an object or an empty array
	var envelope struct {
		Message struct {
			Header struct {
				StatusCode int `json:"status_code"`
			} `json:"header"`
			Body json.RawMessage `json:"body"`
		} `json:"message"`
	}

	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("parsing subtitle envelope: %w", err)
	}

	if envelope.Message.Header.StatusCode != 200 {
		return nil, fmt.Errorf("subtitle status %d", envelope.Message.Header.StatusCode)
	}

	body := bytes.TrimSpace(envelope.Message.Body)
	if len(body) == 0 || body[0] != '{' {
		// "no synced lyrics" fall through to plain or LRCLIB
		return nil, fmt.Errorf("no synced subtitles")
	}

	var subBody struct {
		SubtitleList []struct {
			Subtitle struct {
				SubtitleBody string `json:"subtitle_body"`
			} `json:"subtitle"`
		} `json:"subtitle_list"`
	}
	if err := json.Unmarshal(envelope.Message.Body, &subBody); err != nil {
		return nil, fmt.Errorf("parsing subtitle body envelope: %w", err)
	}

	if len(subBody.SubtitleList) == 0 {
		return nil, fmt.Errorf("empty subtitle list")
	}

	subtitleBody := subBody.SubtitleList[0].Subtitle.SubtitleBody
	if subtitleBody == "" {
		return nil, fmt.Errorf("empty subtitle body")
	}

	var lpLines []struct {
		Text string `json:"text"`
		Time struct {
			Total float64 `json:"total"`
		} `json:"time"`
	}

	if err := json.Unmarshal([]byte(subtitleBody), &lpLines); err != nil {
		return nil, fmt.Errorf("parsing subtitle body: %w", err)
	}

	if len(lpLines) == 0 {
		return nil, fmt.Errorf("no lines in subtitle body")
	}

	lines := make([]LyricsLine, 0, len(lpLines))
	for _, ml := range lpLines {
		ms := int(ml.Time.Total * 1000)
		lines = append(lines, LyricsLine{
			StartTimeMs: strconv.Itoa(ms),
			Words:       ml.Text,
		})
	}

	return &LyricsResult{
		SyncType: "LINE_SYNCED",
		Lines:    lines,
	}, nil
}

func (s *secondaryLyricProvider) parsePlain(raw json.RawMessage) (*LyricsResult, error) {
	var lyr struct {
		Message struct {
			Header struct {
				StatusCode int `json:"status_code"`
			} `json:"header"`
			Body struct {
				Lyrics struct {
					LyricsBody string `json:"lyrics_body"`
				} `json:"lyrics"`
			} `json:"body"`
		} `json:"message"`
	}

	if err := json.Unmarshal(raw, &lyr); err != nil {
		return nil, fmt.Errorf("parsing lyrics: %w", err)
	}

	if lyr.Message.Header.StatusCode != 200 || lyr.Message.Body.Lyrics.LyricsBody == "" {
		return nil, fmt.Errorf("no plain lyrics")
	}

	return plainTextToResult(lyr.Message.Body.Lyrics.LyricsBody), nil
}

// fetchRichsync gets word by word timing
func (s *secondaryLyricProvider) fetchRichsync(ctx context.Context, q lyricsQuery) (*LyricsResult, error) {
	if s.tokenURL == "" || s.subtitleURL == "" {
		return nil, errors.New("richsync disabled (no env config)")
	}

	s.mu.Lock()
	cooling := time.Now().Before(s.cooldownUntil)
	s.mu.Unlock()
	if cooling {
		return nil, errors.New("richsync cooling down")
	}

	token, err := s.getToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("richsync token: %w", err)
	}

	commonTrackID, err := s.resolveCommonTrackID(ctx, token, q)
	if err != nil {
		return nil, err
	}

	base := s.subtitleURL[:strings.LastIndex(s.subtitleURL, "/")+1]
	params := url.Values{
		"format":         {"json"},
		"app_id":         {s.appID},
		"usertoken":      {token},
		"commontrack_id": {commonTrackID},
	}
	req, err := http.NewRequestWithContext(ctx, "GET", base+"track.richsync.get?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating richsync request: %w", err)
	}
	s.addHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("richsync request failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var env struct {
		Message struct {
			Header struct {
				StatusCode int `json:"status_code"`
			} `json:"header"`
			Body json.RawMessage `json:"body"`
		} `json:"message"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("parsing richsync envelope: %w", err)
	}

	sc := env.Message.Header.StatusCode
	switch {
	case sc == 401 || sc == 403 || sc == 429:
		s.invalidateToken()
		return nil, fmt.Errorf("richsync rate-limit (status %d), cooling down", sc)
	case sc != 200:
		// 404
		return nil, fmt.Errorf("richsync status %d (no word-level for track)", sc)
	}

	var rb struct {
		Richsync struct {
			RichsyncBody string `json:"richsync_body"`
		} `json:"richsync"`
	}
	if err := json.Unmarshal(env.Message.Body, &rb); err != nil {
		// ratelimit
		s.invalidateToken()
		return nil, fmt.Errorf("richsync rate-limit (200 + non-object body), cooling down")
	}

	return parseRichsync(rb.Richsync.RichsyncBody)
}

func (s *secondaryLyricProvider) resolveCommonTrackID(ctx context.Context, token string, q lyricsQuery) (string, error) {
	base := s.subtitleURL[:strings.LastIndex(s.subtitleURL, "/")+1]
	params := url.Values{
		"format":    {"json"},
		"app_id":    {s.appID},
		"usertoken": {token},
		"q_track":   {q.trackName},
		"q_artist":  {q.artistName},
	}
	req, err := http.NewRequestWithContext(ctx, "GET", base+"matcher.track.get?"+params.Encode(), nil)
	if err != nil {
		return "", fmt.Errorf("creating matcher request: %w", err)
	}
	s.addHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("matcher request failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	// decode the header separately from a raw body
	var env struct {
		Message struct {
			Header struct {
				StatusCode int `json:"status_code"`
			} `json:"header"`
			Body json.RawMessage `json:"body"`
		} `json:"message"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return "", fmt.Errorf("parsing matcher envelope: %w", err)
	}
	sc := env.Message.Header.StatusCode
	var b struct {
		Track struct {
			CommonTrackID int64 `json:"commontrack_id"`
		} `json:"track"`
	}
	if sc == 401 || sc == 403 || sc == 429 || json.Unmarshal(env.Message.Body, &b) != nil {
		s.invalidateToken()
		return "", fmt.Errorf("matcher rate-limit (status %d), cooling down", sc)
	}
	if b.Track.CommonTrackID == 0 {
		return "", fmt.Errorf("richsync: no track match")
	}
	return strconv.FormatInt(b.Track.CommonTrackID, 10), nil
}

func parseRichsync(richsyncBody string) (*LyricsResult, error) {
	if richsyncBody == "" {
		return nil, fmt.Errorf("richsync: empty body")
	}

	var segs []struct {
		Ts float64 `json:"ts"`
		X  string  `json:"x"`
		L  []struct {
			C string  `json:"c"`
			O float64 `json:"o"`
		} `json:"l"`
	}
	if err := json.Unmarshal([]byte(richsyncBody), &segs); err != nil {
		return nil, fmt.Errorf("parsing richsync body: %w", err)
	}
	if len(segs) == 0 {
		return nil, fmt.Errorf("richsync: no lines")
	}

	lines := make([]LyricsLine, 0, len(segs))
	for _, seg := range segs {
		words := make([]LyricsWord, 0, len(seg.L))
		for _, w := range seg.L {
			words = append(words, LyricsWord{
				StartTimeMs: strconv.Itoa(int((seg.Ts + w.O) * 1000)),
				Word:        w.C,
			})
		}
		lines = append(lines, LyricsLine{
			StartTimeMs: strconv.Itoa(int(seg.Ts * 1000)),
			Words:       seg.X,
			Syllables:   words,
		})
	}

	return &LyricsResult{SyncType: "LINE_SYNCED", Lines: lines}, nil
}

// final source LRCLIB (public api w/no auth but not as much lyrics)
const lrclibURL = "https://lrclib.net/api/get"

type tertiaryLyricProvider struct {
	log    librespot.Logger
	client *http.Client
	url    string
}

func newTertiaryLyricProvider(logger librespot.Logger) *tertiaryLyricProvider {
	return &tertiaryLyricProvider{
		log:    logger,
		client: &http.Client{Timeout: 10 * time.Second},
		url:    lrclibURL,
	}
}

func (t *tertiaryLyricProvider) name() string { return "lrclib" }

func (t *tertiaryLyricProvider) fetch(ctx context.Context, q lyricsQuery) (*LyricsResult, error) {
	params := url.Values{
		"track_name":  {q.trackName},
		"artist_name": {q.artistName},
	}
	if q.durationMs > 0 {
		params.Set("duration", strconv.Itoa(q.durationMs/1000))
	}

	reqURL := t.url + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating lrclib request: %w", err)
	}
	req.Header.Set("User-Agent", "go-librespot-observer/1.0")

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("lrclib request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("lrclib: no lyrics found: %w", ErrNoLyrics)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading lrclib response: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("lrclib status %d: %s", resp.StatusCode, string(body))
	}

	var lrcResp struct {
		Instrumental bool   `json:"instrumental"`
		SyncedLyrics string `json:"syncedLyrics"`
		PlainLyrics  string `json:"plainLyrics"`
	}

	if err := json.Unmarshal(body, &lrcResp); err != nil {
		return nil, fmt.Errorf("parsing lrclib response: %w", err)
	}

	if lrcResp.Instrumental {
		return &LyricsResult{
			SyncType: "UNSYNCED",
			Lines:    []LyricsLine{{StartTimeMs: "0", Words: "♪ Instrumental ♪"}},
		}, nil
	}

	if lrcResp.SyncedLyrics != "" {
		result, err := parseLRC(lrcResp.SyncedLyrics)
		if err == nil && len(result.Lines) > 0 {
			return result, nil
		}
	}

	if lrcResp.PlainLyrics != "" {
		return plainTextToResult(lrcResp.PlainLyrics), nil
	}

	return nil, fmt.Errorf("lrclib: empty lyrics: %w", ErrNoLyrics)
}

// helpers

// matches lines like [03:20.31] some text
var lrcLineRegex = regexp.MustCompile(`^\[(\d{2}):(\d{2})\.(\d{2,3})\]\s?(.*)$`)

func parseLRC(lrc string) (*LyricsResult, error) {
	rawLines := strings.Split(lrc, "\n")
	lines := make([]LyricsLine, 0, len(rawLines))

	for _, raw := range rawLines {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		matches := lrcLineRegex.FindStringSubmatch(raw)
		if matches == nil {
			continue
		}

		minutes, _ := strconv.Atoi(matches[1])
		seconds, _ := strconv.Atoi(matches[2])

		// support both 2-digit and 3-digit fractions
		fracStr := matches[3]
		frac, _ := strconv.Atoi(fracStr)
		if len(fracStr) == 2 {
			frac *= 10
		}

		ms := minutes*60*1000 + seconds*1000 + frac
		text := matches[4]

		lines = append(lines, LyricsLine{
			StartTimeMs: strconv.Itoa(ms),
			Words:       text,
		})
	}

	if len(lines) == 0 {
		return nil, fmt.Errorf("no valid LRC lines found")
	}

	return &LyricsResult{
		SyncType: "LINE_SYNCED",
		Lines:    lines,
	}, nil
}

func plainTextToResult(text string) *LyricsResult {
	rawLines := strings.Split(text, "\n")
	lines := make([]LyricsLine, 0, len(rawLines))

	for _, line := range rawLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lines = append(lines, LyricsLine{
			StartTimeMs: "0",
			Words:       line,
		})
	}

	if len(lines) == 0 {
		return nil
	}

	return &LyricsResult{
		SyncType: "UNSYNCED",
		Lines:    lines,
	}
}
