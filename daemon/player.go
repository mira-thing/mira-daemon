package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/devgianlu/go-librespot/ap"
	"github.com/devgianlu/go-librespot/dealer"
	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	extmetadatapb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
	"github.com/devgianlu/go-librespot/session"
)

// MaxStateVolume is the maximum volume value used in Spotify connect state
const MaxStateVolume = 65535

type AppPlayer struct {
	app  *App
	sess *session.Session

	stop   chan struct{}
	logout chan *AppPlayer

	spotConnId string

	registerGen       atomic.Uint64
	registered        atomic.Bool
	heartbeatInFlight atomic.Bool
	clusterCh         chan *connectpb.Cluster

	prodInfo    *ProductInfo
	countryCode *string

	hasSpotConnId          bool
	hasInitialConnectState bool
	hasCountryCode         bool
	playbackReadyCh        chan struct{}
	playbackReadyOnce      sync.Once

	state *State

	// clockEst learns the local-vs-server clock offset from cluster deliveries
	// so stale snapshots can be aged correctly without trusting wall time.
	clockEst       clockOffsetEstimator
	clockEstSeeded bool
	// lastSampledClusterTs dedupes estimator samples: redelivered snapshots
	// share a Timestamp and must not count as fresh clock evidence
	lastSampledClusterTs int64

	prefetchTimer *time.Timer

	// lyricsProvider handles fetching lyrics from primary + LRCLIB sources
	lyricsProvider *LyricsProvider

	// queueResolver fills in artist/album names for queue entries
	queueResolver   *queueResolver
	queueResolvedCh chan struct{}

	// async artist/album resolution
	metaResolvedCh       chan resolvedTrackMeta
	metaResolveInFlight  string
	metaResolveFailedUri string
}

type resolvedTrackMeta struct {
	uri      string
	name     string
	artist   string
	album    string
	imageUrl string
}

func (p *AppPlayer) playbackReady() bool {
	select {
	case <-p.playbackReadyCh:
		return true
	default:
		return false
	}
}

func (p *AppPlayer) notifyPlaybackReadyIfNeeded() {
	if !p.hasSpotConnId || !p.hasInitialConnectState || !p.hasCountryCode {
		return
	}

	p.playbackReadyOnce.Do(func() {
		close(p.playbackReadyCh)
		p.app.server.Emit(&ApiEvent{Type: ApiEventTypePlaybackReady})
	})
}

func (p *AppPlayer) handleAccesspointPacket(pktType ap.PacketType, payload []byte) error {
	switch pktType {
	case ap.PacketTypeProductInfo:
		var prod ProductInfo
		if err := xml.Unmarshal(payload, &prod); err != nil {
			return fmt.Errorf("failed umarshalling ProductInfo: %w", err)
		}

		if len(prod.Products) != 1 {
			return fmt.Errorf("invalid ProductInfo")
		}

		p.prodInfo = &prod
		return nil
	case ap.PacketTypeCountryCode:
		*p.countryCode = string(payload)
		p.hasCountryCode = true
		p.notifyPlaybackReadyIfNeeded()
		return nil
	default:
		return nil
	}
}

// registerAsync registers this device with the connect cluster
func (p *AppPlayer) registerAsync(connId string, gen uint64) {
	waits := []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second}
	const slowRetry = 30 * time.Second

	for attempt := 0; ; attempt++ {
		wait := slowRetry
		if attempt < len(waits) {
			wait = waits[attempt]
		}
		if wait > 0 {
			time.Sleep(wait)
		}

		// superseded by a newer connection-id
		if p.registerGen.Load() != gen || p.app.currentPlayer.Load() != p {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		cluster, err := p.putConnectState(ctx, connId, connectpb.PutStateReason_NEW_DEVICE)
		cancel()
		if err == nil {
			if p.registerGen.Load() == gen {
				p.registered.Store(true)
				// the registration response carries the current cluster
				p.injectCluster(cluster)
			}
			if attempt > 0 {
				p.app.log.Debugf("connect state put landed on attempt %d", attempt+1)
			}
			return
		}

		// warn through the fast ladder
		if attempt < len(waits) {
			p.app.log.WithError(err).Warnf("connect state put failed (attempt %d), retrying", attempt+1)
		} else {
			p.app.log.WithError(err).Debugf("connect state put failed (attempt %d), retrying in %s", attempt+1, slowRetry)
		}
	}
}

// connectStateHeartbeatInterval keeps our cluster registration alive
const connectStateHeartbeatInterval = 4 * time.Minute

// heartbeatConnectState re-puts our connect state periodically
func (p *AppPlayer) heartbeatConnectState() {
	if !p.hasSpotConnId || !p.registered.Load() {
		// registerAsync is still retrying, don't pile on
		return
	}
	if !p.heartbeatInFlight.CompareAndSwap(false, true) {
		return
	}

	connId, gen := p.spotConnId, p.registerGen.Load()
	go func() {
		defer p.heartbeatInFlight.Store(false)

		for attempt := 1; attempt <= 2; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			cluster, err := p.putConnectState(ctx, connId, connectpb.PutStateReason_NEW_DEVICE)
			cancel()
			if err == nil {
				p.app.log.Tracef("connect state heartbeat ok")
				// keep the device list fresh even if no dealer push arrived
				if p.registerGen.Load() == gen {
					p.injectCluster(cluster)
				}
				return
			}

			// a newer connection-id took over, its registerAsync owns recovery now
			if p.registerGen.Load() != gen || p.app.currentPlayer.Load() != p {
				return
			}

			p.app.log.WithError(err).Warnf("connect state heartbeat failed (attempt %d)", attempt)
			if attempt == 1 {
				time.Sleep(5 * time.Second)
			}
		}

		p.registered.Store(false)
		p.sess.Dealer().ForceReconnect()
	}()
}

func (p *AppPlayer) handleDealerMessage(ctx context.Context, msg dealer.Message) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if strings.HasPrefix(msg.Uri, "hm://pusher/v1/connections/") {
		p.spotConnId = msg.Headers["Spotify-Connection-Id"]
		p.hasSpotConnId = p.spotConnId != ""
		if len(p.spotConnId) >= 32 {
			p.app.log.Debugf("received connection id: %s...%s", p.spotConnId[:16], p.spotConnId[len(p.spotConnId)-16:])
		} else {
			p.app.log.Debugf("received connection id (%d bytes)", len(p.spotConnId))
		}

		// a fresh connection-id voids any previous registration
		p.app.log.Infof("dealer connection-id received (%d bytes), re-registering with connect cluster", len(p.spotConnId))
		p.registered.Store(false)
		go p.registerAsync(p.spotConnId, p.registerGen.Add(1))

		p.hasInitialConnectState = true
		p.notifyPlaybackReadyIfNeeded()
		return nil
	} else if strings.HasPrefix(msg.Uri, "hm://connect-state/v1/cluster") {
		var clusterUpdate connectpb.ClusterUpdate
		if err := proto.Unmarshal(msg.Payload, &clusterUpdate); err != nil {
			return fmt.Errorf("failed unmarshalling ClusterUpdate: %w", err)
		}

		p.handleCluster(ctx, clusterUpdate.Cluster)
		return nil
	}

	p.app.log.Debugf("skipping dealer message, uri: %s", msg.Uri)
	return nil
}

// handleCluster applies a Cluster snapshot
func (p *AppPlayer) handleCluster(ctx context.Context, cluster *connectpb.Cluster) {
	if cluster == nil {
		return
	}

	activeDeviceId := cluster.ActiveDeviceId

	// snapshot the selectable device list
	p.updateConnectDevices(cluster)

	// "is anything active" signal is ActiveDeviceId
	switch {
	case activeDeviceId == p.app.deviceId:
		// ignore we cannot playback on the car thing
	case activeDeviceId == "":
		// nothing is active anywhere so we go idle.
		p.clearActiveDevice()
	case cluster.PlayerState != nil:
		p.updateRemoteState(ctx, cluster)
	}
}

// injectCluster hands a Cluster from a put_state response
func (p *AppPlayer) injectCluster(cluster *connectpb.Cluster) {
	if cluster == nil {
		return
	}
	select {
	case p.clusterCh <- cluster:
	default:
		// drop the previous queued snapshot in favour of this newer one
		select {
		case <-p.clusterCh:
		default:
		}
		select {
		case p.clusterCh <- cluster:
		default:
		}
	}
}

func (p *AppPlayer) handleDealerRequest(ctx context.Context, req dealer.Request) error {
	// Observer mode: reject all player commands
	p.app.log.Debugf("observer mode: rejecting player command %s from %s",
		req.Payload.Command.Endpoint, req.Payload.SentByDeviceId)
	return nil
}

// updateRemoteState pulls active device state from a cluster + stores it
func clusterToRemoteState(cluster *connectpb.Cluster) *RemoteState {
	if cluster == nil || cluster.PlayerState == nil {
		return nil
	}

	ps := cluster.PlayerState
	track := ps.Track

	activeDeviceId := cluster.ActiveDeviceId
	var deviceName, deviceType string
	var volume uint32
	var volumeDisabled bool
	var volumeSteps int32
	if dev, ok := cluster.Device[activeDeviceId]; ok {
		deviceName = dev.Name
		deviceType = dev.DeviceType.String()
		volume = dev.Volume
		if dev.Capabilities != nil {
			volumeDisabled = dev.Capabilities.DisableVolume
			volumeSteps = dev.Capabilities.VolumeSteps
		}
	}

	trackUri := ""
	trackName := ""
	imageUrl := ""
	contextUri := ""
	var rawMeta map[string]string

	if track != nil {
		trackUri = track.Uri
		if track.Metadata != nil {
			trackName = track.Metadata["title"]
			rawMeta = track.Metadata

			for _, k := range []string{"image_url", "image_xlarge_url", "image_large_url", "image_small_url"} {
				if img := track.Metadata[k]; img != "" {
					imageUrl = convertSpotifyImageUrl(img)
					break
				}
			}
		}
	}

	if ps.ContextUri != "" {
		contextUri = ps.ContextUri
	}

	var contextName string
	if ps.ContextMetadata != nil {
		contextName = ps.ContextMetadata["context_description"]
	}

	rs := &RemoteState{
		DeviceId:              activeDeviceId,
		DeviceName:            deviceName,
		DeviceType:            deviceType,
		TrackUri:              trackUri,
		TrackName:             trackName,
		TrackImageUrl:         imageUrl,
		ContextUri:            contextUri,
		ContextName:           contextName,
		Duration:              int64(ps.Duration),
		PositionAsOfTimestamp: ps.PositionAsOfTimestamp,
		Timestamp:             ps.Timestamp,
		IsPlaying:             !ps.IsPaused && ps.IsPlaying,
		IsPaused:              ps.IsPaused,
		PlaybackSpeed:         ps.PlaybackSpeed,
		Volume:                volume,
		VolumeDisabled:        volumeDisabled,
		VolumeSteps:           volumeSteps,
		ShuffleContext:        ps.Options != nil && ps.Options.ShufflingContext,
		RepeatContext:         ps.Options != nil && ps.Options.RepeatingContext,
		RepeatTrack:           ps.Options != nil && ps.Options.RepeatingTrack,
		DisallowSkipPrev:      ps.Restrictions != nil && len(ps.Restrictions.DisallowSkippingPrevReasons) > 0,
		DisallowSkipNext:      ps.Restrictions != nil && len(ps.Restrictions.DisallowSkippingNextReasons) > 0,
		DisallowSeek:          ps.Restrictions != nil && len(ps.Restrictions.DisallowSeekingReasons) > 0,
		PrevTracks:            projectQueue(ps.PrevTracks, QueueLimit),
		NextTracks:            projectQueue(ps.NextTracks, QueueLimit),
		RawMetadata:           rawMeta,
	}
	now := time.Now()
	rs.ReceivedAt = now
	rs.ReceivedAtWallMs = now.UnixMilli()
	rs.Position = rs.RemotePosition()
	return rs
}

const clockSyncedFlag = "/run/clock_synced"

func (p *AppPlayer) noteClusterTiming(rs *RemoteState) {
	if !p.clockEstSeeded {
		if _, err := os.Stat(clockSyncedFlag); err != nil {
			rs.Position = rs.RemotePosition()
			return
		}
		p.clockEst.add(0)
		p.clockEstSeeded = true
	}
	if rs.Timestamp > 0 && rs.Timestamp != p.lastSampledClusterTs {
		p.clockEst.add(rs.ReceivedAtWallMs - rs.Timestamp)
		p.lastSampledClusterTs = rs.Timestamp
	}
	if off, ok := p.clockEst.offset(); ok {
		rs.clockOffsetMs = off
		rs.offsetKnown = true
	}
	rs.Position = rs.RemotePosition()
}

func (p *AppPlayer) updateRemoteState(ctx context.Context, cluster *connectpb.Cluster) {
	rs := clusterToRemoteState(cluster)
	if rs == nil {
		return
	}
	p.noteClusterTiming(rs)
	if dev, ok := cluster.Device[rs.DeviceId]; ok {
		rs.DeviceName = p.deviceDisplayName(rs.DeviceId, dev)
	}
	track := cluster.PlayerState.Track
	if prev := p.state.remoteState; prev != nil && prev.TrackUri == rs.TrackUri {
		delta := rs.RemotePosition() - prev.RemotePosition()
		staleMs := rs.ReceivedAtWallMs - rs.Timestamp - rs.clockOffsetMs
		if delta < -1500 && staleMs > 5000 {
			// a backward jump sourced from a STALE snapshot is the rewind
			// bug; a fresh-timestamp regression is just the user seeking back
			p.app.log.Warnf("cluster: position regressed %dms on %q (posAsOf %d->%d ts %d->%d stale %dms playing=%v paused=%v)",
				delta, rs.TrackUri, prev.PositionAsOfTimestamp, rs.PositionAsOfTimestamp,
				prev.Timestamp, rs.Timestamp, staleMs, rs.IsPlaying, rs.IsPaused)
		} else {
			p.app.log.Debugf("cluster: apply %q pos %dms (delta %+dms posAsOf %d ts %d)",
				rs.TrackUri, rs.RemotePosition(), delta, rs.PositionAsOfTimestamp, rs.Timestamp)
		}
	} else {
		p.app.log.Debugf("cluster: apply new track %q pos %dms (posAsOf %d ts %d playing=%v)",
			rs.TrackUri, rs.RemotePosition(), rs.PositionAsOfTimestamp, rs.Timestamp, rs.IsPlaying)
	}

	// Fill in any cached artist/album for the queue entries
	if p.queueResolver != nil {
		needNext := p.queueResolver.applyCache(rs.NextTracks)
		needPrev := p.queueResolver.applyCache(rs.PrevTracks)
		p.queueResolver.ResolveAsync(append(needNext, needPrev...))
	}

	// Resolve artist and album from track metadata or spclient.
	// Unofficial connect devices often send a bare URI with empty metadata
	artistName := ""
	albumName := ""
	if track != nil && track.Metadata != nil {
		artistName = track.Metadata["artist_name"]
		albumName = track.Metadata["album_title"]
	}

	if rs.TrackUri != "" && (artistName == "" || rs.TrackName == "" || rs.TrackImageUrl == "") {
		spotId, err := librespot.SpotifyIdFromUri(rs.TrackUri)
		if err == nil && spotId != nil {
			// carry anything already resolved for this same track
			if prevState := p.state.remoteState; prevState != nil && prevState.TrackUri == rs.TrackUri {
				if artistName == "" && prevState.TrackArtist != "" {
					artistName = prevState.TrackArtist
					if albumName == "" {
						albumName = prevState.TrackAlbum
					}
				}
				if rs.TrackName == "" {
					rs.TrackName = prevState.TrackName
				}
				if rs.TrackImageUrl == "" {
					rs.TrackImageUrl = prevState.TrackImageUrl
				}
			}
			if artistName == "" && p.queueResolver != nil {
				if a, alb, ok := p.queueResolver.lookup(rs.TrackUri); ok {
					artistName, albumName = a, alb
				}
			}
			if artistName == "" || rs.TrackName == "" || rs.TrackImageUrl == "" {
				// resolve via spclient WITHOUT blocking the run loop
				p.resolveCurrentTrackMetaAsync(rs.TrackUri, *spotId)
			}
		}
	}

	rs.TrackArtist = artistName
	rs.TrackAlbum = firstNonEmpty(albumName, rs.RawMetadata["album_title"])

	prevState := p.state.remoteState
	trackChanged := prevState == nil || prevState.TrackUri != rs.TrackUri

	p.state.remoteState = rs
	// remember the active device
	if rs.DeviceId != "" {
		p.state.lastActiveDeviceId = rs.DeviceId
		p.state.lastActiveDeviceName = rs.DeviceName
	}

	if v := p.app.voice; v != nil {
		v.notifyPlayback(rs.IsPlaying && !rs.IsPaused)
	}

	if trackChanged {
		p.app.log.Debugf("observer: track changed to %q by %s on %s", rs.TrackName, rs.TrackArtist, rs.DeviceName)
		p.app.server.Emit(&ApiEvent{
			Type: ApiEventTypeObserverTrackChanged,
			Data: rs,
		})
	} else {
		p.app.server.Emit(&ApiEvent{
			Type: ApiEventTypeObserverStateChanged,
			Data: rs,
		})
	}
}

// ConnectDevice is a selectable Spotify Connect device
type ConnectDevice struct {
	Id             string `json:"id"`
	Name           string `json:"name"`
	Type           string `json:"type"`
	Volume         uint32 `json:"volume"`
	VolumeSteps    int32  `json:"volume_steps"`
	VolumeDisabled bool   `json:"volume_disabled"`
	IsActive       bool   `json:"is_active"`
	IsOffline      bool   `json:"is_offline"`
	CanTransfer    bool   `json:"can_transfer"`
}

func looksLikeDeviceId(s string) bool {
	if len(s) < 16 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r >= 'A' && r <= 'F', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// the best human readable name a DeviceInfo offers
func friendlyDeviceName(d *connectpb.DeviceInfo) string {
	if alias, ok := d.DeviceAliases[d.SelectedAliasId]; ok && alias.GetDisplayName() != "" {
		return alias.GetDisplayName()
	}
	var bestId uint32
	best := ""
	for id, alias := range d.DeviceAliases {
		if alias.GetDisplayName() != "" && (best == "" || id < bestId) {
			best, bestId = alias.GetDisplayName(), id
		}
	}
	if best != "" {
		return best
	}
	if d.Name != "" && !looksLikeDeviceId(d.Name) {
		return d.Name
	}
	if s := strings.TrimSpace(d.Brand + " " + d.Model); s != "" && !looksLikeDeviceId(s) {
		return s
	}
	return ""
}

// resolves a display name remembering the last good one per device id
func (p *AppPlayer) deviceDisplayName(id string, d *connectpb.DeviceInfo) string {
	if name := friendlyDeviceName(d); name != "" {
		if p.state.connectDeviceNames == nil {
			p.state.connectDeviceNames = map[string]string{}
		}
		p.state.connectDeviceNames[id] = name
		return name
	}
	if cached := p.state.connectDeviceNames[id]; cached != "" {
		return cached
	}
	return d.Name
}

// snapshots the selectable connect devices from a cluster
func (p *AppPlayer) updateConnectDevices(cluster *connectpb.Cluster) {
	activeDeviceId := cluster.ActiveDeviceId
	devs := make([]ConnectDevice, 0, len(cluster.Device))
	for id, d := range cluster.Device {
		if id == p.app.deviceId {
			continue
		}
		// ghost/stale cluster entries aren't selectable, don't list them
		if d.IsOffline && id != activeDeviceId {
			continue
		}
		cd := ConnectDevice{
			Id:          id,
			Name:        p.deviceDisplayName(id, d),
			Type:        d.DeviceType.String(),
			Volume:      d.Volume,
			IsActive:    id == activeDeviceId,
			IsOffline:   d.IsOffline,
			CanTransfer: len(d.DisallowTransferReasons) == 0,
		}
		if d.Capabilities != nil {
			cd.VolumeSteps = d.Capabilities.VolumeSteps
			cd.VolumeDisabled = d.Capabilities.DisableVolume
		}
		devs = append(devs, cd)
	}
	// active device first, then alphabetical
	sort.Slice(devs, func(i, j int) bool {
		if devs[i].IsActive != devs[j].IsActive {
			return devs[i].IsActive
		}
		return devs[i].Name < devs[j].Name
	})

	p.state.connectDevices = devs

	// only emit when the meaningful shape changes
	sig := connectDevicesSignature(devs)
	if sig == p.state.connectDevSig {
		return
	}
	p.state.connectDevSig = sig
	p.app.server.Emit(&ApiEvent{Type: ApiEventTypeConnectDevices, Data: devs})
}

func connectDevicesSignature(devs []ConnectDevice) string {
	var sb strings.Builder
	for _, d := range devs {
		fmt.Fprintf(&sb, "%s:%s:%t:%t;", d.Id, d.Name, d.IsActive, d.IsOffline)
	}
	return sb.String()
}

// returns the device snapshot, never nil
func (p *AppPlayer) connectDevicesOrEmpty() []ConnectDevice {
	if p.state.connectDevices == nil {
		return []ConnectDevice{}
	}
	return p.state.connectDevices
}

// drops the observed remote state when no device is active
func (p *AppPlayer) clearActiveDevice() {
	if p.state.remoteState == nil {
		return
	}
	p.state.remoteState = nil
	if v := p.app.voice; v != nil {
		v.notifyPlayback(false)
	}
	p.app.server.Emit(&ApiEvent{Type: ApiEventTypeObserverInactive})
}

// resolveTrackMetadata tries spclient first (fast), falls back to the web API
// when spclient is unavailable
func (p *AppPlayer) resolveTrackMetadata(ctx context.Context, spotId librespot.SpotifyId) resolvedTrackMeta {
	meta := p.resolveViaSpclient(ctx, spotId)
	if meta.artist != "" {
		return meta
	}

	if wb := p.resolveViaWebApi(ctx, spotId); wb.artist != "" {
		return wb
	}
	return meta
}

// fetches metadata for the current track off the run loop
func (p *AppPlayer) resolveCurrentTrackMetaAsync(uri string, spotId librespot.SpotifyId) {
	if p.metaResolveInFlight == uri || p.metaResolveFailedUri == uri {
		return
	}
	p.metaResolveInFlight = uri
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var meta resolvedTrackMeta
		if strings.HasPrefix(uri, "spotify:episode:") {
			// podcasts dont carry an artist name
			meta = p.resolveEpisodeMetadata(ctx, spotId)
		} else {
			meta = p.resolveTrackMetadata(ctx, spotId)
		}
		meta.uri = uri
		select {
		case p.metaResolvedCh <- meta:
		default:
			// drop
		}
	}()
}

// picks the closest cover file id and turns it into an image CDN url
func coverImageUrl(images []*metadatapb.Image, size string) string {
	fileId := getBestImageIdForSize(images, size)
	if fileId == nil {
		return ""
	}
	return "https://i.scdn.co/image/" + hex.EncodeToString(fileId)
}

// resolveEpisodeMetadata resolves a podcast episode's show name
func (p *AppPlayer) resolveEpisodeMetadata(ctx context.Context, spotId librespot.SpotifyId) resolvedTrackMeta {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var ep metadatapb.Episode
	if err := p.sess.Spclient().ExtendedMetadataSimple(reqCtx, spotId,
		extmetadatapb.ExtensionKind_EPISODE_V4, &ep); err != nil {
		p.app.log.Debugf("observer: spclient episode metadata for %s failed: %v", spotId.Uri(), err)
		return resolvedTrackMeta{}
	}

	var meta resolvedTrackMeta
	meta.name = ep.GetName()
	if ep.Show != nil && ep.Show.Name != nil {
		meta.artist = *ep.Show.Name
		meta.album = *ep.Show.Name
	}
	if ep.CoverImage != nil {
		meta.imageUrl = coverImageUrl(ep.CoverImage.Image, p.app.cfg.ImageSize)
	}
	p.app.log.Debugf("observer: spclient episode metadata for %s: show=%q", spotId.Uri(), meta.artist)
	return meta
}

func (p *AppPlayer) resolveViaSpclient(ctx context.Context, spotId librespot.SpotifyId) resolvedTrackMeta {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var trackMeta metadatapb.Track
	if err := p.sess.Spclient().ExtendedMetadataSimple(reqCtx, spotId,
		extmetadatapb.ExtensionKind_TRACK_V4, &trackMeta); err != nil {
		p.app.log.Debugf("observer: spclient metadata for %s failed: %v", spotId.Uri(), err)
		return resolvedTrackMeta{}
	}

	var meta resolvedTrackMeta
	if trackMeta.Name != nil {
		meta.name = *trackMeta.Name
	}
	if len(trackMeta.Artist) > 0 && trackMeta.Artist[0].Name != nil {
		meta.artist = *trackMeta.Artist[0].Name
	}
	if trackMeta.Album != nil {
		if trackMeta.Album.Name != nil {
			meta.album = *trackMeta.Album.Name
		}
		meta.imageUrl = coverImageUrl(trackMeta.Album.Cover, p.app.cfg.ImageSize)
		if meta.imageUrl == "" && trackMeta.Album.CoverGroup != nil {
			meta.imageUrl = coverImageUrl(trackMeta.Album.CoverGroup.Image, p.app.cfg.ImageSize)
		}
	}

	p.app.log.Debugf("observer: spclient metadata for %s: name=%q artist=%q, album=%q",
		spotId.Uri(), meta.name, meta.artist, meta.album)
	return meta
}

func (p *AppPlayer) resolveViaWebApi(ctx context.Context, spotId librespot.SpotifyId) resolvedTrackMeta {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := p.sess.WebApi(reqCtx, "GET", "/v1/tracks/"+spotId.Base62(), nil, nil, nil)
	if err != nil {
		p.app.log.Debugf("observer: web api metadata for %s failed: %v", spotId.Uri(), err)
		return resolvedTrackMeta{}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		p.app.log.Debugf("observer: web api metadata for %s returned status %d", spotId.Uri(), resp.StatusCode)
		return resolvedTrackMeta{}
	}

	var data struct {
		Name    string `json:"name"`
		Artists []struct {
			Name string `json:"name"`
		} `json:"artists"`
		Album struct {
			Name   string `json:"name"`
			Images []struct {
				Url string `json:"url"`
			} `json:"images"`
		} `json:"album"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		p.app.log.Debugf("observer: web api metadata for %s decode failed: %v", spotId.Uri(), err)
		return resolvedTrackMeta{}
	}

	var meta resolvedTrackMeta
	meta.name = data.Name
	if len(data.Artists) > 0 {
		meta.artist = data.Artists[0].Name
	}
	meta.album = data.Album.Name
	if len(data.Album.Images) > 0 {
		meta.imageUrl = data.Album.Images[0].Url
	}

	p.app.log.Debugf("observer: web api metadata for %s: name=%q artist=%q, album=%q",
		spotId.Uri(), meta.name, meta.artist, meta.album)
	return meta
}

const (
	pfAddToLibraryHash         = "7c5a69420e2bfae3da5cc4e14cbc8bb3f6090f80afc00ffc179177f19be3f33d"
	pfApplyCurationsHash       = "05b739a3a73091c213385233b9d3ed8a857c2ca29d2eebadb3d04ed12e288697"
	pfAreEntitiesInLibraryHash = "134337999233cc6fdd6b1e6dbf94841409f04a946c5c7b744b09ba0dfe5a85ed"
)

// returns the current hash for an operation
func (p *AppPlayer) hashOf(op string) string {
	return p.app.hashes.hash(op)
}

// fires re-scrape when a pathfinder call reports a rotated hash
func (p *AppPlayer) onPersistedDrift() {
	if v := p.app.voice; v != nil {
		v.triggerHashRotate()
	}
}

// reports whether a graphQL error signals a rotated hash
func isPersistedQueryErr(e error) bool {
	s := e.Error()
	return strings.Contains(s, "PersistedQueryNotFound") || strings.Contains(s, "PersistedQueryNotSupported")
}

func pathfinderGraphQLError(body []byte) error {
	var r struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &r) == nil && len(r.Errors) > 0 {
		return fmt.Errorf("pathfinder: %s", r.Errors[0].Message)
	}
	return nil
}

func (p *AppPlayer) pathfinderQuery(ctx context.Context, body []byte) ([]byte, error) {
	resp, err := p.sess.PartnerApi(ctx, body)
	if err != nil {
		return nil, fmt.Errorf("pathfinder request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, _ := io.ReadAll(resp.Body)

	// Log the failure body
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet := string(data)
		if len(snippet) > 500 {
			snippet = snippet[:500]
		}
		p.app.log.Warnf("pathfinder: status=%d ct=%q body=%q", resp.StatusCode, resp.Header.Get("Content-Type"), snippet)
	}

	switch resp.StatusCode {
	case 401, 403:
		return nil, ErrForbidden
	case 429:
		return nil, ErrTooManyRequests
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("pathfinder returned status %d", resp.StatusCode)
	}

	if e := pathfinderGraphQLError(data); e != nil {
		if isPersistedQueryErr(e) {
			p.onPersistedDrift()
		}
		return nil, e
	}
	return data, nil
}

type pathfinderError struct {
	Status         int
	RetryAfter     time.Duration
	PersistedQuery bool
	msg            string
}

func (e *pathfinderError) Error() string { return e.msg }

// for the catalog sync
func (p *AppPlayer) pathfinderQueryEx(ctx context.Context, body []byte, force bool) ([]byte, error) {
	resp, err := p.sess.PartnerApiEx(ctx, body, force)
	if err != nil {
		return nil, fmt.Errorf("pathfinder request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, _ := io.ReadAll(resp.Body)
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet := string(data)
		if len(snippet) > 300 {
			snippet = snippet[:300]
		}
		return nil, &pathfinderError{
			Status:     resp.StatusCode,
			RetryAfter: retryAfter,
			msg:        fmt.Sprintf("pathfinder status %d: %s", resp.StatusCode, snippet),
		}
	}

	if msg := pathfinderGraphQLErrorMsg(data); msg != "" {
		pq := strings.Contains(msg, "PersistedQueryNotFound") || strings.Contains(msg, "PersistedQueryNotSupported")
		return nil, &pathfinderError{Status: 200, PersistedQuery: pq, msg: "pathfinder: " + msg}
	}
	return data, nil
}

func pathfinderGraphQLErrorMsg(body []byte) string {
	var r struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &r) == nil && len(r.Errors) > 0 {
		return r.Errors[0].Message
	}
	return ""
}

func parseRetryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

// report whether the URI is in liked songs
func (p *AppPlayer) checkTrackSaved(ctx context.Context, uri string) (bool, error) {
	if !strings.HasPrefix(uri, "spotify:") {
		return false, ErrBadRequest
	}

	body, _ := json.Marshal(map[string]any{
		"operationName": "areEntitiesInLibrary",
		"variables":     map[string]any{"uris": []string{uri}},
		"extensions": map[string]any{
			"persistedQuery": map[string]any{"version": 1, "sha256Hash": p.hashOf("areEntitiesInLibrary")},
		},
	})

	data, err := p.pathfinderQuery(ctx, body)
	if err != nil {
		return false, err
	}

	var r struct {
		Data struct {
			Lookup []struct {
				Saved *bool `json:"saved"`
				Data  struct {
					Saved *bool `json:"saved"`
				} `json:"data"`
			} `json:"lookup"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &r) == nil && len(r.Data.Lookup) > 0 {
		e := r.Data.Lookup[0]
		if e.Data.Saved != nil {
			return *e.Data.Saved, nil
		}
		if e.Saved != nil {
			return *e.Saved, nil
		}
	}
	return false, nil
}

// adds or removes the track or local file from liked songs
func (p *AppPlayer) setTrackSaved(ctx context.Context, uri string, saved bool) error {
	if !strings.HasPrefix(uri, "spotify:") {
		return ErrBadRequest
	}

	var body []byte
	switch {
	case saved && strings.HasPrefix(uri, "spotify:local:"):
		body = p.applyCurationsBody(uri, "CURATE")
	case saved:
		body, _ = json.Marshal(map[string]any{
			"operationName": "addToLibrary",
			"variables":     map[string]any{"libraryItemUris": []string{uri}},
			"extensions": map[string]any{
				"persistedQuery": map[string]any{"version": 1, "sha256Hash": p.hashOf("addToLibrary")},
			},
		})
	default:
		body = p.applyCurationsBody(uri, "UNCURATE")
	}

	if _, err := p.pathfinderQuery(ctx, body); err != nil {
		return err
	}
	p.app.log.Infof("liked songs: %s %s", map[bool]string{true: "added", false: "removed"}[saved], uri)
	return nil
}

func (p *AppPlayer) applyCurationsBody(uri, curationType string) []byte {
	body, _ := json.Marshal(map[string]any{
		"operationName": "applyCurations",
		"variables": map[string]any{
			"input": map[string]any{
				"curations": []any{
					map[string]any{"contextUri": "spotify:collection:tracks", "curationType": curationType},
				},
				"itemUris": []string{uri},
			},
		},
		"extensions": map[string]any{
			"persistedQuery": map[string]any{"version": 1, "sha256Hash": p.hashOf("applyCurations")},
		},
	})
	return body
}

func (p *AppPlayer) handleApiRequest(ctx context.Context, req ApiRequest) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	switch req.Type {
	case ApiRequestTypeRoot:
		return &ApiResponseRoot{PlaybackReady: p.playbackReady()}, nil

	case ApiRequestTypeWebApi:
		data := req.Data.(ApiRequestDataWebApi)
		resp, err := p.sess.WebApi(ctx, data.Method, data.Path, data.Query, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to send web api request: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		switch resp.StatusCode {
		case 400:
			return nil, ErrBadRequest
		case 403:
			return nil, ErrForbidden
		case 404:
			return nil, ErrNotFound
		case 405:
			return nil, ErrMethodNotAllowed
		case 429:
			return nil, ErrTooManyRequests
		}

		if !strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
			respBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read response body: %w", err)
			}
			return respBody, nil
		}

		var respJson any
		if err = json.NewDecoder(resp.Body).Decode(&respJson); err != nil {
			return nil, fmt.Errorf("failed to decode response body: %w", err)
		}
		return respJson, nil

	case ApiRequestTypeStatus:
		resp := &ApiResponseStatus{
			Username:   p.sess.Username(),
			DeviceId:   p.app.deviceId,
			DeviceType: p.app.deviceType.String(),
			DeviceName: p.app.cfg.DeviceName,
			Stopped:    true,
			Paused:     false,
		}
		return resp, nil

	case ApiRequestTypeToken:
		accessToken, err := p.sess.Spclient().GetAccessToken(ctx, true)
		if err != nil {
			return nil, fmt.Errorf("failed getting access token: %w", err)
		}
		return &ApiResponseToken{Token: accessToken}, nil

	case ApiRequestTypeObserverStatus:
		settingUp := false
		var setupProgress *catalogProgress
		if v := p.app.voice; v != nil && v.firstSyncInProgress.Load() {
			settingUp = true
			setupProgress = v.syncProgressSnapshot()
		}
		if p.state.remoteState == nil {
			message := "no remote device is currently playing"
			if settingUp {
				message = "setting things up"
			}
			resp := map[string]any{
				"active":            false,
				"message":           message,
				"setting_up":        settingUp,
				"devices":           p.connectDevicesOrEmpty(),
				"utc_offset_min":    p.app.utcOffsetMin(),
				"latest_version":    p.app.latestVersion(),
				"latest_highlights": p.app.latestHighlights(),
				"update_available":  p.app.updateAvailable(),
				"update_mandatory":  p.app.updateMandatory(),
			}
			if setupProgress != nil {
				resp["setting_up_progress"] = setupProgress
			}
			return resp, nil
		}

		rs := p.state.remoteState
		trackId := ""
		if parts := strings.SplitN(rs.TrackUri, ":", 3); len(parts) == 3 {
			trackId = parts[2]
		}
		lyricsUrl := ""
		if trackId != "" {
			lyricsUrl = fmt.Sprintf("/lyrics/%s", trackId)
		}

		resp := map[string]any{
			"active":            true,
			"device_id":         rs.DeviceId,
			"device_name":       rs.DeviceName,
			"device_type":       rs.DeviceType,
			"track_id":          trackId,
			"track_uri":         rs.TrackUri,
			"track_name":        rs.TrackName,
			"track_artist":      rs.TrackArtist,
			"track_album":       rs.TrackAlbum,
			"track_image":       rs.TrackImageUrl,
			"context_uri":       rs.ContextUri,
			"context_name":      rs.ContextName,
			"duration":          rs.Duration,
			"position":          rs.RemotePosition(),
			"is_playing":        rs.IsPlaying,
			"is_paused":         rs.IsPaused,
			"volume":            rs.Volume,
			"volume_max":        MaxStateVolume,
			"volume_disabled":   rs.VolumeDisabled,
			"volume_steps":      rs.VolumeSteps,
			"shuffle":           rs.ShuffleContext,
			"repeat_context":    rs.RepeatContext,
			"repeat_track":      rs.RepeatTrack,
			"disallow_prev":     rs.DisallowSkipPrev,
			"disallow_next":     rs.DisallowSkipNext,
			"disallow_seek":     rs.DisallowSeek,
			"prev_tracks":       rs.PrevTracks,
			"next_tracks":       rs.NextTracks,
			"lyrics_url":        lyricsUrl,
			"raw_metadata":      rs.RawMetadata,
			"setting_up":        settingUp,
			"devices":           p.connectDevicesOrEmpty(),
			"utc_offset_min":    p.app.utcOffsetMin(),
			"latest_version":    p.app.latestVersion(),
			"latest_highlights": p.app.latestHighlights(),
			"update_available":  p.app.updateAvailable(),
			"update_mandatory":  p.app.updateMandatory(),
		}
		if setupProgress != nil {
			resp["setting_up_progress"] = setupProgress
		}
		return resp, nil

	case ApiRequestTypeConnectDevices:
		return map[string]any{"devices": p.connectDevicesOrEmpty()}, nil

	case ApiRequestTypeTransfer:
		data, _ := req.Data.(ApiRequestDataTransfer)
		return nil, p.sendTransfer(ctx, data.DeviceId)

	case ApiRequestTypeResume:
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "resume"})
	case ApiRequestTypeResumeLast:
		return nil, p.resumeLastDevice(ctx)
	case ApiRequestTypePause:
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "pause"})
	case ApiRequestTypePlayPause:
		// pick the endpoint from the last known playback state
		endpoint := "resume"
		if rs := p.state.remoteState; rs != nil && rs.IsPlaying && !rs.IsPaused {
			endpoint = "pause"
		}
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: endpoint})
	case ApiRequestTypeNext:
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "skip_next"})
	case ApiRequestTypePrev:
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "skip_prev"})
	case ApiRequestTypeSeek:
		data, _ := req.Data.(ApiRequestDataSeek)
		if data.Relative {
			return nil, fmt.Errorf("relative seek not supported in observer mode")
		}
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "seek_to", Value: data.Position})
	case ApiRequestTypeSetShufflingContext:
		val, _ := req.Data.(bool)
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "set_shuffling_context", Value: val})
	case ApiRequestTypeSetRepeatingContext:
		val, _ := req.Data.(bool)
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "set_repeating_context", Value: val})
	case ApiRequestTypeSetRepeatingTrack:
		val, _ := req.Data.(bool)
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "set_repeating_track", Value: val})
	case ApiRequestTypeDjSignal:
		// the receiving Spotify client acts on this and asks the backend for a new DJ set
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{Endpoint: "signal", SignalId: "jump"})

	case ApiRequestTypeGetVolume:
		rs := p.state.remoteState
		if rs == nil || rs.DeviceId == "" {
			return nil, fmt.Errorf("no active remote device known yet")
		}
		return &ApiResponseVolume{Value: rs.Volume, Max: MaxStateVolume}, nil

	case ApiRequestTypeSetVolume:
		data, _ := req.Data.(ApiRequestDataVolume)
		rs := p.state.remoteState
		if rs == nil || rs.DeviceId == "" {
			return nil, fmt.Errorf("no active remote device known yet")
		}
		if rs.VolumeDisabled {
			// route volume controls to phone directly
			if data.Relative && rs.DeviceType == "SMARTPHONE" && p.app.bt != nil && p.app.bt.SendPhoneVolumeSteps(int(data.Volume)) {
				p.app.log.Debugf("set_volume: routed %+d phone-volume step(s) (%s)", data.Volume, rs.DeviceName)
				return nil, nil
			}
			return nil, fmt.Errorf("active device does not allow volume control")
		}
		target := int64(data.Volume)
		if data.Relative {
			target = int64(rs.Volume) + int64(data.Volume)
		}
		if target < 0 {
			target = 0
		}
		if target > MaxStateVolume {
			target = MaxStateVolume
		}
		// TEMP diagnostic logging while calibrating the volume knob.
		p.app.log.Infof("set_volume: req={vol:%d rel:%v} deviceVol:%d -> target:%d (%s)",
			data.Volume, data.Relative, rs.Volume, target, rs.DeviceName)
		return nil, p.sendActiveDeviceVolume(ctx, target)

	case ApiRequestTypePlay:
		// tell the active device to start a context
		data, _ := req.Data.(ApiRequestDataPlay)
		if data.Uri == "" {
			return nil, fmt.Errorf("play requires a context uri")
		}
		cmd := connectCommand{
			Endpoint: "play",
			Context: &connectContext{
				Uri: data.Uri,
				Url: "context://" + data.Uri,
			},
			Options: &connectOptions{License: "tft"},
			PlayOrigin: &connectOrigin{
				FeatureIdentifier:  "your_library",
				FeatureVersion:     "go-librespot",
				ReferrerIdentifier: "your_library",
			},
			LoggingParams: &connectLogging{
				PageInstanceIds: []string{},
				InteractionIds:  []string{},
				CommandId:       randomCommandId(),
			},
		}
		if data.SkipToUri != "" {
			cmd.Options.SkipTo = connectSkipTo{TrackUri: data.SkipToUri}
		}
		shuf := "inherit"
		if data.Shuffle != nil {
			cmd.Options.PlayerOptionsOverride.ShufflingContext = data.Shuffle
			shuf = fmt.Sprintf("%v", *data.Shuffle)
		}
		// TEMP diagnostic logging while verifying the play envelope on hardware.
		p.app.log.Infof("play: context=%s skipTo=%q shuffle=%s -> active device", data.Uri, data.SkipToUri, shuf)
		return nil, p.sendActiveDeviceCommand(ctx, cmd)

	case ApiRequestTypeSearch:
		data, _ := req.Data.(ApiRequestDataSearch)
		if data.TopN {
			return p.searchTracks(ctx, data.Query)
		}
		return p.searchTrack(ctx, data.Query)

	case ApiRequestTypeCatalogPage:
		data, _ := req.Data.(ApiRequestDataCatalogPage)
		return p.catalogPage(ctx, data)

	case ApiRequestTypeAddToQueue:
		uri, _ := req.Data.(string)
		if uri == "" {
			return nil, fmt.Errorf("add_to_queue requires a uri")
		}
		return nil, p.sendActiveDeviceCommand(ctx, connectCommand{
			Endpoint: "add_to_queue",
			Track: &connectQueueTrack{
				Uri:      uri,
				Provider: "queue",
				Metadata: map[string]string{"is_queued": "true"},
			},
		})

	case ApiRequestTypeGetSaved:
		data, _ := req.Data.(ApiRequestDataSaved)
		saved, err := p.checkTrackSaved(ctx, data.Uri)
		if err != nil {
			return nil, err
		}
		return map[string]any{"saved": saved}, nil

	case ApiRequestTypeSetSaved:
		data, _ := req.Data.(ApiRequestDataSaved)
		if err := p.setTrackSaved(ctx, data.Uri, data.Saved); err != nil {
			return nil, err
		}
		return map[string]any{"saved": data.Saved}, nil

	default:
		return nil, fmt.Errorf("unknown request type: %s", req.Type)
	}
}

// connectCommand is the JSON shape of a single Spotify Connect remote-control command
type connectCommand struct {
	Endpoint      string             `json:"endpoint"`
	SignalId      string             `json:"signal_id,omitempty"`
	Value         any                `json:"value,omitempty"`
	Context       *connectContext    `json:"context,omitempty"`
	Options       *connectOptions    `json:"options,omitempty"`
	PlayOrigin    *connectOrigin     `json:"play_origin,omitempty"`
	LoggingParams *connectLogging    `json:"logging_params,omitempty"`
	Track         *connectQueueTrack `json:"track,omitempty"`
}

type connectQueueTrack struct {
	Uri      string            `json:"uri"`
	Provider string            `json:"provider,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// connectContext/connectOptions/connectOrigin/connectLogging are the play-command sub-objects
type connectContext struct {
	Uri      string   `json:"uri"`
	Url      string   `json:"url,omitempty"`
	Metadata struct{} `json:"metadata"`
}

type connectOptions struct {
	License               string                       `json:"license,omitempty"`
	SkipTo                connectSkipTo                `json:"skip_to"`
	PlayerOptionsOverride connectPlayerOptionsOverride `json:"player_options_override"`
}

type connectPlayerOptionsOverride struct {
	ShufflingContext *bool `json:"shuffling_context,omitempty"`
}

type connectSkipTo struct {
	TrackUri string `json:"track_uri,omitempty"`
}

type connectOrigin struct {
	FeatureIdentifier  string `json:"feature_identifier"`
	FeatureVersion     string `json:"feature_version,omitempty"`
	ReferrerIdentifier string `json:"referrer_identifier,omitempty"`
}

type connectLogging struct {
	PageInstanceIds []string `json:"page_instance_ids"`
	InteractionIds  []string `json:"interaction_ids"`
	CommandId       string   `json:"command_id"`
}

// randomCommandId returns a 32-char hex id
func randomCommandId() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b)
}

type connectCommandEnvelope struct {
	Command connectCommand `json:"command"`
}

// sendActiveDeviceCommand sends to the active device in the user's cluster
func (p *AppPlayer) sendActiveDeviceCommand(ctx context.Context, cmd connectCommand) error {
	rs := p.state.remoteState
	if rs == nil || rs.DeviceId == "" {
		return fmt.Errorf("no active remote device known yet")
	}
	// always target the current active device
	return p.sendDeviceCommand(ctx, rs.DeviceId, rs.DeviceName, cmd)
}

// sendDeviceCommand sends a connect player command to an explicit device id.
func (p *AppPlayer) sendDeviceCommand(ctx context.Context, deviceId, deviceName string, cmd connectCommand) error {
	if deviceId == "" {
		return fmt.Errorf("no target device")
	}
	if deviceId == p.app.deviceId {
		return fmt.Errorf("target device is us; cannot remote-control self")
	}
	if !p.hasSpotConnId {
		return fmt.Errorf("dealer not connected (no spotify-connection-id)")
	}

	body, err := json.Marshal(connectCommandEnvelope{Command: cmd})
	if err != nil {
		return fmt.Errorf("marshal connect command: %w", err)
	}

	if err := p.sess.Spclient().SendPlayerCommand(ctx, p.app.deviceId, deviceId, p.spotConnId, body); err != nil {
		return fmt.Errorf("send %s to %s: %w", cmd.Endpoint, deviceId, err)
	}
	p.app.log.Debugf("observer: sent %s to %s (%s)", cmd.Endpoint, deviceId, deviceName)
	return nil
}

// resumeLastDevice resumes playback on the most-recent active device
func (p *AppPlayer) resumeLastDevice(ctx context.Context) error {
	targetId := p.state.lastActiveDeviceId
	if targetId == "" {
		return ErrNotFound
	}

	present := false
	for _, d := range p.state.connectDevices {
		if d.Id == targetId {
			present = true
			break
		}
	}
	if !present {
		return ErrNotFound
	}

	return p.sendDeviceCommand(ctx, targetId, p.state.lastActiveDeviceName, connectCommand{Endpoint: "resume"})
}

// sendActiveDeviceVolume sets the active device's volume using a connect-state endpoint
func (p *AppPlayer) sendActiveDeviceVolume(ctx context.Context, volume int64) error {
	rs := p.state.remoteState
	if rs == nil || rs.DeviceId == "" {
		return fmt.Errorf("no active remote device known yet")
	}
	if rs.DeviceId == p.app.deviceId {
		return fmt.Errorf("active device is us; cannot remote-control self")
	}
	if !p.hasSpotConnId {
		return fmt.Errorf("dealer not connected (no spotify-connection-id)")
	}

	body, err := json.Marshal(map[string]int64{"volume": volume})
	if err != nil {
		return fmt.Errorf("marshal volume: %w", err)
	}

	if err := p.sess.Spclient().SetConnectVolume(ctx, p.app.deviceId, rs.DeviceId, p.spotConnId, body); err != nil {
		return fmt.Errorf("set volume on %s: %w", rs.DeviceId, err)
	}
	p.app.log.Debugf("observer: set volume %d on %s (%s)", volume, rs.DeviceId, rs.DeviceName)
	return nil
}

type transferOptions struct {
	RestorePaused string `json:"restore_paused"`
}

type transferBody struct {
	TransferOptions transferOptions `json:"transfer_options"`
	InteractionId   string          `json:"interaction_id"`
	CommandId       string          `json:"command_id"`
}

// moves the current playback session to targetDeviceId
func (p *AppPlayer) sendTransfer(ctx context.Context, targetDeviceId string) error {
	if targetDeviceId == "" {
		return fmt.Errorf("transfer requires a target device id")
	}
	if targetDeviceId == p.app.deviceId {
		return fmt.Errorf("cannot transfer to ourselves (observer cannot play)")
	}
	if !p.hasSpotConnId {
		return fmt.Errorf("dealer not connected (no spotify-connection-id)")
	}

	body, err := json.Marshal(transferBody{
		TransferOptions: transferOptions{RestorePaused: "restore"},
		InteractionId:   randomCommandId(),
		CommandId:       randomCommandId(),
	})
	if err != nil {
		return fmt.Errorf("marshal transfer: %w", err)
	}

	if err := p.sess.Spclient().TransferConnect(ctx, p.app.deviceId, targetDeviceId, p.spotConnId, body); err != nil {
		return fmt.Errorf("transfer to %s: %w", targetDeviceId, err)
	}
	p.app.log.Debugf("observer: transferred playback to %s", targetDeviceId)
	return nil
}

func convertSpotifyImageUrl(s string) string {
	if strings.HasPrefix(s, "spotify:image:") {
		return "https://i.scdn.co/image/" + strings.TrimPrefix(s, "spotify:image:")
	}
	if strings.HasPrefix(s, "https://") || strings.HasPrefix(s, "http://") {
		return s
	}
	return "https://" + s
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func (p *AppPlayer) Close() {
	select {
	case p.stop <- struct{}{}:
	default:
	}
	p.sess.Close()
}

// handleLyricsAsync resolves the track metadata synchronously
// keeps slow lyrics HTTP off the player loop so it never stalls the dealer keepalive/messages
func (p *AppPlayer) handleLyricsAsync(ctx context.Context, req ApiRequest) {
	data := req.Data.(ApiRequestDataLyrics)

	// fetch the synced transcript by episode id
	if data.Episode {
		lp := p.lyricsProvider
		go func() {
			result, err := lp.FetchEpisodeText(ctx, data.TrackId)
			if err != nil {
				if errors.Is(err, ErrNoLyrics) {
					req.Reply(nil, ErrNotFound)
					return
				}
				req.Reply(nil, fmt.Errorf("episode text fetch failed: %w", err))
				return
			}
			req.Reply(result, nil)
		}()
		return
	}

	trackName := data.TrackName
	artistName := data.ArtistName
	albumName := data.AlbumName
	durationMs := data.DurationMs

	if trackName == "" || artistName == "" {
		if rs := p.state.remoteState; rs != nil {
			if trackName == "" {
				trackName = rs.TrackName
			}
			if artistName == "" {
				artistName = rs.TrackArtist
			}
			if albumName == "" {
				albumName = rs.TrackAlbum
			}
			if durationMs == 0 {
				durationMs = int(rs.Duration)
			}
		}
	}

	if trackName == "" {
		req.Reply(nil, fmt.Errorf("track name required (provide ?track= param or wait for observer state)"))
		return
	}

	lp := p.lyricsProvider
	go func() {
		result, err := lp.FetchLyrics(ctx, data.TrackId, trackName, artistName, albumName, durationMs, data.Richsync)
		if err != nil {
			if errors.Is(err, ErrNoLyrics) {
				req.Reply(nil, ErrNotFound)
				return
			}
			req.Reply(nil, fmt.Errorf("lyrics fetch failed: %w", err))
			return
		}
		req.Reply(result, nil)
	}()
}

func (p *AppPlayer) Run(ctx context.Context, apiRecv <-chan ApiRequest) {
	// Signal the API server that we are now consuming the request channel
	p.app.server.SetPlayerReady(true)
	defer p.app.server.SetPlayerReady(false)

	// expose ourselves to app level actions
	p.app.currentPlayer.Store(p)
	defer p.app.currentPlayer.CompareAndSwap(p, nil)

	p.lyricsProvider = NewLyricsProvider(p.app.log, func(ctx context.Context, force bool) (string, error) {
		return p.sess.Spclient().GetAccessToken(ctx, force)
	})
	p.app.log.Infof("lyrics provider initialized")

	p.queueResolver = newQueueResolver(p.app.log, p.sess.Spclient(), p.queueResolvedCh)
	p.metaResolvedCh = make(chan resolvedTrackMeta, 4)

	apRecv := p.sess.Accesspoint().Receive(ap.PacketTypeProductInfo, ap.PacketTypeCountryCode)
	msgRecv := p.sess.Dealer().ReceiveMessage("hm://pusher/v1/connections/", "hm://connect-state/v1/")
	reqRecv := p.sess.Dealer().ReceiveRequest("hm://connect-state/v1/player/command")

	heartbeat := time.NewTicker(connectStateHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			p.sess.Close()
			return
		case <-p.stop:
			return
		case <-heartbeat.C:
			p.heartbeatConnectState()
		case pkt, ok := <-apRecv:
			if !ok {
				p.app.log.Warnf("accesspoint receiver closed")
				apRecv = nil
				continue
			}
			if err := p.handleAccesspointPacket(pkt.Type, pkt.Payload); err != nil {
				p.app.log.Warnf("failed handling accesspoint packet: %v", err)
			}
		case msg, ok := <-msgRecv:
			if !ok {
				p.app.log.Warnf("dealer message receiver closed")
				msgRecv = nil
				continue
			}
			if err := p.handleDealerMessage(ctx, msg); err != nil {
				p.app.log.Warnf("failed handling dealer message: %v", err)
			}
		case cluster := <-p.clusterCh:
			// cluster snapshot from a put_state response
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			p.handleCluster(cctx, cluster)
			cancel()
		case req, ok := <-reqRecv:
			if !ok {
				p.app.log.Warnf("dealer request receiver closed")
				reqRecv = nil
				continue
			}
			if err := p.handleDealerRequest(ctx, req); err != nil {
				p.app.log.Warnf("failed handling dealer request: %v", err)
				req.Reply(false)
			} else {
				req.Reply(true)
			}
		case req, ok := <-apiRecv:
			if !ok {
				apiRecv = nil
				continue
			}
			// fetching lyrics sometimes block the HTTP connection between the ui
			// for seconds at a time in cases where fetching lyrics take too long (no lyrics for several songs in succession)
			// having it inlione stalls this loop that keeps the dealer keepalive/messages
			if req.Type == ApiRequestTypeLyrics {
				p.handleLyricsAsync(ctx, req)
				continue
			}
			data, err := p.handleApiRequest(ctx, req)
			req.Reply(data, err)
		case meta := <-p.metaResolvedCh:
			// background current track metadata
			if p.metaResolveInFlight == meta.uri {
				p.metaResolveInFlight = ""
			}
			if meta.artist == "" && meta.name == "" && meta.imageUrl == "" {
				p.metaResolveFailedUri = meta.uri
			}
			if rs := p.state.remoteState; rs != nil && rs.TrackUri == meta.uri {
				updated := *rs
				changed := false
				if meta.artist != "" && updated.TrackArtist == "" {
					updated.TrackArtist = meta.artist
					changed = true
				}
				if updated.TrackAlbum == "" {
					if alb := firstNonEmpty(meta.album, updated.RawMetadata["album_title"]); alb != "" {
						updated.TrackAlbum = alb
						changed = true
					}
				}
				if meta.name != "" && updated.TrackName == "" {
					updated.TrackName = meta.name
					changed = true
				}
				if meta.imageUrl != "" && updated.TrackImageUrl == "" {
					updated.TrackImageUrl = meta.imageUrl
					changed = true
				}
				if changed {
					updated.Position = updated.RemotePosition()
					p.state.remoteState = &updated
					p.app.server.Emit(&ApiEvent{
						Type: ApiEventTypeObserverStateChanged,
						Data: &updated,
					})
				}
			}
		case <-p.queueResolvedCh:
			// Background queue-metadata resolution landed
			if rs := p.state.remoteState; rs != nil && p.queueResolver != nil {
				updated := *rs
				updated.NextTracks = append([]QueueTrack(nil), rs.NextTracks...)
				updated.PrevTracks = append([]QueueTrack(nil), rs.PrevTracks...)
				p.queueResolver.applyCache(updated.NextTracks)
				p.queueResolver.applyCache(updated.PrevTracks)
				updated.Position = updated.RemotePosition()
				p.state.remoteState = &updated
				p.app.server.Emit(&ApiEvent{
					Type: ApiEventTypeObserverStateChanged,
					Data: &updated,
				})
			}
		}
	}
}
