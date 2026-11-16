package daemon

import (
	"net"
	"time"

	"github.com/vishvananda/netlink"

	librespot "github.com/devgianlu/go-librespot"
)

// linkUp reports whether a network interface exists and is up
func linkUp(name string) bool {
	link, err := netlink.LinkByName(name)
	if err != nil {
		return false
	}
	attrs := link.Attrs()
	return attrs.Flags&net.FlagUp != 0 && attrs.OperState != netlink.OperDown
}

// hasDefaultRoute reports whether an IPv4 default route exists
func hasDefaultRoute() bool {
	routes, err := netlink.RouteList(nil, netlink.FAMILY_V4)
	if err != nil {
		return false
	}
	for _, r := range routes {
		if r.Dst == nil && r.Gw != nil {
			return true
		}
	}
	return false
}

// isOnlinePassive performs a passive local check of network availability
// without generating outbound ICMP or packet traffic over the USB NDIS bridge.
func isOnlinePassive() bool {
	// Check if any primary network interface (USB RNDIS/ECM or Bluetooth PAN) is UP
	if !linkUp("usb0") && !linkUp("usb1") && !linkUp("bnep0") {
		return false
	}
	// Interface is UP; confirm routing or carrier readiness
	return hasDefaultRoute() || linkUp("usb0") || linkUp("bnep0")
}

// startNetworkMonitor monitors network availability passively and emits network_status on transition.
// Using passive local interface and routing checks eliminates the 1-second ICMP ping loop,
// completely avoiding winnat.sys DPC latency spikes on the Windows host.
func startNetworkMonitor(log librespot.Logger, server ApiServer, onTransition func(online bool)) {
	const (
		interval       = 2 * time.Second
		failThreshold  = 3
		rebroadcastSec = 15
	)

	go func() {
		var (
			online        bool
			failCount     int
			emitted       bool
			tickSinceEmit int
		)

		emit := func(status string) {
			server.Emit(&ApiEvent{
				Type: ApiEventTypeNetworkStatus,
				Data: map[string]any{
					"status": status,
					"usb":    linkUp("usb0"),
					"bt":     linkUp("bnep0"),
				},
			})
			tickSinceEmit = 0
		}

		for {
			ok := isOnlinePassive()
			if ok {
				failCount = 0
				if !online || !emitted {
					online = true
					emitted = true
					log.Info("network: online (passive local check)")
					emit("online")
					if onTransition != nil {
						onTransition(true)
					}
				}
			} else {
				failCount++
				if failCount >= failThreshold && (online || !emitted) {
					online = false
					emitted = true
					log.Warn("network: offline (all interfaces down)")
					emit("offline")
					if onTransition != nil {
						onTransition(false)
					}
				}
			}

			// re-broadcast for late WS clients, only once we've determined a state
			if emitted {
				tickSinceEmit++
				if tickSinceEmit >= rebroadcastSec {
					if online {
						emit("online")
					} else {
						emit("offline")
					}
				}
			}

			time.Sleep(interval)
		}
	}()
}
