package azure

import (
	"time"

	"sassoftware.io/viya/arke/pkg/provider"
	"sassoftware.io/viya/arke/pkg/util"
)

// every 30 seconds check the list of active connections
// if a client has 0 active streams and hasn't created or
// deleted a stream in over 30 seconds, disconnect it.
// Severed client connections may hang around for up to 60
// seconds since we are checking every 30.
func connectionCleaner() {
	provy, _ := provider.GetProvider("azure")
	prov := provy.(*azureprovider)
	ticker := time.NewTicker(30 * time.Second)
	for {
		<-ticker.C
		for _, connID := range prov.connections.GetList() {
			if conn, ok := prov.connections.Get(connID); ok {
				bd := conn.(*BrokerDetails)
				util.Logger.Debugf("Client %v has %d open streams", connID, bd.ActiveStreams)
				lastKnown := time.Since(bd.lastPubSubEvent)
				if bd.ActiveStreams < 1 && lastKnown > 30*time.Second {
					util.Logger.Debugf("Client %v has had no streams open for %v. Assuming dead. Disconnecting.", connID, lastKnown)
					prov.disconnectClientByIdentifier(connID)
				}
			}
		}
	}
}
