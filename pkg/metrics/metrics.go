package metrics

import (
	"net"
	"net/http"
	"strings"

	met "github.com/armon/go-metrics"
	promet "github.com/armon/go-metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"sassoftware.io/convoy/arke/pkg/provider"

	"sassoftware.io/convoy/arke/pkg/util"
)

type stats struct {
	met.Metrics
	Sink         *promet.PrometheusSink
	isPrometheus bool
}

// LabelSet go-metrics Label. Avoid importing go-metrics anywhere but here
type LabelSet struct {
	Labels []met.Label
}

// Stats global Stats variable for access to the sinks
var Stats *stats

func init() {
	Stats = &stats{}

	Stats.Sink, _ = promet.NewPrometheusSink()

	met.NewGlobal(met.DefaultConfig(""), Stats.Sink)

}

// Serve Create a new HTTP server and Serve metrics requests
func Serve(lis *net.Listener) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", gatherClientStatsHandler())
	metricsServer := &http.Server{
		Handler: mux,
	}

	if err := metricsServer.Serve(*lis); err != nil {
		util.Logger.FatalI("error.metricsserve", err)
	}
}

func gatherClientStatsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatherClientStats()
		promhttp.Handler().ServeHTTP(w, r)
	})
}

// FIXME?
// I don't particularly like only collecting these client level stats when the metrics handler
// is called because it can and will lead to us never logging stats for some clients.
// We can even have incorrect statistics if part way through a client producing/consuming the
// metrics are collected, but they disconnect before the next metrics collection.
// I also don't like the idea of keeping a separate record of stats from every for an extended
// period of time even if they have disconnected.
// Maybe client level stats don't matter much as long as they're regarded as an incomplete look
// into the current state of only connected clients.
func gatherClientStats() {
	providers := provider.RegisteredProviders()
	for _, providerName := range provider.RegisteredProviders().GetList() {
		provRaw, exists := providers.Get(providerName)
		if !exists {
			continue
		}
		prov := provRaw.(provider.Provider)
		pstats := prov.Stats()
		for _, client := range pstats.Clients {
			clientID := strings.ReplaceAll(client.ID, ".", "-")
			labelset := NewLabelSet()
			labelset.AddLabel("ClientUUID", clientID)
			Stats.Sink.SetGaugeWithLabels([]string{"client", "ActiveMessages", "total"}, float32(client.ActiveMessages), labelset.Labels)
			Stats.Sink.SetGaugeWithLabels([]string{"client", "Streams", "total"}, float32(client.Streams), labelset.Labels)
			Stats.Sink.SetGaugeWithLabels([]string{"client", "Consumed", "total"}, float32(client.Consumed), labelset.Labels)
			Stats.Sink.SetGaugeWithLabels([]string{"client", "Produced", "total"}, float32(client.Produced), labelset.Labels)
		}
	}
}

// NewLabelSet Create a new label set
func NewLabelSet() *LabelSet {
	metl := &LabelSet{}
	metl.Labels = make([]met.Label, 0)
	return metl
}

// AddLabel Add a label to your label set
func (metl *LabelSet) AddLabel(name string, value string) {
	label := met.Label{Name: name, Value: value}
	metl.Labels = append(metl.Labels, label)
}
