package main

import (
	"context"
	"flag"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sassoftware.io/viya/arke/internal/util"
	"sassoftware.io/viya/arke/pkg/arke"

	"sassoftware.io/viya/arke/i18n"
	_ "sassoftware.io/viya/arke/internal/provider/connectors"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var tlsSkipVerify = flag.Bool("tls-skip-verify", false, "Force TLS, but always skip verification")

func run(ctx context.Context) error {

	// Set up cpu and memory profiling if passed in as args
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(filepath.Clean(*cpuprofile))
		if err != nil {
			util.Logger.FatalI(i18n.CPUProfileError, err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			util.Logger.FatalI(i18n.CPUProfileError, err)
		}
		defer pprof.StopCPUProfile()
	}

	defer func() {
		if *memprofile != "" {
			f, err := os.Create(filepath.Clean(*memprofile))
			if err != nil {
				util.Logger.FatalI(i18n.MemProfileError, err)
			}
			defer f.Close()
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				util.Logger.FatalI(i18n.MemProfileError, err)
			}
		}
	}()

	certFile := os.Getenv("CERT_FILE")
	certKey := os.Getenv("CERT_KEY")

	svr := arke.DefaultArkeServer().WithTLSSkipVerify(*tlsSkipVerify).WithCertFilePath(certFile).WithCertKeyPath(certKey)
	err := svr.Serve(ctx)
	if err != nil {
		switch err.(type) { //nolint gocritic
		case *net.OpError:
			return nil
		default:
			util.Logger.FatalI(i18n.GenericError, err)
		}
	}
	return err
}

func main() {
	ctx := context.Background()
	run(ctx) //nolint:errcheck
}
