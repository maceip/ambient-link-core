// Package discovery advertises the ambient-link host on the LAN via mDNS.
package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"

	"github.com/hashicorp/mdns"
)

const ServiceType = "_ambientlink._tcp"

// Advertise publishes _ambientlink._tcp on the LAN until ctx is cancelled.
func Advertise(ctx context.Context, listenAddr, token string, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	_, portStr, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return fmt.Errorf("mdns: parse listen: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("mdns: bad port: %w", err)
	}
	name, _ := os.Hostname()
	if name == "" {
		name = "ambient-link"
	}
	txt := []string{"v=1", "path=/ambient-link/ws"}
	if token != "" {
		txt = append(txt, "token="+token)
	}
	srv, err := mdns.NewMDNSService(name, ServiceType, "", "", port, nil, txt)
	if err != nil {
		return err
	}
	server, err := mdns.NewServer(&mdns.Config{Zone: srv})
	if err != nil {
		return err
	}
	log.Info("mdns: advertising", "service", ServiceType, "port", port, "name", name)
	go func() {
		<-ctx.Done()
		server.Shutdown()
		log.Info("mdns: stopped")
	}()
	return nil
}
