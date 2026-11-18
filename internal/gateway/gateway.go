// Package gateway
package gateway

import (
	"fmt"
	"net/http/httputil"
	"net/netip"
)


func NewGateway(config *Config) (*Gateway, error) {
	var gateway Gateway

	addr, err := netip.ParseAddrPort(config.Gateway.ListenAddr)

	if err != nil {
		return nil, fmt.Errorf("error instantiating server gateway/n %w", err )
	}

	gateway.listener = addr

	return &gateway, nil
}

type Gateway struct {
	listener  netip.AddrPort
	BackendPool  []Service
	proxy httputil.ReverseProxy
}
