package controlplane

import "net"

type Subscriber struct {
	ID   SubscriberID
	Conn net.Conn
}
