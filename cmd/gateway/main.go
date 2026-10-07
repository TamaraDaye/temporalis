package main

import (
	// "fmt"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	// "net/http/httputil"
	// "net/url"
	"sync"

	"github.com/TamaraDaye/temporalis/internal/controlplane"
)

func main() {
	// target, err := url.Parse("https://localhost:8000")

	// if err != nil {
	// 	log.Println(err)
	// }

	// proxy := httputil.NewSingleHostReverseProxy(target)
	var ( 
		application = flag.String("application", "dummy" , "viewserver instance for application")
		port = flag.Int("port", 8001, "registration server port")
	)

	flag.Parse()
	viewServer := NewViewServer(*application)
	mux := controlplane.RunRegistrationServer(viewServer)
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting proxy server :%s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func NewViewServer(application string)  *controlplane.ViewServer {

	services := make(map[controlplane.ServiceID]controlplane.Instances)

	view := controlplane.View{Version: 0, Services: services}

	lastSeen := make(map[controlplane.InstanceID]time.Time)

	subscribers := make(map[controlplane.SubscriberID]*controlplane.Subscriber)

	viewServer := controlplane.ViewServer{Application: application, Mu: sync.RWMutex{}, View: view, LastSeen: lastSeen, Subscribers: subscribers}

	return &viewServer
}
