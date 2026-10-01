package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func main() {
	target, err := url.Parse("https://localhost:8000")

	if err != nil {
		log.Println(err)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	log.Println("Starting proxy server :8001")
	log.Fatal(http.ListenAndServe(":8001", proxy))
}
