package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	goavro "gopkg.in/linkedin/goavro.v2"

	"github.com/timfpark/iceberg-query/controllers"
)

var codec *goavro.Codec

func StartServer() {
	if len(os.Getenv("PORT")) == 0 {
		log.Printf("env var PORT not supplied - exiting.")
		return
	}

	if err := controllers.InitQueryController(); err != nil {
		log.Printf("InitQueryController failed with %s\n", err)
	}

	r := mux.NewRouter()

	r.HandleFunc("/q", controllers.QueryHandler).Methods("GET")

	portString := fmt.Sprintf(":%s", os.Getenv("PORT"))
	if err := http.ListenAndServe(portString, r); err != nil {
		log.Printf("ListenAndServe returned error: %s.", err)
		return
	}
}

func main() {
	StartServer()
}
