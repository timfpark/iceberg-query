package controllers

import (
	"log"
	"net/http"

	"github.com/timfpark/iceberg-query/services"
	goavro "gopkg.in/linkedin/goavro.v2"
)

var queryService services.QueryService
var codec *goavro.Codec

func InitQueryController() (err error) {
	codec, err = goavro.NewCodec(`{
		"type": "record",
		"name": "Location",
		"fields": [
			{ "name": "accuracy", "type": ["null", "double"], "default": null },
			{ "name": "altitude", "type": ["null", "double"], "default": null },
			{ "name": "altitudeAccuracy", "type": ["null", "double"], "default": null },
			{ "name": "course", "type": ["null", "double"], "default": null },
			{
				"name": "features",
				"type": {
					"type": "array",
					"items": { "name": "id", "type": "string" }
				}
			},
			{ "name": "latitude", "type": "double" },
			{ "name": "longitude", "type": "double" },
			{ "name": "speed", "type": ["null", "double"], "default": null },
			{ "name": "source", "type": "string", "default": "device" },
			{ "name": "timestamp", "type": "long" },
			{ "name": "user_id", "type": "string" }
		]
	}`)

	if err != nil {
		log.Printf("codec initialization failed with %s", err)
		return err
	}

	queryService = services.QueryService{
		Codec: codec,
	}

	return queryService.Init()
}

func QueryHandler(w http.ResponseWriter, r *http.Request) {
	partitionParams, ok := r.URL.Query()["p"]
	if !ok || len(partitionParams) != 1 {
		errorText := "url param 'p' (partition) is missing or specified multiple times"
		log.Println(errorText)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	partition := partitionParams[0]
	log.Printf("partition: %s\n", partition)

	startKeyParams, ok := r.URL.Query()["sk"]
	if !ok || len(partitionParams) != 1 {
		errorText := "url param 'sk' (start key) is missing or specified multiple times"
		log.Println(errorText)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	startKey := startKeyParams[0]
	log.Printf("startKey: %s\n", startKey)

	endKeyParams, ok := r.URL.Query()["ek"]
	if !ok || len(partitionParams) != 1 {
		errorText := "url param 'ek' (end key) is missing or specified multiple times"
		log.Println(errorText)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	endKey := endKeyParams[0]
	log.Printf("endKey: %s\n", endKey)

	w.Header().Set("Content-Type", "avro/binary")

	queryResults, err := queryService.Query(partition, startKey, endKey)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Printf("queryResults length: %d", len(queryResults))

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               w,
		CompressionName: "snappy",
		Schema:          codec.Schema(),
	})

	ocfWriter.Append(queryResults)
}
