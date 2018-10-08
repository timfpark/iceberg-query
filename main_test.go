package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
)

func TestQuery(t *testing.T) {
	log.Printf("Starting TestQuery")

	url := fmt.Sprintf("http://localhost:%s/q?p=userid1&sk=950000&ek=1050000", os.Getenv("PORT"))
	response, err := http.Get(url)
	if err != nil {
		t.Errorf("fetching key range query returned error: %s.", err)
		return
	}

	defer response.Body.Close()

	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Printf("%s", err)
		os.Exit(1)
	}

	if len(contents) != 872 {
		t.Errorf("Query results were not the right size: %d vs. 872", len(contents))
		return
	}

	log.Printf("Finished TestQuery")
}
