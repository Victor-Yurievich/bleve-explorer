//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	bleveMappingUI "github.com/blevesearch/bleve-mapping-ui"
	"github.com/blevesearch/bleve/v2"
	bleveHttp "github.com/blevesearch/bleve/v2/http"

	// import general purpose configuration
	_ "github.com/blevesearch/bleve/v2/config"
)

var bindAddr = flag.String("addr", ":8095", "http listen address")
var dataDir = flag.String("dataDir", "data", "data directory")
var staticEtag = flag.String("staticEtag", "", "optional static etag value.")
var staticPath = flag.String("static", "",
	"optional path to static directory for web resources")
var staticBleveMappingPath = flag.String("staticBleveMapping", "",
	"optional path to static-bleve-mapping directory for web resources")
var mainDir, currentDir = "", ""
var firstEnter = true

func main() {
	go handleOsSignals()
	flag.Parse()
	log.Println(*dataDir, " - Indexes directory, start reading")
	defer os.RemoveAll(mainDir)
	err := filepath.Walk(*dataDir, visit)
	dirEntries, err := ioutil.ReadDir(mainDir)
	if err != nil {
		log.Fatalf("error reading data dir: %v", err)
	}
	log.Println("Indexes directory read has finished")
	timer := exitIfIndexesDirectoryIsLocked()
	for _, dirInfo := range dirEntries {
		indexPath := mainDir + string(os.PathSeparator) + dirInfo.Name()
		log.Println("Reading indexes")
		// skip single files in data dir since a valid index is a directory that
		// contains multiple files
		if !dirInfo.IsDir() {
			log.Printf("not registering %s, skipping", indexPath)
			continue
		}
		log.Println(indexPath, " - Path to the Index")
		i, err := bleve.Open(indexPath)
		if err != nil {
			log.Printf("error opening index %s: %v", indexPath, err)
		} else {
			log.Printf("registered index: %s", dirInfo.Name())
			bleveHttp.RegisterIndexName(dirInfo.Name(), i)
			// set correct name in stats
			i.SetName(dirInfo.Name())
		}
	}
	timer.Stop()
	log.Println("Indexes read finished")
	router := mux.NewRouter()
	router.StrictSlash(true)

	// default to bindata for static-bleve-mapping resources.
	staticBleveMapping := http.FileServer(bleveMappingUI.AssetFS())
	if *staticBleveMappingPath != "" {
		fi, err := os.Stat(*staticBleveMappingPath)
		if err == nil && fi.IsDir() {
			log.Printf("using static-bleve-mapping resources from %s",
				*staticBleveMappingPath)
			staticBleveMapping = http.FileServer(http.Dir(*staticBleveMappingPath))
		}
	}

	router.PathPrefix("/static-bleve-mapping/").
		Handler(http.StripPrefix("/static-bleve-mapping/", staticBleveMapping))

	// default to bindata for static resources.
	static := http.FileServer(assetFS())
	if *staticPath != "" {
		fi, err := os.Stat(*staticPath)
		if err == nil && fi.IsDir() {
			log.Printf("using static resources from %s",
				*staticPath)
			static = http.FileServer(http.Dir(*staticPath))
		}
	}

	staticFileRouter(router, static)

	// add the API
	bleveMappingUI.RegisterHandlers(router, "/api")

	createIndexHandler := bleveHttp.NewCreateIndexHandler(mainDir)
	createIndexHandler.IndexNameLookup = indexNameLookup
	router.Handle("/api/{indexName}", createIndexHandler).Methods("PUT")

	getIndexHandler := bleveHttp.NewGetIndexHandler()
	getIndexHandler.IndexNameLookup = indexNameLookup
	router.Handle("/api/{indexName}", getIndexHandler).Methods("GET")

	deleteIndexHandler := bleveHttp.NewDeleteIndexHandler(mainDir)
	deleteIndexHandler.IndexNameLookup = indexNameLookup
	router.Handle("/api/{indexName}", deleteIndexHandler).Methods("DELETE")

	listIndexesHandler := bleveHttp.NewListIndexesHandler()
	router.Handle("/api", listIndexesHandler).Methods("GET")

	docIndexHandler := bleveHttp.NewDocIndexHandler("")
	docIndexHandler.IndexNameLookup = indexNameLookup
	docIndexHandler.DocIDLookup = docIDLookup
	router.Handle("/api/{indexName}/{docID}", docIndexHandler).Methods("PUT")

	docCountHandler := bleveHttp.NewDocCountHandler("")
	docCountHandler.IndexNameLookup = indexNameLookup
	router.Handle("/api/{indexName}/_count", docCountHandler).Methods("GET")

	docGetHandler := bleveHttp.NewDocGetHandler("")
	docGetHandler.IndexNameLookup = indexNameLookup
	docGetHandler.DocIDLookup = docIDLookup
	router.Handle("/api/{indexName}/{docID}", docGetHandler).Methods("GET")

	docDeleteHandler := bleveHttp.NewDocDeleteHandler("")
	docDeleteHandler.IndexNameLookup = indexNameLookup
	docDeleteHandler.DocIDLookup = docIDLookup
	router.Handle("/api/{indexName}/{docID}", docDeleteHandler).Methods("DELETE")

	searchHandler := bleveHttp.NewSearchHandler("")
	searchHandler.IndexNameLookup = indexNameLookup
	router.Handle("/api/{indexName}/_search", searchHandler).Methods("POST")

	listFieldsHandler := bleveHttp.NewListFieldsHandler("")
	listFieldsHandler.IndexNameLookup = indexNameLookup
	router.Handle("/api/{indexName}/_fields", listFieldsHandler).Methods("GET")

	debugHandler := bleveHttp.NewDebugDocumentHandler("")
	debugHandler.IndexNameLookup = indexNameLookup
	debugHandler.DocIDLookup = docIDLookup
	router.Handle("/api/{indexName}/{docID}/_debug", debugHandler).Methods("GET")

	aliasHandler := bleveHttp.NewAliasHandler()
	router.Handle("/api/_aliases", aliasHandler).Methods("POST")

	// start the HTTP server
	http.Handle("/", router)
	log.Printf("Listening on %v", *bindAddr)
	log.Fatal(http.ListenAndServe(*bindAddr, nil))
}

func exitIfIndexesDirectoryIsLocked() *time.Timer {
	timer := time.NewTimer(time.Second * 10)
	go func() {
		<-timer.C
		os.RemoveAll(mainDir)
		log.Fatalf("The %s directory is locked by another process. Exiting the program", *dataDir)
	}()
	return timer
}

func handleReadError(err error) {
	if err != nil {
		os.RemoveAll(mainDir)
		log.Fatalf("Error %s while reading file/directory. Exiting the program", err)
	}
}

func visit(p string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if info.IsDir() {
		createTempDir(p)
		return nil
	}
	copyIndexFileToTempDirectory(p)
	return nil
}

func createTempDir(p string) {
	dir, err := os.MkdirTemp(mainDir, filepath.Base(p))
	handleReadError(err)
	if firstEnter == true {
		mainDir = dir
		firstEnter = false
	}
	currentDir = dir
}

func copyIndexFileToTempDirectory(p string) {
	bytesRead, err := ioutil.ReadFile(p)
	handleReadError(err)
	fmt.Println(filepath.Join(currentDir, filepath.Base(p)))
	dst, err := os.Create(filepath.Join(currentDir, filepath.Base(p)))
	handleReadError(err)
	defer dst.Close()
	fmt.Println(dst.Name())
	err = ioutil.WriteFile(dst.Name(), bytesRead, 0755)
	handleReadError(err)
}

func handleOsSignals() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)

	go func() {
		sig := <-sigs
		fmt.Printf("\nGot %s\n", sig)
		fmt.Printf("Removing temporary dataDir '%s'\n", mainDir)
		os.RemoveAll(mainDir)
		log.Fatalf("Exiting program")
		done <- true
	}()
	<-done
}
