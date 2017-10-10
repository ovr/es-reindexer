// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	esreindexer "github.com/interpals/es-reindexer"
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/olivere/elastic"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"context"
	"time"
)

func startFetch(db *gorm.DB, eschan chan esreindexer.FetchedRecord, configuration esreindexer.DataBaseConfig, command string) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	threadsNumbers := uint64(configuration.Threads)

	for i := uint64(0); i < threadsNumbers; i++ {
		wg.Add(1)

		switch command {
		case "users":
			go fetchUsers(db.New(), eschan, wg, threadsNumbers, i, configuration)
			break
		case "geo":
			go fetchGeo(db.New(), eschan, wg, threadsNumbers, i, configuration)
			break
		default:
			panic("Unknown command to process")
		}

	}

	// Don't close users channel before all fetch goroutines will finish
	wg.Wait()

	log.Print("Wait group for fetch finished")
	log.Print("Total Fetched ", totalFetch.Value())

	// No users, lets close channel to stop range query and send latest bulk request
	close(eschan)
}

func startFetchDelta(
	db *gorm.DB,
	users chan esreindexer.FetchedRecord,
	configuration esreindexer.DataBaseConfig,
	model string,
	field string,
	maxTotalFetch uint64) {

	if model != "users" {
		panic("Model is not supported, only user supported now")
	}

	var (
		limit = strconv.FormatUint(uint64(configuration.Limit), 10)

		lastCount  uint64
		totalCount uint64 = 0
	)

	for {
		lastCount = 0

		rows, err := db.Raw(createSelectUsersQuery(field+" DESC", limit, "")).Rows()
		if err != nil {
			panic(err)
		}

		for rows.Next() {
			lastCount++

			var user esreindexer.User

			err := db.ScanRows(rows, &user)
			if err != nil {
				panic(err)
			}

			user.Prepare()
			users <- user
		}

		if lastCount == 0 {
			// Nothing to fetch
			break
		}

		totalFetch.Add(lastCount)
		rows.Close()

		totalCount += lastCount
		if totalCount >= maxTotalFetch {
			// maxTotalFetch reached, lets exit from fetch
			break
		}
	}

	// No users, lets close channel to stop range query and send latest bulk request
	close(users)
}

func processFetchedRecords(
	client *elastic.Client,
	fetchedRecords chan esreindexer.FetchedRecord,
	wg *sync.WaitGroup,
	configuration esreindexer.ElasticSearchConfig) {

	var memStats runtime.MemStats
	bulkRequest := client.Bulk()

	for record := range fetchedRecords {
		request := elastic.NewBulkIndexRequest().
			Index(record.GetIndex()).
			Type(record.GetType()).
			Id(strconv.FormatUint(record.GetId(), 10)).
			Doc(record.GetSearchData())

		parent := record.GetParent()
		if parent != nil {
			request.Parent(strconv.FormatUint(*parent, 10))
		}

		bulkRequest.Add(request)

		if bulkRequest.NumberOfActions() >= int(configuration.Limit) {
			totalSend.Add(uint64(bulkRequest.NumberOfActions()))

			runtime.ReadMemStats(&memStats)
			log.Print(
				"[ES] Bulk insert ", bulkRequest.NumberOfActions(),
				" buffer ", len(fetchedRecords),
				" fetch ", totalFetch.Value(),
				" send ", totalSend.Value(),
				" alloc ", memStats.Alloc/1024/1024, "mb",
				" HeapObjects ", memStats.HeapObjects)

			ctx := context.Background()
			_, err := bulkRequest.Do(ctx)
			if err != nil {
				panic(err)
			}

			bulkRequest = client.Bulk()
		}
	}

	log.Print("Closed channel")

	if bulkRequest.NumberOfActions() > 0 {
		log.Print("Latest Bulk insert go ", bulkRequest.NumberOfActions())

		ctx := context.Background()
		_, err := bulkRequest.Do(ctx)
		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}

func startProcessing(
	client *elastic.Client,
	fetchedRecords chan esreindexer.FetchedRecord,
	configuration esreindexer.ElasticSearchConfig) {

	var wg *sync.WaitGroup = new(sync.WaitGroup)

	for i := uint8(0); i < configuration.Threads; i++ {
		wg.Add(1)
		go processFetchedRecords(client, fetchedRecords, wg, configuration)
	}

	// Don't close fetchedRecords channel before all fetch goroutines will finish
	wg.Wait()
}

var (
	totalFetch esreindexer.Counter
	totalSend  esreindexer.Counter
)

func main() {
	var (
		configFile    string
		field         string
		maxTotalFetch uint64
	)

	// I cannot define flags parsing inside command with standart library, this will cause a problem
	// flag provided but not defined: -total or not parsed
	// @todo Maybe better CLI library?
	flag.StringVar(&configFile, "config", "", "Config filepath")
	flag.StringVar(&field, "field", "signup", "What field will be used on delta sort")
	flag.Uint64Var(&maxTotalFetch, "total", 1000, "How many records we will fetch before exit")

	flag.Parse()

	if configFile == "" {
		panic("Please setup config parameter")
	}

	var config esreindexer.Configuration
	config.Init(configFile)


	var dbUri string
	var model string
	command := flag.Arg(0)

	switch command {
		case "users-delta":
			fallthrough
		case "users":
			dbUri = config.DataBase.Uri
			model = "users"
			break;
		case "geo":
			dbUri = config.DataBase.UriGeo
			model = "geo"
			break;
		default:
			log.Print("Usage: es-reindexer [users|geo|users-delta]")
			os.Exit(1)
			break
	}

	db, err := gorm.Open(config.DataBase.Dialect, dbUri)

	if err != nil {
		panic(err)
	}
	defer db.Close()

	client, err := elastic.NewClient(elastic.SetURL(config.ElasticSearch.Uri))
	if err != nil {
		panic(err)
	}

	db.LogMode(config.DataBase.ShowLog)
	db.DB().SetMaxIdleConns(config.DataBase.MaxIdleConnections)
	db.DB().SetMaxOpenConns(config.DataBase.MaxOpenConnections)

	fetchedRecords := make(chan esreindexer.FetchedRecord, config.ChannelBufferSize) // async channel

	switch command {
	case "users-delta":
		if field != "signup" && field != "last_login" && field != "modified" {
			panic("Sort field must be [signup, last_login, modified]")
		}

		if maxTotalFetch < 100 || maxTotalFetch > 100000 {
			panic("Total must be 100 < x < 100k")
		}

		log.Print("Sort field ", field)
		log.Print("Max total fetch ", maxTotalFetch)

		go startFetchDelta(db, fetchedRecords, config.DataBase, model, field, maxTotalFetch)
		break
	case "users":
		go startFetch(db, fetchedRecords, config.DataBase, command)
		break
	case "geo":
		go startFetch(db, fetchedRecords, config.DataBase, command)
		break
	}

	time.Sleep(time.Millisecond * 5000)
	startProcessing(client, fetchedRecords, config.ElasticSearch)

	log.Print("Finished ")
}
