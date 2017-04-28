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
)

func createSelectUsersQuery(order string, limit string, condition string) string {
	return `
	SELECT
		u.id,
		u.name,
		u.username,
		u.last_login,
		u.signup,
		u.modified,
		u.sex,
		u.tz,
		u.city,
		u.country,
		u.iso2,
		u.website,
		u.main_photo_id,
		u.photo_exists,
		u.main_thumb,
		u.cont,
		u.age,
		u.birth,
		u.lfor_email,
		u.lfor_flirt,
		u.lfor_friend,
		u.lfor_langex,
		u.lfor_relation,
		u.lfor_snail,
		pt.description,
		pt.books,
		pt.hobbies,
		pt.movies,
		pt.requests,
		pt.music,
		pt.quotes,
		pt.tv,
		pt.langex_desc,
		(SELECT GROUP_CONCAT(CONCAT_WS('|', known.lang, known.level) SEPARATOR ',')
		FROM user_langs known WHERE known.user_id = u.id) as knowninfo,
		(SELECT GROUP_CONCAT(CONCAT_WS('|', learn.lang, learn.level) SEPARATOR ',')
		FROM user_langs_learn learn WHERE learn.user_id = u.id) as learninfo
	FROM users u
	LEFT JOIN profiles_text pt ON u.id = pt.id
	WHERE ` + condition + `
	activated = 1
	AND searchable = 1
	ORDER BY ` + order + `
	LIMIT ` + limit
}

func fetchUsers(
	db *gorm.DB,
	users chan esreindexer.FetchedRecord,
	wg *sync.WaitGroup,
	numberOfThread uint64,
	threadNumber uint64,
	configuration esreindexer.DataBaseConfig) {

	var (
		threadsCount = strconv.FormatUint(numberOfThread, 10)
		threadId     = strconv.FormatUint(threadNumber, 10)
		limit        = strconv.FormatUint(uint64(configuration.Limit), 10)

		lastId    uint64 = 0
		lastCount uint64
	)

	for {
		lastCount = 0

		condition := `u.id > ` + strconv.FormatUint(lastId, 10) + ` AND u.id % ` + threadsCount + ` = ` + threadId + ` AND `
		rows, err := db.Raw(createSelectUsersQuery("id ASC", limit, condition)).Rows()

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

			lastId = user.GetId()
			user.Prepare()

			users <- user
		}

		if lastCount == 0 {
			// Nothing to fetch
			break
		}

		totalFetch.Add(lastCount)

		rows.Close()
	}

	wg.Done()
	log.Print("Finished fetch goroutine ", threadNumber)
}

func startFetch(db *gorm.DB, users chan esreindexer.FetchedRecord, configuration esreindexer.DataBaseConfig, command string) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	threadsNumbers := uint64(configuration.Threads)

	for i := uint64(0); i < threadsNumbers; i++ {
		wg.Add(1)

		switch command {
		case "users":
			go fetchUsers(db.New(), users, wg, threadsNumbers, i, configuration)
			break
		case "geonames":
			go fetchGeoNames(db.New(), users, wg, threadsNumbers, i, configuration)
			break
		default:
			panic("Unknown command to process")
		}

	}

	// Don't close users channel before all fetch goroutines will finish
	wg.Wait()

	log.Print("Waith group for fetch finished")
	log.Print("Total Fetched ", totalFetch.Value())

	// No users, lets close channel to stop range query and send latest bulk request
	close(users)
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

func fetchGeoNames(
	db *gorm.DB,
	channel chan esreindexer.FetchedRecord,
	wg *sync.WaitGroup,
	numberOfThread uint64,
	threadNumber uint64,
	configuration esreindexer.DataBaseConfig) {

	var (
		threadsCount = strconv.FormatUint(numberOfThread, 10)
		threadId     = strconv.FormatUint(threadNumber, 10)
		limit        = strconv.FormatUint(uint64(configuration.Limit), 10)

		lastId    uint64 = 0
		lastCount uint64
	)

	for {
		lastCount = 0

		rows, err := db.Raw(`
			SELECT
			  obj.*,
			  a1.alternatenames
			FROM gn_object obj
			JOIN gn_object_alternatenames a1 ON obj.id = a1.id
			WHERE obj.id > ` + strconv.FormatUint(lastId, 10) +
			` AND obj.id % ` + threadsCount + ` = ` + threadId +
			` ORDER BY obj.id ASC LIMIT ` + limit).Rows()

		if err != nil {
			panic(err)
		}

		for rows.Next() {
			lastCount++

			var row esreindexer.GNObjectAggregate

			err := db.ScanRows(rows, &row)
			if err != nil {
				panic(err)
			}

			lastId = row.GetId()
			channel <- row
		}

		if lastCount == 0 {
			// Nothing to fetch
			break
		}

		totalFetch.Add(lastCount)

		rows.Close()
	}

	wg.Done()
	log.Print("Finished fetch goroutine ", threadNumber)
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

	db, err := gorm.Open(config.DataBase.Dialect, config.DataBase.Uri)
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

	command := flag.Arg(0)
	switch command {
	case "delta":
		model := flag.Arg(1)
		switch model {
		case "users":
		case "geonames":
			break
		default:
			log.Print("Unknown model, available models: [users, geonames]")
			os.Exit(1)
			break
		}

		if field != "signup" && field != "last_login" {
			panic("Sort field must be [signup, last_login]")
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
	case "geonames":
		go startFetch(db, fetchedRecords, config.DataBase, command)
		break
	default:
		log.Print("Unknown command, available commands: [users, geonames]")
		os.Exit(1)
		break
	}

	startProcessing(client, fetchedRecords, config.ElasticSearch)

	log.Print("Finished ")
}
