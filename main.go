// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"gopkg.in/olivere/elastic.v3"
	"log"
	"os"
	"strconv"
	"sync"
)

func fetchUsers(
	db *gorm.DB,
	users chan FetchedRecord,
	wg *sync.WaitGroup,
	numberOfThread uint64,
	threadNumber uint64,
	configuration DataBaseConfig) {

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
				u.id,
				u.name,
				u.username,
				u.last_login,
				u.modified,
				u.sex,
				u.tz,
				u.city,
				u.country,
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
			WHERE u.id > ` + strconv.FormatUint(lastId, 10) +
			` AND u.id % ` + threadsCount + ` = ` + threadId +
			` AND activated = 1 AND searchable = 1 ORDER BY id ASC
			LIMIT ` + limit).Rows()

		if err != nil {
			panic(err)
		}

		for rows.Next() {
			lastCount++

			var user User

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

func startFetch(db *gorm.DB, users chan FetchedRecord, configuration DataBaseConfig, command string) {
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

func fetchGeoNames(
	db *gorm.DB,
	channel chan FetchedRecord,
	wg *sync.WaitGroup,
	numberOfThread uint64,
	threadNumber uint64,
	configuration DataBaseConfig) {

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
			  a1.alternatenames,
			  region.names as region_names,
			  a2.alternatenames as region_alternatenames
			FROM gn_object obj
			LEFT JOIN gn_object_alternatenames a1 ON obj.id = a1.id
			LEFT JOIN gn_object region ON obj.region_id = region.id
			LEFT JOIN gn_object_alternatenames a2 ON region.region_id = a2.id
			WHERE obj.id > ` + strconv.FormatUint(lastId, 10) +
			` AND obj.id % ` + threadsCount + ` = ` + threadId +
			` ORDER BY obj.id ASC LIMIT ` + limit).Rows()

		if err != nil {
			panic(err)
		}

		for rows.Next() {
			lastCount++

			var row GNObjectAggregate

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
	fetchedRecords chan FetchedRecord,
	wg *sync.WaitGroup,
	configuration ElasticSearchConfig,
	meta MetaDataES) {
	bulkRequest := client.Bulk()

	for record := range fetchedRecords {
		request := elastic.NewBulkIndexRequest().
			Index(meta.GetIndex()).
			Type(meta.GetType()).
			Id(strconv.FormatUint(record.GetId(), 10)).
			Doc(record.GetSearchData())

		bulkRequest.Add(request)

		if bulkRequest.NumberOfActions() >= int(configuration.Limit) {
			totalSend.Add(uint64(bulkRequest.NumberOfActions()))

			log.Print(
				"[ES] Bulk insert ", bulkRequest.NumberOfActions(),
				" buffer ", len(fetchedRecords),
				" fetch ", totalFetch.Value(),
				" send ", totalSend.Value())

			_, err := bulkRequest.Do()
			if err != nil {
				panic(err)
			}

			bulkRequest = client.Bulk()
		}
	}

	log.Print("Closed channel")

	if bulkRequest.NumberOfActions() > 0 {
		log.Print("Latest Bulk insert go ", bulkRequest.NumberOfActions())
		_, err := bulkRequest.Do()
		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}

func startProcessing(
	client *elastic.Client,
	fetchedRecords chan FetchedRecord,
	configuration ElasticSearchConfig,
	meta MetaDataES) {

	var wg *sync.WaitGroup = new(sync.WaitGroup)

	for i := uint8(0); i < configuration.Threads; i++ {
		wg.Add(1)
		go processFetchedRecords(client, fetchedRecords, wg, configuration, meta)
	}

	// Don't close fetchedRecords channel before all fetch goroutines will finish
	wg.Wait()
}

var (
	totalFetch Counter
	totalSend  Counter
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "", "Config filepath")
	flag.Parse()

	if configFile == "" {
		panic("Please setup config parameter")
	}

	var config Configuration
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

	fetchedRecords := make(chan FetchedRecord, config.ChannelBufferSize) // async channel

	var metaData MetaDataES

	command := flag.Arg(0)
	switch command {
	case "users":
		go startFetch(db, fetchedRecords, config.DataBase, command)
		metaData = MetaDataESUsers{}
		break
	case "geonames":
		go startFetch(db, fetchedRecords, config.DataBase, command)
		metaData = MetaDataESGeoNames{}
		break
	default:
		log.Print("Unknown command, available commands: [users, geonames]")
		os.Exit(1)
		break
	}

	startProcessing(client, fetchedRecords, config.ElasticSearch, metaData)

	log.Print("Finished ")
}
