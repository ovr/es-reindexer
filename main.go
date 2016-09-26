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
	"encoding/json"
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

			lastId = user.Id
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

func startFetchUsers(db *gorm.DB, users chan FetchedRecord, configuration DataBaseConfig) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	threadsNumbers := uint64(configuration.Threads)

	for i := uint64(0); i < threadsNumbers; i++ {
		wg.Add(1)
		go fetchUsers(db.New(), users, wg, threadsNumbers, i, configuration)
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
			SELECT geo.*,
			(
				SELECT
				admin1.name FROM admin1CodesAscii admin1
				WHERE admin1.code = CONCAT(geo.country, '.', geo.admin1)
			) as region
			FROM geoname as geo
			WHERE geonameid > ` + strconv.FormatUint(lastId, 10) +
			` AND geonameid % ` + threadsCount + ` = ` + threadId +
			` ORDER BY geonameid ASC
			LIMIT ` + limit).Rows()

		if err != nil {
			panic(err)
		}

		for rows.Next() {
			lastCount++

			var row GeoName

			err := db.ScanRows(rows, &row)
			if err != nil {
				panic(err)
			}

			lastId = row.GetId()
			row.Prepare()

			json, _ := json.Marshal(row)
			log.Print(string(json))

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

func startFetchGeoNames(db *gorm.DB, fetchedRecords chan FetchedRecord, configuration DataBaseConfig) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	threadsNumbers := uint64(configuration.Threads)

	for i := uint64(0); i < threadsNumbers; i++ {
		wg.Add(1)
		go fetchGeoNames(db.New(), fetchedRecords, wg, threadsNumbers, i, configuration)
	}

	// Don't close fetchedRecords channel before all fetch goroutines will finish
	wg.Wait()

	log.Print("Waith group for fetch finished")
	log.Print("Total Fetched ", totalFetch.Value())

	// No records to fetch, lets close channel to stop range query and send latest bulk request
	close(fetchedRecords)
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
			Doc(record)

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
		go startFetchUsers(db, fetchedRecords, config.DataBase)
		metaData = MetaDataESGeoNames{}
		break
	case "geonames":
		go startFetchGeoNames(db, fetchedRecords, config.DataBase)
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
