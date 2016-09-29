// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	"encoding/json"
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"log"
	"strconv"
	"sync"
)

func migrateGeoNames(
	db *gorm.DB,
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

			db.Model(&row).Association("AlternativeNames").Find(&row.AlternativeNames)

			// GNObjectBatchChannel
			jsonResult, err := json.Marshal(row.GetLocalizationNames())
			if err != nil {
				panic(err)
			}

			var regionId *uint64 = nil

			adminCodeRow, ok := admin1Codes[row.Country+`.`+row.Admin1]
			if ok {
				regionId = &adminCodeRow.Geonameid
			}

			GNObjectBatchChannel <- GNObject{
				Id:         row.GetId(),
				Names:      string(jsonResult),
				Latitude:   row.Latitude,
				Longitude:  row.Longitude,
				Population: row.Population,
				Iso:        row.Cc2,
				Timezone:   row.Timezone,
				RegionId:   regionId,
			}

			// GNObjectAlternateNamesChannel
			jsonResult, err = json.Marshal(row.GetAlternativeNames())
			if err != nil {
				panic(err)
			}

			GNObjectAlternateNamesChannel <- GNObjectAlternateNames{
				Id:    row.GetId(),
				Names: string(jsonResult),
			}

			lastId = row.GetId()
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

func processGNObjectBatchChannel(db *gorm.DB, configuration DataBaseConfig) {
	batchCount := 0
	trDB := db.Begin()

	for record := range GNObjectBatchChannel {
		trDB.Create(record)

		batchCount++

		if batchCount > 100 {
			log.Print("Batch GNObject")

			trDB.Commit()

			batchCount = 0
			trDB = db.Begin()
		}
	}
}

func processGNObjectAlternateNamesChannel(db *gorm.DB, configuration DataBaseConfig) {
	batchCount := 0
	trDB := db.Begin()

	for record := range GNObjectAlternateNamesChannel {
		trDB.Create(record)

		batchCount++

		if batchCount > 100 {
			log.Print("Batch GNObjectAlternateNames")

			trDB.Commit()

			batchCount = 0
			trDB = db.Begin()
		}
	}
}

func startProcessingMigration(db *gorm.DB, configuration DataBaseConfig) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	threadsNumbers := uint64(configuration.Threads)

	for i := uint64(0); i < threadsNumbers; i++ {
		wg.Add(1)
		go migrateGeoNames(db.New(), wg, threadsNumbers, i, configuration)
	}

	// Don't close fetchedRecords channel before all fetch goroutines will finish
	wg.Wait()

	log.Print("Waith group for fetch finished")
	log.Print("Total Fetched ", totalFetch.Value())
}

func fetchAdmin1Codes(db *gorm.DB) {
	var (
		lastCount uint64
		offset    uint64 = 0
	)

	for {
		lastCount = 0

		rows, err := db.Raw(`
			SELECT *
			FROM admin1CodesAscii
			ORDER BY code ASC
			LIMIT 1000 OFFSET ` + strconv.FormatUint(offset, 10)).Rows()

		if err != nil {
			panic(err)
		}

		for rows.Next() {
			lastCount++

			var row GeoAdmin1Code

			err := db.ScanRows(rows, &row)
			if err != nil {
				panic(err)
			}

			admin1Codes[row.Code] = row
		}

		if lastCount == 0 {
			// Nothing to fetch
			break
		}

		offset += lastCount

		rows.Close()
	}
}

type AdminCodesMap map[string]GeoAdmin1Code

var (
	totalFetch  Counter
	admin1Codes AdminCodesMap

	GNObjectBatchChannel          chan GNObject
	GNObjectAlternateNamesChannel chan GNObjectAlternateNames
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

	db.LogMode(config.DataBase.ShowLog)
	db.DB().SetMaxIdleConns(config.DataBase.MaxIdleConnections)
	db.DB().SetMaxOpenConns(config.DataBase.MaxOpenConnections)

	admin1Codes = AdminCodesMap{}
	fetchAdmin1Codes(db)

	GNObjectBatchChannel = make(chan GNObject, 1000000)                        // async channel
	GNObjectAlternateNamesChannel = make(chan GNObjectAlternateNames, 1000000) // async channel

	go startProcessingMigration(db, config.DataBase)

	go processGNObjectBatchChannel(db, config.DataBase)
	processGNObjectAlternateNamesChannel(db, config.DataBase)

	log.Print("Finished ")
}
