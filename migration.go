// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"log"
	"strconv"
	"sync"
	"encoding/json"
)

func migrateGeoNames(
	db *gorm.DB,
	wg *sync.WaitGroup,
	numberOfThread uint64,
	threadNumber uint64,
	configuration DataBaseConfig) {

	var (
		threadsCount = strconv.FormatUint(numberOfThread, 10)
		threadId = strconv.FormatUint(threadNumber, 10)
		limit = strconv.FormatUint(uint64(configuration.Limit), 10)

		lastId uint64 = 0
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

			db.Model(&row).Association("AlternativeNames").Find(&row.AlternativeNames);


			jsonResult, _ := json.Marshal(row.AlternativeNames)
			alternatenames := string(jsonResult)

			jsonResult, _ = json.Marshal(row.GetLocalizationNames());
			localeNames := string(jsonResult)

			db.Model(&row).Where("geonameid = ?", row.Geonameid).Updates(map[string]interface{}{"alternatenames": alternatenames})

			lastId = row.GetId()
			row.Prepare()
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

var (
	totalFetch Counter
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

	startProcessingMigration(db, config.DataBase)

	log.Print("Finished ")
}
