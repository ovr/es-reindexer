// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	esreindexer "github.com/interpals/es-reindexer"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"log"
	"strconv"
	"strings"
	"sync"
)

func migrateGeoNames(
	db *gorm.DB,
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
			  geo.geonameid,
			  geo.name,
			  geo.country,
			  geo.fclass,
			  geo.fcode,
			  geo.admin1,
			  geo.cc2,
			  geo.population,
			  geo.timezone,
			  geo.latitude,
			  geo.longitude,
			  (
			    SELECT GROUP_CONCAT(
			      CONCAT_WS(
				',',
				alternatename.alternateName,
				alternatename.isoLanguage,
				alternatename.isPreferredName
			      ) SEPARATOR '|'
			    )
			    FROM alternatename
			    WHERE
			    	alternatename.geonameid = geo.geonameid AND
			    	alternatename.isoLanguage NOT IN ("link", "iata")
			  ) as alternativenames_as_string
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

			var row esreindexer.GeoName

			err := db.ScanRows(rows, &row)
			if err != nil {
				panic(err)
			}

			row.Prepare()

			// GNObjectBatchChannel
			jsonResult, err := json.Marshal(row.GetLocalizationNames())
			if err != nil {
				panic(err)
			}

			var regionId *uint64 = nil

			// A: country, state, region,..., it cannot have region_id
			if (row.Fclass != "A") {
				adminCodeRow, ok := admin1Codes[row.Country+`.`+row.Admin1]
				if ok {
					regionId = &adminCodeRow.Geonameid
				}
			}

			GNObjectBatchChannel <- esreindexer.GNObject{
				Id:         row.GetId(),
				Names:      string(jsonResult),
				Latitude:   row.Latitude,
				Longitude:  row.Longitude,
				Population: row.Population,
				Iso:        row.Country,
				Timezone:   row.Timezone,
				RegionId:   regionId,
			}

			// GNObjectAlternateNamesBatchChannel
			jsonResult, err = json.Marshal(row.AlternativeNames)
			if err != nil {
				panic(err)
			}

			GNObjectAlternateNamesBatchChannel <- esreindexer.GNObjectAlternateNames{
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

func processBulkInsert(db *gorm.DB, buffer [][]interface{}, tableName string) {
	sqlStr := "INSERT INTO " + tableName + " VALUES "
	vals := []interface{}{}

	for _, row := range buffer {
		q := strings.Repeat("?,", len(row))
		q = q[0 : len(q)-1]

		sqlStr += fmt.Sprintf("(%s),", q)
		vals = append(vals, row...)
	}

	sqlStr = sqlStr[0 : len(sqlStr)-1]

	commonDb := db.CommonDB()
	stmt, err := commonDb.Prepare(sqlStr)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, execError := stmt.Exec(vals...)
	if execError != nil {
		panic(execError)
	}
}

func processChannelBuffer(db *gorm.DB, buffer chan esreindexer.GNObjectIterface, wg *sync.WaitGroup) {
	var (
		batchCount uint16 = 0
		record     esreindexer.GNObjectIterface
		bulkBuffer [][]interface{}
	)

	for record = range buffer {
		bulkBuffer = append(bulkBuffer, record.GetValues())

		batchCount++

		if batchCount >= 100 {
			processBulkInsert(db, bulkBuffer, record.TableName())

			// clear slice
			bulkBuffer = bulkBuffer[:0]
			batchCount = 0
		}
	}

	// Lets process latest records from bulkBuffer
	if batchCount > 0 {
		processBulkInsert(db, bulkBuffer, record.TableName())
	}

	wg.Done()
}

func startProcessingMigration(db *gorm.DB, configuration esreindexer.DataBaseConfig) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	threadsNumbers := uint64(configuration.Threads)

	for i := uint64(0); i < threadsNumbers; i++ {
		wg.Add(1)
		go migrateGeoNames(db.New(), wg, threadsNumbers, i, configuration)
	}

	// Don't close fetchedRecords channel before all fetch goroutines will finish
	wg.Wait()

	close(GNObjectBatchChannel)
	close(GNObjectAlternateNamesBatchChannel)

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

			var row esreindexer.GeoAdmin1Code

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

type AdminCodesMap map[string]esreindexer.GeoAdmin1Code

var (
	totalFetch  esreindexer.Counter
	admin1Codes AdminCodesMap

	GNObjectBatchChannel               chan esreindexer.GNObjectIterface //GNObject
	GNObjectAlternateNamesBatchChannel chan esreindexer.GNObjectIterface //GNObjectAlternateNames
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "", "Config filepath")
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

	db.LogMode(config.DataBase.ShowLog)
	db.DB().SetMaxIdleConns(config.DataBase.MaxIdleConnections)
	db.DB().SetMaxOpenConns(config.DataBase.MaxOpenConnections)

	admin1Codes = AdminCodesMap{}
	fetchAdmin1Codes(db)

	GNObjectBatchChannel = make(chan esreindexer.GNObjectIterface, 1000000)               // async channel
	GNObjectAlternateNamesBatchChannel = make(chan esreindexer.GNObjectIterface, 1000000) // async channel

	go startProcessingMigration(db, config.DataBase)


	var wgProcess sync.WaitGroup;

	wgProcess.Add(2);

	go processChannelBuffer(db, GNObjectBatchChannel, &wgProcess)
	go processChannelBuffer(db, GNObjectAlternateNamesBatchChannel, &wgProcess)

	wgProcess.Wait()

	log.Print("Finished")
}
