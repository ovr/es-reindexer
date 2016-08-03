// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"gopkg.in/olivere/elastic.v3"
	"log"
	"strconv"
	"sync"
)

func fetchUsers(
	db *gorm.DB,
	users chan *User,
	wg *sync.WaitGroup,
	numberOfThread uint64,
	threadNumber uint64,
	configuration DataBaseConfig) {

	var lastId uint64 = 0
	var limit string = strconv.FormatUint(uint64(configuration.Limit), 10)

	for {
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
			` AND u.id % ` + strconv.FormatUint(numberOfThread, 10) + ` = ` + strconv.FormatUint(threadNumber, 10) +
			` AND activated = 1 AND searchable = 1 ORDER BY id ASC
			LIMIT ` + limit).Rows()
		if err != nil {
			panic(err)
		}

		if !rows.Next() {
			break
		}

		for rows.Next() {
			var user User

			err := db.ScanRows(rows, &user)
			if err != nil {
				panic(err)
			}

			lastId = user.Id
			user.Prepare()

			users <- &user
		}

		rows.Close()
	}

	wg.Done()
	log.Print("Finished fetch goroutine ", threadNumber)
}

func startFetchUsers(db *gorm.DB, users chan *User, configuration DataBaseConfig) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	threadsNumbers := uint64(configuration.Threads)

	for i := uint64(0); i < threadsNumbers; i++ {
		wg.Add(1)
		go fetchUsers(db.New(), users, wg, threadsNumbers, i, configuration)
	}

	// Don't close users channel before all fetch goroutines will finish
	wg.Wait()

	log.Print("Waith group for fetch finished")

	// No users, lets close channel to stop range query and send latest bulk request
	close(users)
}

func batchUsers(client *elastic.Client, users chan *User, done chan bool, configuration ElasticSearchConfig) {
	bulkRequest := client.Bulk()

	for user := range users {
		request := elastic.NewBulkIndexRequest().
			Index("users").
			Type("users").
			Id(strconv.FormatUint(user.Id, 10)).
			Doc(user)

		bulkRequest.Add(request)

		if bulkRequest.NumberOfActions() >= int(configuration.Limit) {
			log.Print("[ES] Bulk insert go ", bulkRequest.NumberOfActions(), " channel buffer size ", len(users))

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

	done <- true
}

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "", "Config filepath")
	flag.Parse()

	if configFile == "" {
		panic("Please setup config parameter")
	}

	var config Configuration
	config.Init(configFile)

	done := make(chan bool, 1)

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

	users := make(chan *User, config.ChannelBufferSize) // async channel

	go startFetchUsers(db, users, config.DataBase)
	go batchUsers(client, users, done, config.ElasticSearch)

	log.Print("Finished ", <-done)
}
