// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"gopkg.in/olivere/elastic.v3"
	"log"
	"strconv"
	"sync"
	"flag"
)

func fetchUsers(db *gorm.DB, users chan *User, wg *sync.WaitGroup, partSize uint64, part uint64)  {
	const LIMIT uint64 = 500

	maxOffset := part * partSize + partSize;
	for offset := uint64(part * partSize); offset < maxOffset; offset += LIMIT {
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
			WHERE activated = 1 AND searchable = 1
			LIMIT ` + strconv.FormatUint(LIMIT, 10) + ` OFFSET ` + strconv.FormatUint(offset, 10)).Rows()
		if err != nil {
			panic(err)
		}

		for rows.Next() {
			var user User

			err := db.ScanRows(rows, &user)
			if err != nil {
				panic(err)
			}

			//db.Model(&user).Related(&user.Known);
			//db.Model(&user).Related(&user.Learn);
			user.Prepare()

			users <- &user
		}

		rows.Close()
	}

	wg.Done()
	log.Print("Finished fetch goroutine ", part);
}

func startFetchUsers(db *gorm.DB, users chan *User) {
	var total uint64

	totalRow := db.Raw(`SELECT COUNT(*) FROM users WHERE activated = 1 AND searchable = 1;`).Row()
	err := totalRow.Scan(&total)
	if err != nil {
		panic(err)
	}

	log.Print("Total users: ", total)

	const THREADS_NUMBER  = 4;

	var wg *sync.WaitGroup = new(sync.WaitGroup)

	for i := uint64(0); i < THREADS_NUMBER; i++ {
		wg.Add(1)
		go fetchUsers(db.New(), users, wg, total / THREADS_NUMBER, i)
	}

	// Don't close users channel before all fetch goroutines will finish
	wg.Wait()

	log.Print("Waith group for fetch finished")

	// No users, lets close channel to stop range query and send latest bulk request
	close(users)
}

func batchUsers(client *elastic.Client, users chan *User, done chan bool) {
	bulkRequest := client.Bulk()

	for user := range users {
		//log.Print(user.Id)

		request := elastic.NewBulkIndexRequest().
			Index("users").
			Type("users").
			Id(strconv.FormatUint(user.Id, 10)).
			Doc(user)

		bulkRequest.Add(request)

		if bulkRequest.NumberOfActions() >= 1000 {
			log.Print("[ES] Bulk insert go ", bulkRequest.NumberOfActions())

			_, err := bulkRequest.Do()
			if err != nil {
				panic(err)
			}

			bulkRequest = client.Bulk()
		}
	}

	log.Print("Closed channel");

	if (bulkRequest.NumberOfActions() > 0) {
		log.Print("Bulk insert go ", bulkRequest.NumberOfActions())
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

	users := make(chan *User, 1000) // 100k async channel

	go startFetchUsers(db, users)
	go batchUsers(client, users, done)

	log.Print("Finished ", <-done)
}
