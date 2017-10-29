package main

import (
	"github.com/jinzhu/gorm"
	"github.com/interpals/es-reindexer"
	"sync"
	"strconv"
	"log"
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
		u.wg_id,
		u.country,
		u.iso2,
		u.website,
		u.main_photo_id,
		u.photo_exists,
		u.main_thumb,
		u.cont,
		u.age,
		u.birth,
		u.lfor_friend,
		u.lfor_langex,
		u.lfor_relation,
		u.lfor_snail,
		u.lfor_meet,
		pt.description,
		pt.books,
		pt.hobbies,
		pt.movies,
		pt.requests,
		pt.music,
		pt.quotes,
		pt.tv,
		pt.langex_desc,
		u.education_level,
		u.education_desc,
		u.occupation,
		u.relationship,
		(SELECT GROUP_CONCAT(CONCAT_WS('|', known.lang, known.level) SEPARATOR ',')
		FROM user_langs known WHERE known.user_id = u.id) as knowninfo,
		(SELECT GROUP_CONCAT(CONCAT_WS('|', learn.lang, learn.level) SEPARATOR ',')
		FROM user_langs_learn learn WHERE learn.user_id = u.id) as learninfo,

		u.city_name_en,
		u.city_id,
		u.region_id,
		u.country_code,

		u.home_city_name_en,
		u.home_city_id,
		u.home_region_id,
		u.home_country_code

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
