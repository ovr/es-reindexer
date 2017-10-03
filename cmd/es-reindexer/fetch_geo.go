package main

import (
	"github.com/jinzhu/gorm"
	"github.com/interpals/es-reindexer"
	"sync"
	"strconv"
	"log"
)

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
	)

	// Fetch country names and cache them for concatenation with region/city suggestions
	// [country code][lang code] = country name
	esIndex := false
	if threadNumber == 0 {
		esIndex = true
	}

	countries := fetchCountries(db, channel, esIndex)
	fetchRegions(db, channel, threadsCount, threadId, limit, countries)
	fetchCities(db, channel, threadsCount, threadId, limit, countries)

	wg.Done()
	log.Print("Finished fetch goroutine ", threadNumber)
}

// Fetch countries, optionally indexing in ES (since called by every thread only one needs to do so)
func fetchCountries(
	db *gorm.DB,
	channel chan esreindexer.FetchedRecord,
	esImport bool,
) map[string]map[string]string {

	ctryLangNameRes := map[string]map[string]string{}

	var lastCount uint64 = 0
	var country esreindexer.GNItem

	rows, err := db.Raw(`
	SELECT
	a.isoLanguage lang,
    a.alternatename name,
	g.geonameid,
	g.country,
	g.population,
	g.latitude,
	g.longitude,
	g.timezone
	FROM gn10k.geoname g
	JOIN gn10k.alternatename a
	ON a.geonameid = g.geonameid
WHERE (g.fcode LIKE 'PCL%' OR g.fcode="TERR") AND
		a.isoLanguage NOT IN('link')
	ORDER BY
		g.geonameid asc,
	    a.isShortName desc,
		a.isPreferredName desc
	`).Rows()

	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var row esreindexer.GNCountryRow
		err := db.ScanRows(rows, &row)

		if err != nil {
			panic(err)
		}

		if country.Geonameid != row.Geonameid {
			if esImport && lastCount > 0 {
				channel <- country
			}

			country = esreindexer.GNItem{
				Type:         "country",
				Geonameid:    row.Geonameid,
				Country:      row.Country,
				Population:   row.Population,
				Timezone:     row.Timezone,
				Latitude:     row.Latitude,
				Longitude:    row.Longitude,
				CountryNames: map[string]string{},
				Suggestions:  map[string]bool{},
			}

			lastCount++
		}

		country.CountryNames[row.Lang] = row.Name
		country.Suggestions[row.Name] = true

		// Add to local cache results for region/city suggestion generation
		if _, ok := ctryLangNameRes[row.Country]; !ok {
			ctryLangNameRes[row.Country] = map[string]string{}
		}

		if _, ok := ctryLangNameRes[row.Country][row.Lang]; !ok {
			ctryLangNameRes[row.Country][row.Lang] = row.Name
		}
	}

	if esImport && lastCount > 0 {
		channel <- country
		totalFetch.Add(lastCount)
	}

	rows.Close()

	return ctryLangNameRes
}

func fetchRegions(
	db *gorm.DB,
	channel chan esreindexer.FetchedRecord,
	threadsCount string,
	threadId string,
	limit string,
	countries map[string]map[string]string,
) {
	var (
		region esreindexer.GNItem
		row    esreindexer.GNRegionRow

		lastId    uint64 = 0
		lastCount uint64
	)

	for {
		lastCount = 0

		rows, err := db.Raw(`
SELECT
    ac.geonameid geonameid,
    g.asciiname asciiname,
	g.name name,
    a.isoLanguage lang,
    a.alternateName altname,
	g.population population,
	g.timezone timezone,
	g.latitude latitude,
	g.longitude longitude,
	g.country country
FROM gn10k.admin1CodesAscii ac
LEFT JOIN gn10k.geoname g ON
	g.geonameid = ac.geonameid
LEFT OUTER JOIN gn10k.alternatename a ON
    ac.geonameid = a.geonameid AND
	a.isoLanguage NOT IN ('link', 'iata', 'post', 'icao', 'faac', 'fr_1793')
WHERE
	ac.geonameid IN
		(SELECT * FROM /* mysql is stupid and won't allow limits in IN subqueries */
			(SELECT geonameid
			FROM gn10k.admin1CodesAscii ac
			WHERE ac.geonameid > ` + strconv.FormatUint(lastId, 10) + ` AND
				  ac.geonameid % ` + threadsCount + ` = ` + threadId + `
		    ORDER BY geonameid ASC
			LIMIT ` + limit + `)t
	    )
ORDER BY
    ac.geonameid ASC,
	a.isoLanguage ASC,
    a.isPreferredName DESC,
    a.isShortName DESC
`).Rows()

		if err != nil {
			panic(err)
		}

		for rows.Next() {
			err := db.ScanRows(rows, &row)

			if err != nil {
				panic(err)
			}

			if region.Geonameid != row.Geonameid {
				if lastCount > 0 {
					channel <- region
				}

				// Create new region for this row
				region = esreindexer.GNItem{
					Type:        "region",
					Geonameid:   row.Geonameid,
					Country:     row.Country,
					Population:  row.Population,
					Timezone:    row.Timezone,
					Latitude:    row.Latitude,
					Longitude:   row.Longitude,
					RegionNames: map[string]string{},
					Suggestions: map[string]bool{},
				}

				lastCount++

				// Add English entries from main geoname table if first record
				regCtry := row.Name + " " + countries[row.Country]["en"]
				region.Suggestions[regCtry] = true

				// Add [City, Localized Country] for any lang that does not have an alt name
				for lang, name := range countries[region.Country] {
					if _, ok := region.RegionNames[lang]; !ok {
						region.Suggestions[row.Name+" "+name] = true
					}
				}

				region.RegionNames["en"] = row.Name
			}

			// Add non-English alternate names
			if len(row.Lang) > 0 {
				if len(row.Altname) > 0 && row.Altname != row.Name {
					if _, ok := region.RegionNames[row.Lang]; !ok {
						// Region names for this language don't exist yet, add it
						region.RegionNames[row.Lang] = row.Altname
					}
				}

				// Add suggestions (["Region Country"])
				if len(countries[row.Country][row.Lang]) > 0 {
					regCtry := row.Altname + " " + countries[row.Country][row.Lang]
					region.Suggestions[regCtry] = true
				}
			}

			lastId = row.Geonameid
		}

		// If no records fetched, finish
		if lastCount == 0 {
			break
		}

		// (For last record) add [Region, Localized Country] for any lang that does not have an alt name
		for lang, name := range countries[region.Country] {
			if _, ok := region.RegionNames[lang]; !ok {
				region.Suggestions[row.Name+" "+name] = true
			}
		}

		channel <- region

		totalFetch.Add(lastCount)
		rows.Close()
	}
}

func fetchCities(
	db *gorm.DB,
	channel chan esreindexer.FetchedRecord,
	threadsCount string,
	threadId string,
	limit string,
	countries map[string]map[string]string,
) {
	var (
		city esreindexer.GNItem
		row  esreindexer.GNCityRow

		lastId    uint64 = 0
		lastCount uint64
	)

	for {
		lastCount = 0

		rows, err := db.Raw(`
SELECT
	g.geonameid geonameid,
	g.asciiname cityasciiname,
	g.name cityname,
	g.population population,
	g.timezone timezone,
	g.latitude latitude,
	g.longitude longitude,
	g.country country,
	a_city.isoLanguage lang,
	a_city.alternateName cityalt,
	g_reg.asciiname regasciiname,
	g_reg.name regname,
	a_reg.alternateName regalt,
	g_reg.geonameid regid
FROM gn10k.geoname g
LEFT OUTER JOIN gn10k.alternatename a_city ON
	g.geonameid = a_city.geonameid AND
	a_city.isoLanguage NOT IN ('link', 'iata', 'post', 'icao', 'faac', 'fr_1793')
LEFT JOIN gn10k.admin1CodesAscii ac ON
	ac.code = CONCAT(g.country, '.', g.admin1)
LEFT JOIN gn10k.geoname g_reg ON
	g_reg.geonameid = ac.geonameid
LEFT OUTER JOIN gn10k.alternatename a_reg ON
	ac.geonameid = a_reg.geonameid AND
	a_reg.isoLanguage = a_city.isoLanguage AND
	a_city.isoLanguage NOT IN ('link', 'iata', 'post', 'icao', 'faac', 'fr_1793')
WHERE
	g.fclass = 'P' AND
	g.geonameid IN
		(SELECT * FROM /* mysql is stupid and won't allow limits in IN subqueries */
			(SELECT geonameid
			FROM gn10k.geoname g
			WHERE g.geonameid > ` + strconv.FormatUint(lastId, 10) + ` AND
				  g.geonameid % ` + threadsCount + ` = ` + threadId + `
			ORDER BY geonameid ASC
			LIMIT ` + limit + `)t
		)
ORDER BY
	geonameid ASC,
	a_city.isoLanguage ASC,
	a_city.isPreferredName DESC,
	a_city.isShortName DESC,
	a_reg.isPreferredName DESC,
	a_reg.isShortName DESC
`).Rows()

		if err != nil {
			panic(err)
		}

		for rows.Next() {
			err := db.ScanRows(rows, &row)

			if err != nil {
				panic(err)
			}

			if city.Geonameid != row.Geonameid {
				if lastCount > 0 {
					channel <- city
				}

				// Create new city for this row
				city = esreindexer.GNItem{
					Type:        "city",
					Geonameid:   row.Geonameid,
					Country:     row.Country,
					Regionid:    row.Regid,
					Population:  row.Population,
					Timezone:    row.Timezone,
					Latitude:    row.Latitude,
					Longitude:   row.Longitude,
					CityNames:   map[string]string{},
					RegionNames: map[string]string{},
					Suggestions: map[string]bool{},
				}

				lastCount++

				// Add English entries from main geoname table if first record
				cityRegCtry := row.Cityname
				if len(row.Regname) > 0 {
					cityRegCtry += " " + row.Regname
				}
				cityRegCtry += " " + countries[row.Country]["en"]
				city.Suggestions[cityRegCtry] = true

				cityCtry := row.Cityname + " " + countries[row.Country]["en"]
				city.Suggestions[cityCtry] = true

				// Add [City, Localized Country] for any lang that does not have an alt name
				for lang, name := range countries[city.Country] {
					if _, ok := city.CityNames[lang]; !ok {
						city.Suggestions[row.Cityname+" "+name] = true
					}
				}

				city.CityNames["en"] = row.Cityname
				city.RegionNames["en"] = row.Regname
			}

			// Add non-English alternate names
			if len(row.Lang) > 0 {
				if len(row.Cityalt) > 0 && row.Cityalt != row.Cityname {
					if _, ok := city.CityNames[row.Lang]; !ok {
						// City names for this language don't exist yet
						city.CityNames[row.Lang] = row.Cityalt
					}
				}
				if len(row.Regalt) > 0 && row.Regalt != row.Regname {
					if _, ok := city.RegionNames[row.Lang]; !ok {
						city.RegionNames[row.Lang] = row.Regalt
					}
				}

				// Add suggestions (["City Country", "City Region Country"])
				cityRegCtry := row.Cityalt
				if len(row.Regalt) > 0 {
					cityRegCtry += " " + row.Regalt
				} else if len(row.Regname) > 0 {
					cityRegCtry += " " + row.Regname
				}

				cityRegCtry += " " + countries[row.Country][row.Lang]
				city.Suggestions[cityRegCtry] = true

				cityCtry := row.Cityname + " " + countries[row.Country][row.Lang]
				city.Suggestions[cityCtry] = true
			}

			lastId = row.Geonameid
		}

		// If no records fetched, finish
		if lastCount == 0 {
			break
		}

		// (For last record) add [City, Localized Country] for any lang that does not have an alt name
		for lang, name := range countries[city.Country] {
			if _, ok := city.CityNames[lang]; !ok {
				city.Suggestions[row.Cityname+" "+name] = true
			}
		}

		channel <- city

		totalFetch.Add(lastCount)
		rows.Close()
	}
}
