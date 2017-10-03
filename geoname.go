// Copyright 2016-present InterPals. All Rights Reserved.

package esreindexer

import (
	"encoding/json"
	"strconv"
	"strings"
)

type GNItem struct {
	FetchedRecord `json:"-"`

	Type         string
	Geonameid    uint64
	Country      string
	Regionid     string
	Population   int32
	Timezone     string
	Latitude     float32
	Longitude    float32
	CityNames    map[string]string
	RegionNames  map[string]string
	CountryNames map[string]string
	Suggestions  map[string]bool
}

type GNCountryRow struct {
	Geonameid  uint64
	Country    string
	Name       string
	Lang       string
	Timezone   string
	Latitude   float32
	Longitude  float32
	Population int32
}

type GNRegionRow struct {
	Geonameid  uint64
	Name       string
	Asciiname  string
	Altname    string
	Lang       string
	Timezone   string
	Latitude   float32
	Longitude  float32
	Population int32
	Country    string
}

type GNCityRow struct {
	Geonameid     uint64
	Cityname      string
	Cityasciiname string
	Cityalt       string
	Lang          string
	Regname       string
	Regasciiname  string
	Regalt        string
	Regid         string
	Timezone      string
	Latitude      float32
	Longitude     float32
	Population    int32
	Country       string
}

type GeoAlternateName struct {
	Id              uint64 `gorm:"primary_key:true;column:alternatenameId" json:"-"`
	GeoNameid       uint64 `gorm:"column:geonameid" json:"-"`
	Language        string `gorm:"column:isoLanguage" json:"language"`
	Name            string `gorm:"column:alternateName" json:"name"`
	IsPreferredName bool   `gorm:"column:is_preferred_name" json:"is_preferred"`
	IsShortName     bool   `json:"-"`
	IsColloquail    bool   `json:"-"`
	IsHistoric      bool   `json:"-"`
}

type GeoName struct {
	FetchedRecord `json:"-"`

	Geonameid uint64 `gorm:"primary_key:true";json:"-"`

	Name      string `json:"name"`
	Asciiname string `json:"asciiname"`

	Alternatenames string `gorm:"column:alternatenames" json:"-"`
	LocaleNames    string `gorm:"column:localenames" json:"-"`

	Fclass     string `json:"fclass"`
	Fcode      string `json:"fcode"`
	Country    string `json:"country"`
	Cc2        string `json:"cc2"`
	Admin1     string `json:"admin1"`
	Admin2     string `json:"admin2"`
	Admin3     string `json:"admin3"`
	Admin4     string `json:"admin4"`
	Population uint32 `json:"population"`
	Elevation  int32  `json:"elevation"`
	Gtopo30    int32  `json:"gtopo30"`
	Timezone   string `json:"timezone"`
	Moddate    string `json:"moddate"`

	Region string `json:"region"`

	Latitude  float32 `json:"-"`
	Longitude float32 `json:"-"`

	Location Location `json:"location"`

	AlternativeNamesAsString string             `gorm:"column:alternativenames_as_string"`
	AlternativeNames         []GeoAlternateName `gorm:"ForeignKey:Geonameid;AssociationForeignKey:Geonameid"`
}

func (this GeoName) GetId() uint64 {
	return this.Geonameid
}

func (this *GeoName) Prepare() {
	this.AlternativeNames = []GeoAlternateName{}

	if this.AlternativeNamesAsString != "" {
		languagesInfo := strings.Split(this.AlternativeNamesAsString, "|")
		for i := 0; i < len(languagesInfo); i++ {
			parts := strings.Split(languagesInfo[i], ",")
			partsLen := len(parts)

			language := ""
			if partsLen >= 2 {
				language = parts[1]
			}

			isPreferredName := false
			if partsLen == 3 {
				isPreferredName = parts[2] == "1"
			}

			this.AlternativeNames = append(this.AlternativeNames, GeoAlternateName{
				Name:            parts[0],
				Language:        language,
				IsPreferredName: isPreferredName,
			})
		}
	}

	this.AlternativeNames = append(this.AlternativeNames, GeoAlternateName{
		Name:            this.Name,
		Language:        "en",
		IsPreferredName: true,
	})
}

func (this GeoName) GetLocalizationNames() JSONMap {
	result := JSONMap{}

	for _, alterName := range this.AlternativeNames {
		if alterName.Language == "" {
			continue
		}

		result[alterName.Language] = alterName.Name
	}

	return result
}

func (GeoName) TableName() string {
	return "geoname"
}

type GNObjectIterface interface {
	TableName() string
	GetValues() []interface{}
}

type GNObject struct {
	GNObjectIterface `json:"-"`

	Id         uint64 `gorm:"primary_key:true;column:id"`
	Names      string
	Latitude   float32
	Longitude  float32
	Population uint32
	Iso        string
	Timezone   string
	RegionId   *uint64 `gorm:"column:region_id"`
}

func (GNObject) TableName() string {
	return "gn_object"
}

func (this GNObject) GetValues() []interface{} {
	return []interface{}{
		this.Id,
		this.Names,
		this.Latitude,
		this.Longitude,
		this.Population,
		this.Iso,
		this.Timezone,
		this.RegionId,
	}
}

type GNObjectAlternateNames struct {
	GNObjectIterface `json:"-"`

	Id    uint64 `gorm:"primary_key:true;column:id"`
	Names string `gorm:"column:alternatenames"`
}

func (this GNObjectAlternateNames) GetValues() []interface{} {
	return []interface{}{
		this.Id,
		this.Names,
	}
}

func (GNObjectAlternateNames) TableName() string {
	return "gn_object_alternatenames"
}

type GeoAdmin1Code struct {
	Code      string
	Name      string
	NameAscii string `gorm:"column:nameascii"`
	Geonameid uint64 `gorm:"column:geonameid"`
}

func (GeoAdmin1Code) TableName() string {
	return "admin1CodesAscii"
}

type GNObjectAggregate struct {
	FetchedRecord `json:"-"`

	Id         uint64 `gorm:"primary_key:true;column:id"`
	Names      string
	Latitude   float32
	Longitude  float32
	Population uint32
	Iso        string
	Timezone   string
	RegionId   *uint64 `gorm:"column:region_id"`

	// Virtual from Left Joins
	Alternatenames string `gorm:"column:alternatenames" json:"alternatenames"`
}

func (GNObjectAggregate) GetIndex() string {
	return "geonames"
}

func (this GNObjectAggregate) GetType() string {
	if this.RegionId != nil {
		return "city"
	}

	return "region"
}

func (this GNObjectAggregate) GetId() uint64 {
	return this.Id
}

func (this GNObjectAggregate) GetParent() *uint64 {
	return this.RegionId
}

type JSONMap map[string]interface{}

func (this GNObjectAggregate) GetSearchData() interface{} {
	result := JSONMap{}

	result["iso"] = this.Iso
	result["population"] = this.Population
	result["timezone"] = this.Timezone
	result["region_id"] = this.RegionId
	result["location"] = &JSONMap{
		"lat": this.Latitude,
		"lon": this.Longitude,
	}

	var alternatenames []JSONMap
	err := json.Unmarshal([]byte(this.Alternatenames), &alternatenames)
	if err != nil {
		//panic(err)
	}

	result["alternatenames"] = alternatenames

	var names JSONMap
	err = json.Unmarshal([]byte(this.Names), &names)
	if err != nil {
		//panic(err)
	}

	result["names"] = names

	return result
}

func (GNItem) GetIndex() string {
	return "geo"
}

func (this GNItem) GetType() string {
	return this.Type
}

func (this GNItem) GetId() uint64 {
	return this.Geonameid
}

func (this GNItem) GetParent() *uint64 {
	return nil
}

func (this GNItem) GetSearchData() interface{} {
	result := JSONMap{
		"country_iso2": this.Country,
		"location": strconv.FormatFloat(float64(this.Latitude), 'f', -1, 64) + "," +
			strconv.FormatFloat(float64(this.Longitude), 'f', -1, 64),
		"population": strconv.Itoa(int(this.Population)),
		"suggest":    []string{},
		"timezone":   this.Timezone,
	}

	for lang, name := range this.CityNames {
		result["city_"+lang] = name
	}

	for lang, name := range this.RegionNames {
		result["region_"+lang] = name
	}

	for lang, name := range this.CountryNames {
		result["country_"+lang] = name
	}

	for sug, _ := range this.Suggestions {
		result["suggest"] = append(result["suggest"].([]string), sug)
	}

	if this.Type == "city" {
		result["regionid"] = this.Regionid
	}

	return result
}
