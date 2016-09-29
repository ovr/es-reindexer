// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	"encoding/json"
)

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

func (GeoAlternateName) TableName() string {
	return "alternatename"
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

	AlternativeNames []GeoAlternateName `gorm:"ForeignKey:Geonameid;AssociationForeignKey:Geonameid"`
}

func (this GeoName) GetId() uint64 {
	return this.Geonameid
}

func (this GeoName) GetAlternativeNames() []GeoAlternateName {
	return append(this.AlternativeNames, GeoAlternateName{Language: "en", Name: this.Name, IsPreferredName: true})
}


func (this GeoName) GetLocalizationNames() GeoAlternateNamesMap {
	result := GeoAlternateNamesMap{
		"en": this.Name,
	}

	for _, alterName := range this.AlternativeNames {
		if alterName.Language == "link" {
			continue
		}

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
	};
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
	};
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
	RegionNames string `gorm:"column:region_names" json:"region_names"`
	RegionAlternatenames string `gorm:"column:region_alternatenames" json:"region_alternatenames"`
}

func (this GNObjectAggregate) GetId() uint64 {
	return this.Id
}

type GeoAlternateNamesMap map[string]string

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

	var regionNames JSONMap
	err = json.Unmarshal([]byte(this.RegionNames), &regionNames)
	if err != nil {
		//panic(err)
	}

	result["region_names"] = names

	var regionAlternateNames JSONMap
	err = json.Unmarshal([]byte(this.RegionAlternatenames), &regionAlternateNames)
	if err != nil {
		//panic(err)
	}

	result["region_alternatenames"] = regionAlternateNames

	return result
}
