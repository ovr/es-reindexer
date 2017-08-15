// Copyright 2016-present InterPals. All Rights Reserved.

package esreindexer

import (
	"strconv"
	"strings"
)

type Trip struct {
	Id            uint64  `gorm:"primary_key:true;column:id" json:"id"`
	OwnerId       uint64  `gorm:"column:owner_id" json:"owner_id"`
	DestinationId uint64  `gorm:"column:destination_id" json:"destination_id"`
	Latitude      float64 `gorm:"column:latitude" json:"latitude"`
	Longitude     float64 `gorm:"column:longitude" json:"longitude"`
	ArrivalDate   string  `gorm:"column:arrival_date" json:"arrival_date"`
	DepartureDate string  `gorm:"column:departure_date" json:"departure_date"`
	Description   string  `gorm:"column:description" json:"description"`
	MaxTravelers  uint64  `gorm:"column:max_travelers" json:"max_travelers"`
	Acl           uint64  `gorm:"column:acl" json:"acl"`
	Open          uint64  `gorm:"column:open" json:"open"`
	Created       string  `gorm:"column:created_at" json:"created_at"`
	City          string  `gorm:"column:city" json:"city"`
	Country       string  `gorm:"column:country" json:"country"`
	TripDays      uint64  `gorm:"column:trip_days" json:"trip_days"`
	Age           uint64  `gorm:"column:age" json:"age"`
	Sex           string  `gorm:"column:sex" json:"sex"`
	CountryHome   string  `gorm:"column:country_home" json:"country_home"`
	KnownInfo     string  `gorm:"column:knowninfo" json:"knowninfo"`
	Known         []Known `json:"known"`
	SexBool       bool    `json:"sex_bool"`
}

func (this *Trip) Prepare() {
	this.SexBool = this.Sex == "female"

	if this.KnownInfo != "" {
		languagesInfo := strings.Split(this.KnownInfo, ",")
		for i := 0; i < len(languagesInfo); i++ {
			parts := strings.Split(languagesInfo[i], "|")

			level, err := strconv.ParseUint(parts[1], 10, 8)
			if err != nil {
				panic(err)
			}

			this.Known = append(this.Known, Known{Lang: parts[0], Level: uint8(level)})
		}
	}
}

func (this Trip) GetType() string {
	return "trip"
}

func (this Trip) GetSearchData() interface{} {
	result := JSONMap{}

	result["id"] = this.Id
	result["owner_id"] = this.OwnerId
	result["destination_id"] = this.DestinationId
	result["location"] = &JSONMap{
		"lat": this.Latitude,
		"lon": this.Longitude,
	}
	result["arrival_date"] = this.ArrivalDate
	result["departure_date"] = this.DepartureDate
	result["description"] = this.Description
	result["max_travelers"] = this.MaxTravelers
	result["acl"] = this.Acl
	result["created"] = this.Created
	result["city"] = this.City
	result["country"] = this.Country
	result["trip_days"] = this.TripDays
	result["age"] = this.Age
	result["sex"] = this.Sex
	result["country_home"] = this.CountryHome
	result["knowninfo"] = this.KnownInfo

	return result
}

func (this Trip) GetValues() []interface{} {
	return []interface{}{
		this.Id,
		this.OwnerId,
		this.DestinationId,
		this.Latitude,
		this.Longitude,
		this.ArrivalDate,
		this.DepartureDate,
		this.Description,
		this.MaxTravelers,
		this.Acl,
		this.Open,
		this.Created,
		this.City,
		this.Country,
		this.TripDays,
		this.Age,
		this.Sex,
		this.CountryHome,
		this.KnownInfo,
	}
}

func (this Trip) GetParent() *uint64 {
	return nil
}

func (Trip) GetIndex() string {
	return "trips"
}

func (this Trip) GetId() uint64 {
	return this.Id
}

func (Trip) TableName() string {
	return "trips"
}
