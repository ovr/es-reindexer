// Copyright 2016-present InterPals. All Rights Reserved.

package main

type GeoName struct {
	FetchedRecord

	Geonameid      uint64 `json:"-"`

	Name           string `json:"name"`
	Asciiname      string `json:"asciiname"`
	Alternatenames string `json:"alternatenames"`
	Fclass         string `json:"fclass"`
	Fcode          string `json:"fcode"`
	Country        string `json:"country"`
	Cc2            string `json:"cc2"`
	Admin1         string `json:"admin1"`
	Admin2         string `json:"admin2"`
	Admin3         string `json:"admin3"`
	Admin4         string `json:"admin4"`
	Population     int32  `json:"population"`
	Elevation      int32  `json:"elevation"`
	Gtopo30        int32  `json:"gtopo30"`
	Timezone       string `json:"timezone"`
	Moddate        string `json:"moddate"`

	Region         string `json:"region"`

	Latitude       float32 `json:"-"`
	Longitude      float32 `json:"-"`

	Location       Location `json:"location"`
}

func (GeoName) TableName() string {
	return "geoname"
}

func (this GeoName) GetId() uint64 {
	return this.Geonameid
}

func (this GeoName) Prepare() {
	this.Location.Lat = this.Latitude
	this.Location.Lon = this.Longitude
}
