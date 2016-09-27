// Copyright 2016-present InterPals. All Rights Reserved.

package main

type GeoAlternateName struct {
	Id              uint64  `gorm:"primary_key:true;column:alternatenameId" json:"-"`
	GeoNameid       uint64  `gorm:"column:geonameid" json:"-"`
	Language        string  `gorm:"column:isoLanguage" json:"language"`
	Name            string  `gorm:"column:alternateName" json:"name"`
	IsPreferredName bool    `gorm:"column:is_preferred_name" json:"is_preferred"`
	IsShortName     bool    `json:"-"`
	IsColloquail    bool    `json:"-"`
	IsHistoric      bool    `json:"-"`
}

func (GeoAlternateName) TableName() string {
	return "alternatename"
}

type GeoName struct {
	FetchedRecord `json:"-"`

	Geonameid        uint64 `gorm:"primary_key:true";json:"-"`

	Name             string `json:"name"`
	Asciiname        string `json:"asciiname"`

	Alternatenames   string `gorm:"column:alternatenames" json:"alternatenames"`
	LocaleNames      string `gorm:"column:localenames" json:"localenames"`

	Fclass           string `json:"fclass"`
	Fcode            string `json:"fcode"`
	Country          string `json:"country"`
	Cc2              string `json:"cc2"`
	Admin1           string `json:"admin1"`
	Admin2           string `json:"admin2"`
	Admin3           string `json:"admin3"`
	Admin4           string `json:"admin4"`
	Population       int32  `json:"population"`
	Elevation        int32  `json:"elevation"`
	Gtopo30          int32  `json:"gtopo30"`
	Timezone         string `json:"timezone"`
	Moddate          string `json:"moddate"`

	Region           string `json:"region"`

	Latitude         float32 `json:"-"`
	Longitude        float32 `json:"-"`

	Location         Location `json:"location"`

	AlternativeNames []GeoAlternateName `gorm:"ForeignKey:Geonameid;AssociationForeignKey:Geonameid"`
}

func (GeoName) TableName() string {
	return "geoname"
}

func (this GeoName) GetId() uint64 {
	return this.Geonameid
}

type GeoAlternateNamesMap map[string]string

func (this GeoName) GetLocalizationNames() GeoAlternateNamesMap {
	result := GeoAlternateNamesMap{}

	for _, alterName := range this.AlternativeNames {
		if alterName.Language == "link" {
			continue;
		}

		if alterName.Language == "" {
			continue;
		}

		result[alterName.Language] = alterName.Name;
	}

	return result
}

func (this GeoName) Prepare() {
	this.Location.Lat = this.Latitude
	this.Location.Lon = this.Longitude
}
