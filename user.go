// Copyright 2016-present InterPals. All Rights Reserved.

package esreindexer

import (
	"strconv"
	"strings"
)

type Known struct {
	UserId uint64 `json:"-"`
	Level  uint8  `json:"level"`
	Lang   string `json:"lang"`
}

func (Known) TableName() string {
	return "user_langs"
}

type Learn struct {
	UserId uint64 `json:"-"`
	Level  uint8  `json:"level"`
	Lang   string `json:"lang"`
}

func (Learn) TableName() string {
	return "user_langs_learn"
}

type User struct {
	FetchedRecord `json:"-"`

	Id            uint64 `json:"id"`
	Signup        string `json:"signup"`
	Last_login    string `json:"last_login"`
	Modified      string `json:"modified"`
	Name          string `json:"name"`
	Birth         string `json:"birth"`
	Age           uint8  `json:"age"`
	Username      string `json:"username"`
	Main_photo_id string `json:"main_photo_id"`
	Photo_exists  bool   `json:"photo_exists"`
	Main_thumb    string `json:"main_thumb"`
	Cont          string `json:"continent"`
	Sex           string `json:"sex"`
	SexBool       bool   `json:"sex_bool"`
	Tz            string `json:"tz"`

	// Legacy
	City    string `json:"city"`
	Wg_id   string `json:"wg_id"`
	Country string `json:"country"`
	Iso2    string `json:"iso2"`

	CityNameEn  string `json:"city_name_en"`
	CityId      uint64 `json:"city_id"`
	RegionId    uint64 `json:"region_id"`
	CountryCode string `json:"country_code"`

	HomeCityNameEn  string `json:"home_city_name_en"`
	HomeCityId      uint64 `json:"home_city_id"`
	HomeRegionId    uint64 `json:"home_region_id"`
	HomeCountryCode string `json:"home_country_code"`

	Lfor_friend   bool `json:"lfor_friend"`
	Lfor_langex   bool `json:"lfor_langex"`
	Lfor_relation bool `json:"lfor_relation"`
	Lfor_snail    bool `json:"lfor_snail"`
	Lfor_meet     bool `json:"lfor_meet"`

	Description string `json:"description"`
	Books       string `json:"books"`
	Hobbies     string `json:"hobbies"`
	Movies      string `json:"movies"`
	Requests    string `json:"requests"`
	Music       string `json:"music"`
	Quotes      string `json:"quotes"`
	Tv          string `json:"tv"`
	Langex_desc string `json:"langex_desc"`

	Education_level uint8  `json:"education_level"`
	Education_desc  string `json:"education_desc"`
	Occupation      string `json:"occupation"`
	Relationship    uint8  `json:"relationship"`

	Learninfo string `json:"learninfo"`
	Knowninfo string `json:"knowninfo"`

	Known []Known `json:"known"`
	Learn []Learn `json:"learn"`
}

func (User) TableName() string {
	return "users"
}

func (this User) GetId() uint64 {
	return this.Id
}

func (this User) GetSearchData() interface{} {
	return this
}

func (User) GetIndex() string {
	return "users"
}

func (User) GetType() string {
	return "users"
}

func (User) GetParent() *uint64 {
	return nil
}

func (this *User) Prepare() {
	this.SexBool = this.Sex == "female"

	if this.Learninfo != "" {
		languagesInfo := strings.Split(this.Learninfo, ",")
		for i := 0; i < len(languagesInfo); i++ {
			parts := strings.Split(languagesInfo[i], "|")

			level, err := strconv.ParseUint(parts[1], 10, 8)
			if err != nil {
				panic(err)
			}

			this.Learn = append(this.Learn, Learn{Lang: parts[0], Level: uint8(level)})
		}
	}

	if this.Knowninfo != "" {
		languagesInfo := strings.Split(this.Knowninfo, ",")
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
