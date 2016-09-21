// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

type Known struct {
	UserId uint64
	Level  uint8  `json:"level"`
	Lang   string `json:"lang"`
}

func (Known) TableName() string {
	return "user_langs"
}

type Learn struct {
	UserId uint64
	Level  uint8  `json:"level"`
	Lang   string `json:"lang"`
}

func (Learn) TableName() string {
	return "user_langs_learn"
}

type User struct {
	FetchedRecord

	Id            uint64
	Last_login    string `json:"last_login"`
	Modified      string `json:"modified"`
	Name          string `json:"name"`
	Birth         string `json:"birth"`
	Age           uint8  `json:"age"`
	Username      string `json:"username"`
	Main_photo_id string `json:"main_photo_id"`
	Photo_exists  bool   `json:"photo_exists"`
	Main_thumb    string `json:"photo_exists"`
	Cont          string `json:"continent"`
	Sex           string `json:"sex"`
	SexBool       bool   `json:"sex_bool"`
	Tz            string `json:"tz"`
	Сity          string `json:"city"`
	Country       string `json:"country"`

	Lfor_email    bool `json:"lfor_email"`
	Lfor_flirt    bool `json:"lfor_flirt"`
	Lfor_friend   bool `json:"lfor_friend"`
	Lfor_langex   bool `json:"lfor_langex"`
	Lfor_relation bool `json:"lfor_relation"`
	Lfor_snail    bool `json:"lfor_snail"`

	Description   string `json:"description"`
	Books         string `json:"books"`
	Hobbies       string `json:"hobbies"`
	Movies        string `json:"movies"`
	Requests      string `json:"requests"`
	Music         string `json:"music"`
	Quotes        string `json:"quotes"`
	Tv            string `json:"tv"`
	Langex_desc   string `json:"langex_desc"`

	Learninfo     string
	Knowninfo     string

	Known         []Known `json:"known"`
	Learn         []Learn `json:"learn"`
}

func (User) TableName() string {
	return "users"
}

func (this User) GetId() uint64 {
	return this.Id
}

func (this User) Prepare() {
	this.SexBool = this.Sex == "female"

	if this.Learninfo != "" {
		languagesInfo := strings.Split(this.Learninfo, ",")
		for i := 0; i < len(languagesInfo); i++ {
			parts := strings.Split(languagesInfo[0], "|")

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
			parts := strings.Split(languagesInfo[0], "|")

			level, err := strconv.ParseUint(parts[1], 10, 8)
			if err != nil {
				panic(err)
			}

			this.Known = append(this.Known, Known{Lang: parts[0], Level: uint8(level)})
		}
	}
}

type ElasticSearchConfig struct {
	Uri   string `json:"uri"`
	Limit uint16 `json:"limit"`
}

type DataBaseConfig struct {
	Dialect            string `json:"dialect"`
	Uri                string `json:"uri"`
	MaxIdleConnections int    `json:"max-idle-connections"`
	MaxOpenConnections int    `json:"max-open-connections"`
	ShowLog            bool   `json:"log"`
	Threads            uint8  `json:"threads"`
	Limit              uint16 `json:"limit"`
}

type Configuration struct {
	ElasticSearch     ElasticSearchConfig `json:"elasticsearch"`
	DataBase          DataBaseConfig      `json:"db"`
	ChannelBufferSize int                 `json:"channel-buffer-size"`
}

func (this *Configuration) Init(configFile string) {
	configJson, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = json.Unmarshal(configJson, &this)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type Location struct {
	Lat float32 `json:"lat"`
	Lon float32 `json:"lon"`
}

type FetchedRecord interface {
	GetId() uint64
}

type GeoName struct {
	FetchedRecord

	Geonameid      uint64

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
	Population     int32 `json:"population"`
	Elevation      int32 `json:"elevation"`
	Gtopo30        int32 `json:"gtopo30"`
	Timezone       string `json:"timezone"`
	Moddate        string `json:"moddate"`

	Latitude       float32
	Longitude      float32

	Location       Location
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