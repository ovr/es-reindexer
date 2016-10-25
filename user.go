// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
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
	FetchedRecord `json:"-"`

	Id            uint64 `json:"-"`
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
	Iso2          string `json:"iso2"`

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

	Learninfo     string `json:"-"`
	Knowninfo     string `json:"-"`

	Known         []Known `json:"known"`
	Learn         []Learn `json:"learn"`
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

func (this User) Prepare() {
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
