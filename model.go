// Copyright 2016-present InterPals. All Rights Reserved.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Location struct {
	Lat float32 `json:"lat"`
	Lon float32 `json:"lon"`
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

type FetchedRecord interface {
	GetId() uint64
}

type MetaDataES interface {
	GetIndex() string
	GetType() string
}

type MetaDataESUsers struct {
	MetaDataES
}

func (MetaDataESUsers) GetIndex() string {
	return "users";
}

func (MetaDataESUsers) GetType() string {
	return "users";
}

type MetaDataESGeoNames struct {
	MetaDataES
}

func (MetaDataESGeoNames) GetIndex() string  {
	return "geonames";
}

func (MetaDataESGeoNames) GetType() string {
	return "geonames";
}
