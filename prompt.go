// Copyright 2016-present InterPals. All Rights Reserved.

package esreindexer

type Prompt struct {
	FetchedRecord `json:"-"`

	Id  uint64 `gorm:"primary_key:true";json:"id"`
	Uid uint64 `json:"uid"`

	Language string `json:"name"`
	Data     string `json:"data"`
	Created  string `json:"created"`
}

func (this Prompt) GetId() uint64 {
	return this.Id
}

func (this *Prompt) Prepare() {
}

func (Prompt) TableName() string {
	return "le_prompts"
}

func (this Prompt) GetSearchData() interface{} {
	result := JSONMap{}

	result["id"] = this.Id
	result["uid"] = this.Uid
	result["language"] = this.Language
	result["data"] = this.Data
	result["created"] = this.Created

	return result
}
