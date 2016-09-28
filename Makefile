
migration:
	go build migration.go model.go geoname.go

build:
	go build main.go model.go geoname.go user.go