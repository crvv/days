package main

import (
	"archive/zip"
	"bytes"
	"io/ioutil"
	"log"
	"strings"

	"github.com/crvv/days/import/utility"
	"github.com/jackc/pgx"
)

const CitiesURL = "http://download.geonames.org/export/dump/cities5000.zip"
const NamesURL = "http://download.geonames.org/export/dump/alternateNames.zip"

type Name struct {
	Names []AlternateName
	Links []string
}

type AlternateName struct {
	Name     string `json:"name"`
	Language string `json:"language"`
}

type City struct {
	ID         string
	Name       string
	Latitude   string
	Longitude  string
	Elevation  string
	Class      string
	Country    string
	Population string
	Timezone   string
}

func main() {
	citiesZip := utility.Download(CitiesURL)
	namesZip := utility.Download(NamesURL)

	log.Println("parse response")
	cities := readCities(citiesZip)
	names := readNames(namesZip)
	log.Println("completed")

	saveCities(cities, names)
}

func readZipFile(data []byte, filename string) []byte {
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range z.File {
		if f.Name == filename {
			reader, err := f.Open()
			if err != nil {
				log.Fatal(err)
			}
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Fatal(err)
			}
			return result
		}
	}
	log.Fatalf("can't find %v in zip data", filename)
	return nil
}

func readCities(data []byte) []City {
	data = readZipFile(data, "cities5000.txt")
	lines := bytes.Split(data, []byte("\n"))
	cities := make([]City, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		fields := strings.Split(string(line), "\t")
		var city City

		city.ID = fields[0]
		city.Name = fields[1]
		city.Latitude = fields[4]
		city.Longitude = fields[5]
		city.Class = fields[6]
		city.Country = fields[8]
		city.Population = fields[14]
		city.Elevation = fields[16]
		city.Timezone = fields[17]
		if city.Class != "P" {
			log.Println(city)
			continue
		}

		cities = append(cities, city)
	}
	return cities
}

func readNames(data []byte) map[string]Name {
	data = readZipFile(data, "alternateNames.txt")
	lines := bytes.Split(data, []byte("\n"))
	names := make(map[string]Name)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		fields := strings.Split(string(line), "\t")

		id := fields[1]
		language := fields[2]
		nameStr := fields[3]
		name := names[id]

		if language == "link" {
			name.Links = append(name.Links, nameStr)
		} else {
			name.Names = append(name.Names, AlternateName{
				Name:     nameStr,
				Language: language,
			})
		}
		names[id] = name
	}
	return names
}

const CityTableSQL = `
DROP TABLE IF EXISTS cities;

CREATE TABLE cities (
    id              BIGINT PRIMARY KEY,
    station_id      TEXT,
    name            TEXT   NOT NULL,
    country         TEXT   NOT NULL,
    alternate_names JSONB  NOT NULL,
    link            JSONB  NOT NULL,
    coordinate      POINT,
    elevation       REAL   NOT NULL,
    population      BIGINT NOT NULL,
    timezone        TEXT   NOT NULL);
`

const CityIndexSQL = `
CREATE INDEX ON cities (station_id);
CREATE INDEX ON cities (name);
CREATE INDEX ON cities USING GIN (alternate_names jsonb_path_ops);
CREATE INDEX ON cities USING GIST (coordinate);
`

func saveCities(cities []City, names map[string]Name) {
	conn, err := pgx.Connect(pgx.ConnConfig{Database: "days"})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	tx, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}
	_, err = tx.Exec(CityTableSQL)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("created table")
	for i, city := range cities {
		name := names[city.ID]
		sql := "INSERT INTO cities (id, name, link, alternate_names, coordinate, elevation, " +
			"country, population, timezone) VALUES ($1, $2, $3, $4, point($5, $6), $7, $8, $9, $10)"
		_, err := tx.Exec(sql, city.ID, city.Name, name.Links, name.Names,
			city.Latitude, city.Longitude, city.Elevation,
			city.Country, city.Population, city.Timezone)
		if err != nil {
			log.Fatal(err)
		}
		if (i+1)%1000 == 0 {
			log.Println("inserted", i+1, "cities")
		}
	}
	log.Println("create index")
	_, err = tx.Exec(CityIndexSQL)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("commit")
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
