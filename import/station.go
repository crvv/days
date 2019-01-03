package main

import (
	"bytes"
	"log"
	"strings"

	"github.com/crvv/days/import/utility"
	"github.com/jackc/pgx"
)

type Station struct {
	ID        string
	Name      string
	Country   string
	ICAO      string
	Latitude  string
	Longitude string
	Elevation string
}

func main() {
	const StationsURL = "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt"
	stationsData := utility.Download(StationsURL)
	log.Println("parse response")
	stations := parseStations(stationsData)
	log.Println("completed")
	saveStations(stations)
}

func parseStations(data []byte) []Station {
	lines := bytes.Split(data, []byte("\n"))
	for i := 0; i < len(lines); i++ {
		if bytes.HasPrefix(lines[i], []byte("USAF   WBAN")) {
			lines = lines[i+1:]
			break
		}
	}
	stations := make([]Station, 0, len(lines))
	for _, lineBytes := range lines {
		if len(lineBytes) == 0 {
			continue
		}
		line := string(lineBytes)
		var station Station
		station.ID = line[0:6] + "-" + line[7:12]
		station.Name = strings.TrimSpace(line[13:43])
		station.Country = strings.TrimSpace(line[43:48])
		station.ICAO = strings.TrimSpace(line[51:57])
		station.Latitude = strings.TrimSpace(line[57:65])
		station.Longitude = strings.TrimSpace(line[65:74])
		station.Elevation = strings.TrimSpace(line[74:82])

		stations = append(stations, station)
	}
	return stations
}

const StationTableSQL = `
DROP TABLE IF EXISTS stations;

CREATE TABLE stations (
    id         TEXT PRIMARY KEY,
    name       TEXT,
    country    TEXT,
    icao       TEXT,
    coordinate POINT,
    elevation  REAL)
`

const StationIndexSQL = `
CREATE INDEX ON stations (icao);
CREATE INDEX ON stations (name);
CREATE INDEX ON stations USING GIST (coordinate);
`

func saveStations(stations []Station) {
	conn, err := pgx.Connect(pgx.ConnConfig{
		Database: "days",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	tx, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}
	_, err = tx.Exec(StationTableSQL)
	if err != nil {
		log.Fatal(err)
	}
	for _, station := range stations {
		sql := "INSERT INTO stations (id, name, country, icao, coordinate, elevation) " +
			"VALUES ($1, $2, $3, $4, point($5, $6), $7)"
		strArgs := []string{station.ID, station.Name, station.Country, station.ICAO,
			station.Latitude, station.Longitude, station.Elevation}
		args := make([]interface{}, len(strArgs))
		for i, v := range strArgs {
			if v == "" {
				args[i] = nil
			} else {
				args[i] = v
			}
		}
		_, err := tx.Exec(sql, args...)
		if err != nil {
			log.Fatal(err)
		}
	}
	_, err = tx.Exec(StationIndexSQL)
	if err != nil {
		log.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
