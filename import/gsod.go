package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/crvv/days/import/utility"
	"github.com/jackc/pgx"
)

type File struct {
	name string
	data []byte
}

func main() {
	dataDir := flag.String("data", "data", "where to store gsod data")
	year := flag.Int("year", 1997, "which year will be import")
	flag.Parse()
	url := fmt.Sprintf("ftp://ftp.ncdc.noaa.gov/pub/data/gsod/%[1]v/gsod_%[1]v.tar", *year)
	data := utility.Download(url)

	fileChan := make(chan File)
	go getFiles(data, fileChan)
	saveGSOD(fileChan, *dataDir)
}

func getFiles(data []byte, output chan File) {
	tarReader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			close(output)
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		if !strings.HasSuffix(header.Name, ".gz") {
			continue
		}
		name := filepath.Base(header.Name)
		name = name[:len(name)-3]
		gzReader, err := gzip.NewReader(tarReader)
		if err != nil {
			log.Fatal(err)
		}
		data, err := ioutil.ReadAll(gzReader)
		if err != nil {
			log.Fatal(err)
		}
		output <- File{
			name: name,
			data: data,
		}
	}
}

const GSODTableSQL = `
CREATE TABLE IF NOT EXISTS gsod_availability (
    station_id        TEXT     NOT NULL,
    year              SMALLINT NOT NULL,
    mean_temperature  SMALLINT,
    max_temperature   SMALLINT,
    min_temperature   SMALLINT,
    mean_dew_point    SMALLINT,
    mean_sea_pressure SMALLINT,
    mean_pressure     SMALLINT,
    mean_visibility   SMALLINT,
    mean_wind_speed   SMALLINT,
    precipitation     SMALLINT,
    count             SMALLINT,
    PRIMARY KEY (station_id, year)
)`

func saveGSOD(files chan File, dataDir string) {
	conn, err := pgx.Connect(pgx.ConnConfig{Database: "days"})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	tx, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}
	_, err = tx.Exec(GSODTableSQL)
	if err != nil {
		log.Fatal(err)
	}
	for file := range files {
		saveStation(file, tx, dataDir)
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

var filenameRe = regexp.MustCompile(`^(\d{6}-\d{5})-(\d{4})\.op$`)

func saveStation(file File, tx *pgx.Tx, dataDir string) {
	match := filenameRe.FindStringSubmatch(file.name)
	stationId := match[1]
	lines := bytes.Split(file.data, []byte("\n"))
	records := make([]*Line, 0, 400)
	for _, line := range lines {
		if len(line) == 0 || bytes.HasPrefix(line, []byte("STN--- WBAN")) {
			continue
		}
		records = append(records, parseLine(line))
	}
	count := checkDataValid(tx, stationId, records)
	if count > 350 {
		err := ioutil.WriteFile(filepath.Join(dataDir, file.name), file.data, 0440)
		if err != nil {
			log.Fatal(err)
		}
	}
}

type Line struct {
	date            string
	MeanTemperature interface{}
	MeanDewPoint    interface{}
	MeanSeaPressure interface{}
	MeanPressure    interface{}
	MeanVisibility  interface{}
	MeanWindSpeed   interface{}
	MaxTemperature  interface{}
	MinTemperature  interface{}
	Precipitation   interface{}
}

func parseLine(lineStr []byte) *Line {
	var line Line
	line.date = string(lineStr[14:22])
	line.MeanTemperature = checkNullAndConvert(lineStr[24:30], "9999.9", Fahrenheit)
	line.MeanDewPoint = checkNullAndConvert(lineStr[35:41], "9999.9", Fahrenheit)
	line.MeanSeaPressure = checkNullAndConvert(lineStr[46:52], "9999.9", Millibar)
	line.MeanPressure = checkNullAndConvert(lineStr[57:63], "9999.9", Millibar)
	line.MeanVisibility = checkNullAndConvert(lineStr[68:73], "999.9", Mile)
	line.MeanWindSpeed = checkNullAndConvert(lineStr[78:83], "999.9", Knot)
	line.MaxTemperature = checkNullAndConvert(lineStr[102:108], "9999.9", Fahrenheit)
	line.MinTemperature = checkNullAndConvert(lineStr[110:116], "9999.9", Fahrenheit)
	line.Precipitation = checkNullAndConvert(lineStr[118:123], "99.99", Inch)
	return &line
}

const (
	Fahrenheit = iota
	Mile
	Knot
	Inch
	Millibar
)

func checkNullAndConvert(value []byte, nullValue string, unit byte) interface{} {
	str := strings.TrimSpace(string(value))
	if str == nullValue {
		return nil
	}
	v, err := strconv.ParseFloat(str, 64)
	if err != nil {
		log.Fatal(err)
	}
	switch unit {
	case Fahrenheit:
		return (v - 32) / 9 * 5
	case Mile:
		return v * 1609.34
	case Knot:
		return v * 0.514444
	case Inch:
		if v == 0 {
			return nil
		}
		return v * 25.4
	case Millibar:
		return v * 100
	}
	log.Fatal("wrong unit")
	return nil
}

func checkDataValid(tx *pgx.Tx, stationId string, records []*Line) int16 {
	total := int16(len(records))
	year := records[0].date[:4]
	nilCount := make(map[string]int16)
	lineType := reflect.TypeOf(*records[0])
	var fields []string
	for i := 0; i < lineType.NumField(); i++ {
		field := lineType.Field(i).Name
		if field == "date" {
			continue
		}
		fields = append(fields, field)
	}
	for _, record := range records {
		value := reflect.ValueOf(*record)
		for _, field := range fields {
			if value.FieldByName(field).IsNil() {
				nilCount[field]++
			}
		}
	}
	var availability Line
	availabilityValue := reflect.ValueOf(&availability)
	for _, field := range fields {
		availabilityValue.Elem().FieldByName(field).Set(reflect.ValueOf(total - nilCount[field]))
	}
	insertAvailability(tx, stationId, year, total, &availability)
	return total
}

func insertAvailability(tx *pgx.Tx, stationId string, year string, count int16, line *Line) {
	sql := "INSERT INTO gsod_availability (station_id, year, count, mean_temperature, " +
		"max_temperature, min_temperature, mean_dew_point, mean_sea_pressure, " +
		"mean_pressure, mean_visibility, mean_wind_speed, precipitation) " +
		"SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12"
	_, err := tx.Exec(sql, stationId, year, count, line.MeanTemperature, line.MaxTemperature,
		line.MinTemperature, line.MeanDewPoint, line.MeanSeaPressure, line.MeanPressure,
		line.MeanVisibility, line.MeanWindSpeed, line.Precipitation)
	if err != nil {
		log.Fatal(err)
	}
}
