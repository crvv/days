#!/bin/bash

set -e

cd `dirname $0`
cd ..

dropdb days || echo 'create database days'
createdb days
go run import/city.go
go run import/station.go
go run import/gsod.go
