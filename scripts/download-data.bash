#!/bin/bash

set -e

cd `dirname $0`
cd ../data

for year in `seq 1997 2016`;do
    wget ftp://ftp.ncdc.noaa.gov/pub/data/gsod/$year/gsod_$year.tar -O gsod_$year.tar
done
