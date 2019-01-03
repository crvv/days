#!/bin/bash

pg_dump -ddays -t cities -t stations -t gsod_availability | zstd > dump.sql.zstd
