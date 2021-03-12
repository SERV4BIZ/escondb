#!/bin/sh

export GO111MODULE=auto
RUNNAME="app.darwin"

clear
./build.sh
./$RUNNAME