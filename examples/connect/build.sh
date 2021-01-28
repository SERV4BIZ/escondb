#!/bin/sh

NAMEARM="app.arm"
NAMELINUX="app.linux"
NAMEFREEBSD="app.freebsd"
NAMEWINDOWS="app.windows"
NAMEDARWIN="app.darwin"

rm -f $NAMEARM
export GOOS=linux
export GOARCH=arm64
go build -o $NAMEARM

rm -f $NAMELINUX
export GOOS=linux
export GOARCH=amd64
go build -o $NAMELINUX

export GOOS=freebsd
export GOARCH=amd64
rm -f $NAMEFREEBSD
go build -o $NAMEFREEBSD

export GOOS=windows
export GOARCH=amd64
rm -f $NAMEWINDOWS
go build -o $NAMEWINDOWS

export GOOS=darwin
export GOARCH=amd64
rm -f $NAMEDARWIN
go build -o $NAMEDARWIN