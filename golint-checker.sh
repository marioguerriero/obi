#!/bin/sh

if [[ "$(golint $1)" ]]; then
	echo "errors found"
	golint $1
	exit 1
else
	echo "no errors found"
	exit 0
fi