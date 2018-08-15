#!/bin/sh

pylama -i W0401 $1

if [ $? -eq 0 ]; then
        echo "no errors found"
        exit 0
else
        echo "errors found"
        exit 1
fi
