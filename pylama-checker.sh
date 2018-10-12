#!/bin/sh

pylama $1 --skip master/heartbeat/sender_dataproc.py

if [ $? -eq 0 ]; then
        echo "no errors found"
        exit 0
else
        echo "errors found"
        exit 1
fi
