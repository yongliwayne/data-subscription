#!/usr/bin/env bash

for i in `ps -ef | grep "-e $1 -s $2 -m $3 -f $4" | grep data.py | awk '{print $2}'`;
    do kill ${i};
done