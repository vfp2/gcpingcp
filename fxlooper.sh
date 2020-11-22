#!/bin/bash

i=0
while [ $i -lt 8141 ]
    do
        for j in {1..6};
            do
                node --no-warnings fxinserter.js $i 2003-01-01&
                i=$((i+1))
            done
    wait < <(jobs -p)
    done
