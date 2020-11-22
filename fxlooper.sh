#!/bin/bash

i=0
while [ $i -lt 8141 ]
    do
        for j in {1..1};
            do
                node --no-warnings fxinserter.js $i 2003-05-05&
                i=$((i+1))
            done
    wait < <(jobs -p)
    done
