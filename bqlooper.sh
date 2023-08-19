#!/bin/bash

i=0
while [ $i -lt 996 ]
    do
        for j in {1..6};
            do
                node --no-warnings bqinserter.js $i 2020-11-27&
                i=$((i+1))
            done
    wait < <(jobs -p)
    done
