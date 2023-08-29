#!/bin/bash

i=0
while [ $i -lt 248 ]
    do
        for j in {1..6};
            do
                node --no-warnings bqinserter.js $i 2022-12-20&
                i=$((i+1))
            done
    wait < <(jobs -p)
    done
