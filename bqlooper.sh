#!/bin/bash

i=0
while [ $i -lt 8141 ]
    do
        for j in {1..6};
            do
                node --no-warnings bqinserter.js $i 1998-08-02&
                i=$((i+1))
            done
    wait < <(jobs -p)
    done
