#!/bin/bash

i=0
while [ $i -lt 708 ]
    do
        for j in {1..6};
            do
                node --no-warnings bqinserter.js $i 2023-08-26&
                i=$((i+1))
            done
    wait < <(jobs -p)
    done
