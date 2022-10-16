#!/bin/bash

bk="FAIL"
ts="PASS"
i=1
while true
do
    ret=$(rm -rf raft.log && go test -run TestBasicAgree2B  -race)
    if grep -q "$bk" <<< "$ret"; then
    echo "It's there"
    break
    fi
    i=`expr $i + 1`
done
