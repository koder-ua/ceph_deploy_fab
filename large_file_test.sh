#!/bin/bash
set -x
set -e
set -o pipefail

fname=data.bin

# dd if=/dev/zero of=$fname bs=1048576 count=80960

for count in `seq 5` ; do

    name="data_${count}"
    echo -n $name >> long_res.txt

    for i in `seq 5` ; do
        (time swift upload $name --segment-size=100m $fname > /dev/null) 2>&1 >>long_res.txt
    done

    echo >> long_res.txt
    echo >> long_res.txt
done
