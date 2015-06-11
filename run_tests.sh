#!/bin/sh
set -e
set -o pipefail
set -x

TH_COUNT=30
FILL_TH_COUNT=150

SIZES="1k 16k 64k 128k 1m"
RUNTIME=60
FILLRUNTIME=300
CONTAINER="PERF_TEST"
OBJ="PERF_TEST"

for SIZE in $SIZES; do
	for I in `seq 7` ; do
		./getput -c "$CONTAINER" --obj "$OBJ" --size "$SIZE" --tests "p,g,g,g,g,d" --runtime "$RUNTIME" --procs $TH_COUNT
	done
done

