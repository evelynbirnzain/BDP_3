#!/bin/bash

# submit with 1 core and 1G of memory
spark-submit --master "$MASTER" --packages $PACKAGES  --executor-memory 1G --executor-cores 1 --driver-memory 1G --driver-cores 1 --num-executors 1 --total-executor-cores 1 $1