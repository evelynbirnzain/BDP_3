#!/bin/bash

spark-submit --master "$MASTER"  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1  $1