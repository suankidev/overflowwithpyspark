#!/usr/bin/env bash


echo "run the checks"

spark-submit --master local --deploy-mode client \
--py-files overflowwithpyspark.zip,init.py,\
init.py
