# -*- coding: utf-8 -*-
"""
==============================================

Author       : Sujeet kumar Singh
Version      : 0.1
Description : ingest the data from oracle db

===============================================
"""
import sys

from oracle_reporting_project.utils.readUtils.metaread import get_db_meta_data_dictionary
# set env
from oracle_reporting_project.utils.setenv import *
from oracle_reporting_project.IngestData import ingest_data
from utils.sparkUtils.spark_utils import get_spark_session

if __name__ == '__main__':
    if len(sys.argv) >= 3:
        logger = init_logger(sys.argv[2])
        _date = sys.argv[1]
        _id = sys.argv[2]
        _sequence = sys.argv[3]
        metadata = get_db_meta_data_dictionary(_id, logger)
        spark = get_spark_session(logger)
        if _sequence == 'i':
            ingest_data(logger, metadata, spark)
        if _sequence == 'j':
            transform_date(logger, metadata)
