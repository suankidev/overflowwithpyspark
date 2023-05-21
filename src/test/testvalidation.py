import sys
import unittest
import logging
import logging.config

logging.config.fileConfig("../conf/logging.conf")

logger = logging.getLogger(__name__)


logger.info("starting validatio...! ")

class ValidationTest(unittest.TestCase):
    def test_one(self):
        logger.info("checking values 2")
        self.assertEqual(10, 10)

