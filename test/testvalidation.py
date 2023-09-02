import sys
import unittest
import logging
import logging.config


# logging.config.fileConfig("../conf/logging.conf")
#
# logger = logging.getLogger(__name__)
#
#
# logger.info("starting validatio...! ")

class ValidationTest(unittest.TestCase):
    def test_one(self):
        self.assertEqual(10, 10)


    def test_two(self):
        d = {"A":1,"B":2}
        for k,v in enumerate(d):
            print(k,v)
        self.assertEqual(10,10)

