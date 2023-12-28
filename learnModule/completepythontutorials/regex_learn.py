"""

. "anything"- letters, numbers, symbols..but not newlines
+ "one or more of"
* "zero or more of"
? "zero or one of

.*   zero or more of anything
.+  one or more of anything
.? zero or one of anything

"""

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logger.warning("test")

import re

email = "sujeetkumarsingh@live.com"
expression = '[a-z]+'

matches = re.findall(expression,email)

print(matches)





