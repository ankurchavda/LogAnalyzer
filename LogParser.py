import re

from pyspark.sql import Row


APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:\/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'

# Returns a dictionary containing the parts of the Apache Access Log.
def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        pass
    else:
        return (match.group(1),match.group(2),match.group(3),match.group(4),match.group(5),match.group(6),match.group(7),int(match.group(8)),long(match.group(9)))


