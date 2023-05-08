"""
udf module : define helper udf functions to use it with spark in the ETL process
"""
# import libs
from datetime import datetime


# spark imports and configurations
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


# udf to convert millsecond ts to timestamp format
timestamp_udf = F.udf(lambda x: datetime.utcfromtimestamp(
    int(x) / 1000), TimestampType())
