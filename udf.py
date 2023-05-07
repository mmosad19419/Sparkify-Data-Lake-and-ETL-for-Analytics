"""
udf module is where i define helper udf functions to use it with spark in the ETL process
"""
# import libs
import os
import glob
from datetime import *
import pandas as pd

# spark imports and configurations
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# udf to convert millsecond ts to timestamp format
timestamp_udf = F.udf(lambda x: datetime.utcfromtimestamp(
    int(x) / 1000), TimestampType())