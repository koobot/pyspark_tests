import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType # Might not need this depends on method
spark.conf.set("spark.sql.session.timeZone", "Australia/Melbourne")

# Single example -----------------------------------------------------------
df1 = spark.sql("""
select id, cast('2024-01-19 13:00:50' as timestamp) ts from range(1)
          """)
df1.show() # This is in aus time - confirmed with display()
    # +---+-------------------+
    # | id|                 ts|
    # +---+-------------------+
    # |  0|2024-01-19 13:00:50|
    # +---+-------------------+
df1_avg = df1.groupBy('id').agg(F.avg('ts').alias('avg_ts')).withColumn('avg_ts_conv', F.from_unixtime('avg_ts')) # This produces a string, not a timestamp column
df1_avg.show()
    # +---+------------+-------------------+
    # | id|      avg_ts|        avg_ts_conv|
    # +---+------------+-------------------+
    # |  0|1.70562965E9|2024-01-19 13:00:50|
    # +---+------------+-------------------+

# Bigger example ------------------------------------------------------------
df2 = sqlContext.createDataFrame([
    ("easy", "2024-01-19 05:24:02"),
    ("easy", "2024-01-19 05:25:02"),
    ("easy", "2024-01-19 05:26:02"),
    ("one", "2024-01-19 13:00:50"),
    ("same", "2024-01-19 08:12:00"),
    ("same", "2024-01-19 08:12:00"),
    ("diff_days", "2024-01-19 23:12:31"),
    ("diff_days", "2024-01-19 23:35:16"),
    ("diff_days", "2024-01-20 01:48:23"),
    ("has_null", "2024-01-19 03:09:01"),
    ("has_null", None),
    ("has_null", "2024-01-19 05:09:01")
], ['group', 'ts'])
df2 = df2.withColumn('ts', F.to_timestamp('ts'))
df2.show() # Takes session timezone - confirmed with display()
    # +---------+-------------------+
    # |    group|                 ts|
    # +---------+-------------------+
    # |     easy|2024-01-19 05:24:02|
    # |     easy|2024-01-19 05:25:02|
    # |     easy|2024-01-19 05:26:02|
    # |      one|2024-01-19 13:00:50|
    # |     same|2024-01-19 08:12:00|
    # |     same|2024-01-19 08:12:00|
    # |diff_days|2024-01-19 23:12:31|
    # |diff_days|2024-01-19 23:35:16|
    # |diff_days|2024-01-20 01:48:23|
    # | has_null|2024-01-19 03:09:01|
    # | has_null|               NULL|
    # | has_null|2024-01-19 05:09:01|
    # +---------+-------------------+

# Don't need to convert ts to unix before averaging - yay.
# These methods all seem to preserve the session timezone as expected - yay.
df2_agg = (df2
           .groupBy('group')
           .agg(F.avg('ts').alias('avg_ts_unix'))
           # first method seems easiest
           .withColumn('avg_ts_unix_to_ts', F.to_timestamp('avg_ts_unix')) # timestamp - USE THIS
           .withColumn('avg_ts_string', F.from_unixtime('avg_ts_unix')) # string - no tz attched so be careful
           .withColumn('avg_ts_str_to_ts', F.to_timestamp('avg_ts_string')) # timestamp - but more steps - also truncated seconds because of string
           .withColumn('avg_ts_cast', (F.col('avg_ts_unix')).cast(TimestampType()))) # clunky

df2_agg.show(truncate=False)
    # +---------+--------------------+--------------------------+-------------------+-------------------+--------------------------+
    # |group    |avg_ts_unix         |avg_ts_unix_to_ts         |avg_ts_string      |avg_ts_str_to_ts   |avg_ts_cast               |
    # +---------+--------------------+--------------------------+-------------------+-------------------+--------------------------+
    # |easy     |1.705602302E9       |2024-01-19 05:25:02       |2024-01-19 05:25:02|2024-01-19 05:25:02|2024-01-19 05:25:02       |
    # |same     |1.70561232E9        |2024-01-19 08:12:00       |2024-01-19 08:12:00|2024-01-19 08:12:00|2024-01-19 08:12:00       |
    # |diff_days|1.7056699233333333E9|2024-01-20 00:12:03.333333|2024-01-20 00:12:03|2024-01-20 00:12:03|2024-01-20 00:12:03.333333|
    # |has_null |1.705597741E9       |2024-01-19 04:09:01       |2024-01-19 04:09:01|2024-01-19 04:09:01|2024-01-19 04:09:01       |
    # |one      |1.70562965E9        |2024-01-19 13:00:50       |2024-01-19 13:00:50|2024-01-19 13:00:50|2024-01-19 13:00:50       |
    # +---------+--------------------+--------------------------+-------------------+-------------------+--------------------------+
