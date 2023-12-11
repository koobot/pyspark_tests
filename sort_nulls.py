# Nulls and sorting functions - includes window and rank
import pyspark.sql.functions as F
from pyspark.sql.window import Window

have = spark.createDataFrame(
    [('a', 'x', 3, '10'),
     ('b', 'x', 2, '11'),
     ('c', 'x', None, '12'),
     ('d', 'y', 1, '13'),
     ('e', 'y', 2, '14')],
    ['id', 'part_col', 'order_col', 'value'])

# Ascending
have.orderBy("order_col").show() # nulls are first
# +---+--------+---------+-----+
# | id|part_col|order_col|value|
# +---+--------+---------+-----+
# |  c|       x|     NULL|   12|
# |  d|       y|        1|   13|
# |  b|       x|        2|   11|
# |  e|       y|        2|   14|
# |  a|       x|        3|   10|
# +---+--------+---------+-----+
have.orderBy(F.col("order_col").asc_nulls_last()).show()
# +---+--------+---------+-----+
# | id|part_col|order_col|value|
# +---+--------+---------+-----+
# |  d|       y|        1|   13|
# |  b|       x|        2|   11|
# |  e|       y|        2|   14|
# |  a|       x|        3|   10|
# |  c|       x|     NULL|   12|
# +---+--------+---------+-----+

# Descending
have.orderBy(F.col("order_col").desc_nulls_last()).show()
# +---+--------+---------+-----+
# | id|part_col|order_col|value|
# +---+--------+---------+-----+
# |  a|       x|        3|   10|
# |  e|       y|        2|   14|
# |  b|       x|        2|   11|
# |  d|       y|        1|   13|
# |  c|       x|     NULL|   12|
# +---+--------+---------+-----+

# With partition
w = Window.partitionBy("part_col")
# Descending nulls last
df1 = have.withColumn('rank', F.rank().over(w.orderBy(F.col("order_col").desc_nulls_last())))
df1.show()
  # +---+--------+---------+-----+----+
  # | id|part_col|order_col|value|rank|
  # +---+--------+---------+-----+----+
  # |  a|       x|        3|   10|   1|
  # |  b|       x|        2|   11|   2|
  # |  c|       x|     NULL|   12|   3|
  # |  e|       y|        2|   14|   1|
  # |  d|       y|        1|   13|   2|
  # +---+--------+---------+-----+----+
# Descending nulls first
df2 = have.withColumn('rank', F.rank().over(w.orderBy(F.col("order_col").desc_nulls_first())))
df2.show()
  # +---+--------+---------+-----+----+
  # | id|part_col|order_col|value|rank|
  # +---+--------+---------+-----+----+
  # |  c|       x|     NULL|   12|   1|
  # |  a|       x|        3|   10|   2|
  # |  b|       x|        2|   11|   3|
  # |  e|       y|        2|   14|   1|
  # |  d|       y|        1|   13|   2|
  # +---+--------+---------+-----+----+
# Regular descending
df3 = have.withColumn('rank', F.rank().over(w.orderBy(F.col("order_col").desc())))
df3.show()
  # +---+--------+---------+-----+----+
  # | id|part_col|order_col|value|rank|
  # +---+--------+---------+-----+----+
  # |  a|       x|        3|   10|   1|
  # |  b|       x|        2|   11|   2|
  # |  c|       x|     NULL|   12|   3|
  # |  e|       y|        2|   14|   1|
  # |  d|       y|        1|   13|   2|
  # +---+--------+---------+-----+----+
# multiple columns
df32 = have.withColumn('rank', F.rank().over(w.orderBy(*[F.desc_nulls_last(c) for c in ["order_col", "value"]])))

# Ascending nulls last
df4 = have.withColumn('rank', F.rank().over(w.orderBy(F.col("order_col").asc_nulls_last())))
df4.show()
  # +---+--------+---------+-----+----+
  # | id|part_col|order_col|value|rank|
  # +---+--------+---------+-----+----+
  # |  b|       x|        2|   11|   1|
  # |  a|       x|        3|   10|   2|
  # |  c|       x|     NULL|   12|   3|
  # |  d|       y|        1|   13|   1|
  # |  e|       y|        2|   14|   2|
  # +---+--------+---------+-----+----+
# Ascending nulls first
df5 = have.withColumn('rank', F.rank().over(w.orderBy(F.col("order_col").asc_nulls_first())))
df5.show()
  # +---+--------+---------+-----+----+
  # | id|part_col|order_col|value|rank|
  # +---+--------+---------+-----+----+
  # |  c|       x|     NULL|   12|   1|
  # |  b|       x|        2|   11|   2|
  # |  a|       x|        3|   10|   3|
  # |  d|       y|        1|   13|   1|
  # |  e|       y|        2|   14|   2|
  # +---+--------+---------+-----+----+
# Regular ascending
df6 = have.withColumn('rank', F.rank().over(w.orderBy(F.col("order_col").asc())))
df6.show()
  # +---+--------+---------+-----+----+
  # | id|part_col|order_col|value|rank|
  # +---+--------+---------+-----+----+
  # |  c|       x|     NULL|   12|   1|
  # |  b|       x|        2|   11|   2|
  # |  a|       x|        3|   10|   3|
  # |  d|       y|        1|   13|   1|
  # |  e|       y|        2|   14|   2|
  # +---+--------+---------+-----+----+
