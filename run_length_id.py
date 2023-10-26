# Want to replicate data.table's rleid
# Group IDs don't have to be contiguous
# From answer to my question: https://stackoverflow.com/a/77356734/19825782
import pyspark.sql.functions as F
from pyspark.sql.window import Window

have = spark.createDataFrame(
    [('x', 'a', 'r1', '1'),
    ('x', 'b', 'r1', '2'),
    ('x', 'c', 'r1', '3'),
    ('x', 'd', 's3', '4'),
    ('x', 'e', 's3', '5'),
    ('x', 'f', 's4', '6'),
    ('x', 'g', 'r1', '7'),
    ('y', 'h', 'r2', '2'),
    ('y', 'i', 's3', '3'),
    ('y', 'j', 's3', '4'),
    ('y', 'k', 'r2', '5'),
    ('y', 'l', 'r2', '7'),
    ('y', 'm', 's4', '8')],
    ['part_col', 'id', 'group_col', 'order_col'])

want = spark.createDataFrame(
    [('x', 'a', 'r1', '1', '1'),
    ('x', 'b', 'r1', '2', '1'),
    ('x', 'c', 'r1', '3', '1'),
    ('x', 'd', 's3', '4', '2'),
    ('x', 'e', 's3', '5', '2'),
    ('x', 'f', 's4', '6', '3'),
    ('x', 'g', 'r1', '7', '4'),
    ('y', 'h', 'r2', '2', '1'),
    ('y', 'i', 's3', '3', '2'),
    ('y', 'j', 's3', '4', '2'),
    ('y', 'k', 'r2', '5', '3'),
    ('y', 'l', 'r2', '7', '3'),
    ('y', 'm', 's4', '8', '4')],
    ['part_col', 'id', 'group_col', 'order_col', 'rleid'])

w = Window.partitionBy('part_col').orderBy('order_col')
cond = F.lag('group_col').over(w) != F.col('group_col')
df = have.withColumn('rleid', F.coalesce(F.sum(cond.cast('int')).over(w), F.lit(0)))
df.show()
# +--------+---+---------+---------+-----+
# |part_col| id|group_col|order_col|rleid|
# +--------+---+---------+---------+-----+
# |       x|  a|       r1|        1|    0|
# |       x|  b|       r1|        2|    0|
# |       x|  c|       r1|        3|    0|
# |       x|  d|       s3|        4|    1|
# |       x|  e|       s3|        5|    1|
# |       x|  f|       s4|        6|    2|
# |       x|  g|       r1|        7|    3|
# |       y|  h|       r2|        2|    0|
# |       y|  i|       s3|        3|    1|
# |       y|  j|       s3|        4|    1|
# |       y|  k|       r2|        5|    2|
# |       y|  l|       r2|        7|    2|
# |       y|  m|       s4|        8|    3|
# +--------+---+---------+---------+-----+

# This is just so the assert statements will work - but unnecssary in practice
clean = df.withColumn("rleid", F.col("rleid") + 1)
clean.show()
# +--------+---+---------+---------+-----+
# |part_col| id|group_col|order_col|rleid|
# +--------+---+---------+---------+-----+
# |       x|  a|       r1|        1|    1|
# |       x|  b|       r1|        2|    1|
# |       x|  c|       r1|        3|    1|
# |       x|  d|       s3|        4|    2|
# |       x|  e|       s3|        5|    2|
# |       x|  f|       s4|        6|    3|
# |       x|  g|       r1|        7|    4|
# |       y|  h|       r2|        2|    1|
# |       y|  i|       s3|        3|    2|
# |       y|  j|       s3|        4|    2|
# |       y|  k|       r2|        5|    3|
# |       y|  l|       r2|        7|    3|
# |       y|  m|       s4|        8|    4|
# +--------+---+---------+---------+-----+

want.orderBy('id').show()
# +--------+---+---------+---------+-----+
# |part_col| id|group_col|order_col|rleid|
# +--------+---+---------+---------+-----+
# |       x|  a|       r1|        1|    1|
# |       x|  b|       r1|        2|    1|
# |       x|  c|       r1|        3|    1|
# |       x|  d|       s3|        4|    2|
# |       x|  e|       s3|        5|    2|
# |       x|  f|       s4|        6|    3|
# |       x|  g|       r1|        7|    4|
# |       y|  h|       r2|        2|    1|
# |       y|  i|       s3|        3|    2|
# |       y|  j|       s3|        4|    2|
# |       y|  k|       r2|        5|    3|
# |       y|  l|       r2|        7|    3|
# |       y|  m|       s4|        8|    4|
# +--------+---+---------+---------+-----+

# Compare dataframes
assert clean.orderBy('id').exceptAll(want.orderBy('id')).count() == 0
# want.orderBy('id').exceptAll(clean.orderBy('id')).show()

# Tried
# df = have.withColumn("rleid", F.dense_rank().over(Window.orderBy('group_col')))
# df = have.withColumn("rleid", F.dense_rank().over(Window.orderBy('order_col')))
# Also tried: https://stackoverflow.com/questions/68380054/rank-values-in-spark-on-a-column-based-on-previous-values
# df = (have
#     .withColumn("rank", F.array_sort(F.collect_set('group_col').over(Window.orderBy('order_col').rowsBetween(Window.unboundedPreceding, Window.currentRow))))
#     .withColumn('rleid', F.expr("array_position(rank, group_col)")))
