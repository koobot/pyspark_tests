# Want to replicate data.table's rleid
# Group IDs don't have to be contiguous
# adapted from https://stackoverflow.com/questions/63909778/assign-id-based-on-another-column-value
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
df = (have
      .withColumn("prev_group", F.lag('group_col', 1, default="first").over(w))
      .withColumn('state_num', F.when(F.col('prev_group') == "first", 1).when(F.col('group_col') == F.col('prev_group'), 0).otherwise(2))
      .withColumn('rleid', F.sum('state_num').over(w)))
# can drop columns if you want to be clean

df.show()
# +--------+---+---------+---------+----------+---------+-----+
# |part_col| id|group_col|order_col|prev_group|state_num|rleid|
# +--------+---+---------+---------+----------+---------+-----+
# |       x|  a|       r1|        1|     first|        1|    1|
# |       x|  b|       r1|        2|        r1|        0|    1|
# |       x|  c|       r1|        3|        r1|        0|    1|
# |       x|  d|       s3|        4|        r1|        2|    3|
# |       x|  e|       s3|        5|        s3|        0|    3|
# |       x|  f|       s4|        6|        s3|        2|    5|
# |       x|  g|       r1|        7|        s4|        2|    7|
# |       y|  h|       r2|        2|     first|        1|    1|
# |       y|  i|       s3|        3|        r2|        2|    3|
# |       y|  j|       s3|        4|        s3|        0|    3|
# |       y|  k|       r2|        5|        s3|        2|    5|
# |       y|  l|       r2|        7|        r2|        0|    5|
# |       y|  m|       s4|        8|        r2|        2|    7|
# +--------+---+---------+---------+----------+---------+-----+

clean = df.drop('prev_group', 'state_num')
# clean.show()
# +--------+---+---------+---------+-----+
# |part_col| id|group_col|order_col|rleid|
# +--------+---+---------+---------+-----+
# |       x|  a|       r1|        1|    1|
# |       x|  b|       r1|        2|    1|
# |       x|  c|       r1|        3|    1|
# |       x|  d|       s3|        4|    3|
# |       x|  e|       s3|        5|    3|
# |       x|  f|       s4|        6|    5|
# |       x|  g|       r1|        7|    7|
# |       y|  h|       r2|        2|    1|
# |       y|  i|       s3|        3|    3|
# |       y|  j|       s3|        4|    3|
# |       y|  k|       r2|        5|    5|
# |       y|  l|       r2|        7|    5|
# |       y|  m|       s4|        8|    7|
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

# Tried
# df = have.withColumn("rleid", F.dense_rank().over(Window.orderBy('group_col')))
# df = have.withColumn("rleid", F.dense_rank().over(Window.orderBy('order_col')))
# Also tried: https://stackoverflow.com/questions/68380054/rank-values-in-spark-on-a-column-based-on-previous-values
# df = (have
#     .withColumn("rank", F.array_sort(F.collect_set('group_col').over(Window.orderBy('order_col').rowsBetween(Window.unboundedPreceding, Window.currentRow))))
#     .withColumn('rleid', F.expr("array_position(rank, group_col)")))
