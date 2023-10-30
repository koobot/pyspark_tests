# First and last functions
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

w = Window.partitionBy("part_col", "group_col")
df1 = have.withColumn("test", F.first("order_col").over(w.orderBy(F.col("order_col").desc())))
df1.orderBy("id").show()
# +--------+---+---------+---------+----+
# |part_col| id|group_col|order_col|test|
# +--------+---+---------+---------+----+
# |       x|  a|       r1|        1|   7|
# |       x|  b|       r1|        2|   7|
# |       x|  c|       r1|        3|   7|
# |       x|  d|       s3|        4|   5|
# |       x|  e|       s3|        5|   5|
# |       x|  f|       s4|        6|   6|
# |       x|  g|       r1|        7|   7|
# |       y|  h|       r2|        2|   7|
# |       y|  i|       s3|        3|   4|
# |       y|  j|       s3|        4|   4|
# |       y|  k|       r2|        5|   7|
# |       y|  l|       r2|        7|   7|
# |       y|  m|       s4|        8|   8|
# +--------+---+---------+---------+----+


# Last is not the opposite of first
# It shows last thing is saw - and this can change depending on shuffling
df2 = have.withColumn("test", F.last("order_col").over(w.orderBy(F.col("order_col"))))
df2.orderBy("id").show()
# +--------+---+---------+---------+----+
# |part_col| id|group_col|order_col|test|
# +--------+---+---------+---------+----+
# |       x|  a|       r1|        1|   1|
# |       x|  b|       r1|        2|   2|
# |       x|  c|       r1|        3|   3|
# |       x|  d|       s3|        4|   4|
# |       x|  e|       s3|        5|   5|
# |       x|  f|       s4|        6|   6|
# |       x|  g|       r1|        7|   7|
# |       y|  h|       r2|        2|   2|
# |       y|  i|       s3|        3|   3|
# |       y|  j|       s3|        4|   4|
# |       y|  k|       r2|        5|   5|
# |       y|  l|       r2|        7|   7|
# |       y|  m|       s4|        8|   8|
# +--------+---+---------+---------+----+
