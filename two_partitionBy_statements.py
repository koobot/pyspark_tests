import pyspark.sql.functions as F
from pyspark.sql.window import Window

have = spark.createDataFrame(
    [('a', 'r1', '1'),
    ('b', 'r1', '2'),
    ('c', 'r1', '3'),
    ('d', 's3', '4'),
    ('e', 's3', '5'),
    ('f', 's4', '6'),
    ('g', 'r1', '7')],
    ['id', 'group_col', 'order_col'])

# test two partition by statements. In df =, it ignores the first partitionBy, and just does the second.
df = (have.withColumn('rleid', F.dense_rank().over(Window.partitionBy('id').orderBy('order_col').partitionBy("group_col") )))
df2 = (have.withColumn('rleid', F.dense_rank().over(Window.orderBy('order_col').partitionBy("group_col") )))
df3 = (have.withColumn('rleid', F.dense_rank().over(Window.partitionBy('group_col').orderBy('order_col').partitionBy("id") )))
df.orderBy("order_col").show()
df2.orderBy("order_col").show()
df3.orderBy("order_col").show()

# +---+---------+---------+-----+
# | id|group_col|order_col|rleid|
# +---+---------+---------+-----+
# |  a|       r1|        1|    1|
# |  b|       r1|        2|    2|
# |  c|       r1|        3|    3|
# |  d|       s3|        4|    1|
# |  e|       s3|        5|    2|
# |  f|       s4|        6|    1|
# |  g|       r1|        7|    4|
# +---+---------+---------+-----+

# +---+---------+---------+-----+
# | id|group_col|order_col|rleid|
# +---+---------+---------+-----+
# |  a|       r1|        1|    1|
# |  b|       r1|        2|    2|
# |  c|       r1|        3|    3|
# |  d|       s3|        4|    1|
# |  e|       s3|        5|    2|
# |  f|       s4|        6|    1|
# |  g|       r1|        7|    4|
# +---+---------+---------+-----+

# +---+---------+---------+-----+
# | id|group_col|order_col|rleid|
# +---+---------+---------+-----+
# |  a|       r1|        1|    1|
# |  b|       r1|        2|    1|
# |  c|       r1|        3|    1|
# |  d|       s3|        4|    1|
# |  e|       s3|        5|    1|
# |  f|       s4|        6|    1|
# |  g|       r1|        7|    1|
# +---+---------+---------+-----+
