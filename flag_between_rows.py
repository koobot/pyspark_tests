# Overly/unecessarily complicated way of flagging rows inbetween a start and end condition
# Want to avoid using joins here.
# https://stackoverflow.com/questions/52690102/pyspark-dataframe-iterate-between-flags-based-on-group
import pyspark.sql.functions as F
from pyspark.sql.window import Window

have = spark.createDataFrame(
    [('zz', 'aaaa', 1, None, None, 0),
    ('zz', 'aaaa', 2, None, None, 0),
    ('zz', 'aaaa', 3, None, None, 0),
    ('a', 'apple', 1, 'matched', 'start', 1),
    ('b', 'apple', 2, 'matched', None, 1),
    ('c', 'apple', 3, 'matched', 'end', 1),
    ('cc', 'apple', 4, None, None, 0),
    ('d', 'banana', 1, None, None, 0),
    ('e', 'banana', 2, None, None, 0),
    ('ee', 'banana', 3, 'matched', 'start', 1),
    ('eee', 'banana', 4, None, None, 1),
    ('eeeee', 'banana', 5, 'matched', 'end', 1),
    ('f', 'coconut', 0, None, None, 0),
    ('ff', 'coconut', 1, 'matched', 'start', 1),
    ('fff', 'coconut', 2, 'matched', None, 1),
    ('ffff', 'coconut', 3, None, None, 1),
    ('fffff', 'coconut', 4, 'matched', 'end', 1),
    ('ffff1', 'coconut', 5, 'matched', 'start', 1),
    ('ffff2', 'coconut', 6, 'matched', None, 1),
    ('ffff3', 'coconut', 7, 'matched', 'end', 1),
    ('ffff4', 'coconut', 8, None, None, 0),
    ('g', 'orange', 1, None, None, 0),
    ('h', 'orange', 2, 'matched', 'start', 1),
    ('i', 'orange', 3, 'matched', 'end', 1),
    ('j', 'orange', 4, None, None, 0),
    ('l', 'orange', 5, 'matched', 'start', 1),
    ('m', 'orange', 6, 'matched', None, 1),
    ('n', 'orange', 7, None, None, 1),
    ('o', 'orange', 8, 'matched', 'end', 1),
    ('p', 'orange', 9, None, None, 0)],
    ['id', 'group_col', 'order_col', 'flag', 'boundary', "want"])
have.show(n=100)
  # +-----+---------+---------+-------+--------+----+
  # |   id|group_col|order_col|   flag|boundary|want|
  # +-----+---------+---------+-------+--------+----+
  # |   zz|     aaaa|        1|   NULL|    NULL|   0|
  # |   zz|     aaaa|        2|   NULL|    NULL|   0|
  # |   zz|     aaaa|        3|   NULL|    NULL|   0|
  # |    a|    apple|        1|matched|   start|   1|
  # |    b|    apple|        2|matched|    NULL|   1|
  # |    c|    apple|        3|matched|     end|   1|
  # |   cc|    apple|        4|   NULL|    NULL|   0|
  # |    d|   banana|        1|   NULL|    NULL|   0|
  # |    e|   banana|        2|   NULL|    NULL|   0|
  # |   ee|   banana|        3|matched|   start|   1|
  # |  eee|   banana|        4|   NULL|    NULL|   1|
  # |eeeee|   banana|        5|matched|     end|   1|
  # |    f|  coconut|        0|   NULL|    NULL|   0|
  # |   ff|  coconut|        1|matched|   start|   1|
  # |  fff|  coconut|        2|matched|    NULL|   1|
  # | ffff|  coconut|        3|   NULL|    NULL|   1|
  # |fffff|  coconut|        4|matched|     end|   1|
  # |ffff1|  coconut|        5|matched|   start|   1|
  # |ffff2|  coconut|        6|matched|    NULL|   1|
  # |ffff3|  coconut|        7|matched|     end|   1|
  # |ffff4|  coconut|        8|   NULL|    NULL|   0|
  # |    g|   orange|        1|   NULL|    NULL|   0|
  # |    h|   orange|        2|matched|   start|   1|
  # |    i|   orange|        3|matched|     end|   1|
  # |    j|   orange|        4|   NULL|    NULL|   0|
  # |    l|   orange|        5|matched|   start|   1|
  # |    m|   orange|        6|matched|    NULL|   1|
  # |    n|   orange|        7|   NULL|    NULL|   1|
  # |    o|   orange|        8|matched|     end|   1|
  # |    p|   orange|        9|   NULL|    NULL|   0|
  # +-----+---------+---------+-------+--------+----+
# The end condition would normally be even number, but lag means this goes to row after
# If a start condition follows end condition, then that will be "incorrectly" flagged as even, so we just change it after

# Can flag rows between
w = Window.partitionBy('group_col').orderBy('order_col')
# See steps - otherwise drop intermediate columns
df = have.withColumn('counter', F.when(F.col('boundary') == "start", 1).when(F.col('boundary') == "end", 1).otherwise(0)) # Can tidy this
df = df.withColumn('lag', F.lag('counter').over(w)) # Can skip. This just for completeness
df = df.withColumn('sum_lag', F.sum(F.lag('counter').over(w)).over(w))
df = df.withColumn('fill_missing', F.coalesce(F.when(F.col('boundary') == "start", 1).otherwise(F.col('sum_lag')), F.lit(0)))
df = df.withColumn("final_flag", F.col("fill_missing") % 2) # modulo odd
df.show(n=100)
  # +-----+---------+---------+-------+--------+----+-------+----+-------+------------+----------+
  # |   id|group_col|order_col|   flag|boundary|want|counter| lag|sum_lag|fill_missing|final_flag|
  # +-----+---------+---------+-------+--------+----+-------+----+-------+------------+----------+
  # |   zz|     aaaa|        1|   NULL|    NULL|   0|      0|NULL|   NULL|           0|         0|
  # |   zz|     aaaa|        2|   NULL|    NULL|   0|      0|   0|      0|           0|         0|
  # |   zz|     aaaa|        3|   NULL|    NULL|   0|      0|   0|      0|           0|         0|
  # |    a|    apple|        1|matched|   start|   1|      1|NULL|   NULL|           1|         1|
  # |    b|    apple|        2|matched|    NULL|   1|      0|   1|      1|           1|         1|
  # |    c|    apple|        3|matched|     end|   1|      1|   0|      1|           1|         1|
  # |   cc|    apple|        4|   NULL|    NULL|   0|      0|   1|      2|           2|         0|
  # |    d|   banana|        1|   NULL|    NULL|   0|      0|NULL|   NULL|           0|         0|
  # |    e|   banana|        2|   NULL|    NULL|   0|      0|   0|      0|           0|         0|
  # |   ee|   banana|        3|matched|   start|   1|      1|   0|      0|           1|         1|
  # |  eee|   banana|        4|   NULL|    NULL|   1|      0|   1|      1|           1|         1|
  # |eeeee|   banana|        5|matched|     end|   1|      1|   0|      1|           1|         1|
  # |    f|  coconut|        0|   NULL|    NULL|   0|      0|NULL|   NULL|           0|         0|
  # |   ff|  coconut|        1|matched|   start|   1|      1|   0|      0|           1|         1|
  # |  fff|  coconut|        2|matched|    NULL|   1|      0|   1|      1|           1|         1|
  # | ffff|  coconut|        3|   NULL|    NULL|   1|      0|   0|      1|           1|         1|
  # |fffff|  coconut|        4|matched|     end|   1|      1|   0|      1|           1|         1|
  # |ffff1|  coconut|        5|matched|   start|   1|      1|   1|      2|           1|         1|
  # |ffff2|  coconut|        6|matched|    NULL|   1|      0|   1|      3|           3|         1|
  # |ffff3|  coconut|        7|matched|     end|   1|      1|   0|      3|           3|         1|
  # |ffff4|  coconut|        8|   NULL|    NULL|   0|      0|   1|      4|           4|         0|
  # |    g|   orange|        1|   NULL|    NULL|   0|      0|NULL|   NULL|           0|         0|
  # |    h|   orange|        2|matched|   start|   1|      1|   0|      0|           1|         1|
  # |    i|   orange|        3|matched|     end|   1|      1|   1|      1|           1|         1|
  # |    j|   orange|        4|   NULL|    NULL|   0|      0|   1|      2|           2|         0|
  # |    l|   orange|        5|matched|   start|   1|      1|   0|      2|           1|         1|
  # |    m|   orange|        6|matched|    NULL|   1|      0|   1|      3|           3|         1|
  # |    n|   orange|        7|   NULL|    NULL|   1|      0|   0|      3|           3|         1|
  # |    o|   orange|        8|matched|     end|   1|      1|   0|      3|           3|         1|
  # |    p|   orange|        9|   NULL|    NULL|   0|      0|   1|      4|           4|         0|
  # +-----+---------+---------+-------+--------+----+-------+----+-------+------------+----------+

tidy = df.drop("counter", "lag", "sum_lag", "fill_missing")
tidy.show(n=100)
  # +-----+---------+---------+-------+--------+----+----------+
  # |   id|group_col|order_col|   flag|boundary|want|final_flag|
  # +-----+---------+---------+-------+--------+----+----------+
  # |   zz|     aaaa|        1|   NULL|    NULL|   0|         0|
  # |   zz|     aaaa|        2|   NULL|    NULL|   0|         0|
  # |   zz|     aaaa|        3|   NULL|    NULL|   0|         0|
  # |    a|    apple|        1|matched|   start|   1|         1|
  # |    b|    apple|        2|matched|    NULL|   1|         1|
  # |    c|    apple|        3|matched|     end|   1|         1|
  # |   cc|    apple|        4|   NULL|    NULL|   0|         0|
  # |    d|   banana|        1|   NULL|    NULL|   0|         0|
  # |    e|   banana|        2|   NULL|    NULL|   0|         0|
  # |   ee|   banana|        3|matched|   start|   1|         1|
  # |  eee|   banana|        4|   NULL|    NULL|   1|         1|
  # |eeeee|   banana|        5|matched|     end|   1|         1|
  # |    f|  coconut|        0|   NULL|    NULL|   0|         0|
  # |   ff|  coconut|        1|matched|   start|   1|         1|
  # |  fff|  coconut|        2|matched|    NULL|   1|         1|
  # | ffff|  coconut|        3|   NULL|    NULL|   1|         1|
  # |fffff|  coconut|        4|matched|     end|   1|         1|
  # |ffff1|  coconut|        5|matched|   start|   1|         1|
  # |ffff2|  coconut|        6|matched|    NULL|   1|         1|
  # |ffff3|  coconut|        7|matched|     end|   1|         1|
  # |ffff4|  coconut|        8|   NULL|    NULL|   0|         0|
  # |    g|   orange|        1|   NULL|    NULL|   0|         0|
  # |    h|   orange|        2|matched|   start|   1|         1|
  # |    i|   orange|        3|matched|     end|   1|         1|
  # |    j|   orange|        4|   NULL|    NULL|   0|         0|
  # |    l|   orange|        5|matched|   start|   1|         1|
  # |    m|   orange|        6|matched|    NULL|   1|         1|
  # |    n|   orange|        7|   NULL|    NULL|   1|         1|
  # |    o|   orange|        8|matched|     end|   1|         1|
  # |    p|   orange|        9|   NULL|    NULL|   0|         0|
  # +-----+---------+---------+-------+--------+----+----------+
