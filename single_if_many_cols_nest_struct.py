import pyspark.sql.functions as F

# not the best example, but shows how to use one If statement and assign multiple columns
df = spark.createDataFrame(
    [('a', None, None, 1),
     ('b', None, 2, None),
     ('c', 3, None, None),
     ('d', 99, None, 4),
     ('e', None, 5, None),
     ('f', 6, None, None)],
    ['id', 'up', 'giveyou', 'nevergonna']
)
df.show()
    # +---+----+-------+----------+
    # | id|  up|giveyou|nevergonna|
    # +---+----+-------+----------+
    # |  a|NULL|   NULL|         1|
    # |  b|NULL|      2|      NULL|
    # |  c|   3|   NULL|      NULL|
    # |  d|  99|   NULL|         4|
    # |  e|NULL|      5|      NULL|
    # |  f|   6|   NULL|      NULL|
    # +---+----+-------+----------+

df_nested = (df
             .withColumn('nested',
                         F.when(F.col('nevergonna').isNotNull(),
                                F.struct(F.col('nevergonna').alias('value'),
                                         F.lit('never gonna').alias('value_source')))
                         .when(F.col('giveyou').isNotNull(),
                               F.struct(F.col('giveyou').alias('value'),
                                        F.lit('give you').alias('value_source')))
                         .when(F.col('up').isNotNull(),
                               F.struct(F.col('up').alias('value'),
                                        F.lit('up').alias('value_source')))))

df_nested.show()
    # +---+----+-------+----------+----------------+
    # | id|  up|giveyou|nevergonna|          nested|
    # +---+----+-------+----------+----------------+
    # |  a|NULL|   NULL|         1|{1, never gonna}|
    # |  b|NULL|      2|      NULL|   {2, give you}|
    # |  c|   3|   NULL|      NULL|         {3, up}|
    # |  d|  99|   NULL|         4|{4, never gonna}|
    # |  e|NULL|      5|      NULL|   {5, give you}|
    # |  f|   6|   NULL|      NULL|         {6, up}|
    # +---+----+-------+----------+----------------+

# unnest and drop nested columns
df_final = df_nested.select("*", "nested.*").drop('nested', 'nevergonna', 'giveyou', 'up')
df_final.show()
    # +---+-----+------------+
    # | id|value|value_source|
    # +---+-----+------------+
    # |  a|    1| never gonna|
    # |  b|    2|    give you|
    # |  c|    3|          up|
    # |  d|    4| never gonna|
    # |  e|    5|    give you|
    # |  f|    6|          up|
    # +---+-----+------------+
