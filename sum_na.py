import pyspark.sql.functions as F

a = spark.createDataFrame(
    [("a", 1),
     ("a", 2),
     ("a", None)],
    ["let", 'num']
)
a.groupBy("let").agg(F.sum("num")).show()
  # +---+--------+
  # |let|sum(num)|
  # +---+--------+
  # |  a|       3|
  # +---+--------+

b = spark.createDataFrame(
    [("a", 1),
     ("a", 2),
     ("a", None),
     ("b", 0),
     ("b", None),
     ("c", None),
     ("c", None)],
    ["let", 'num']
)
b.groupBy("let").agg(F.sum("num")).show()
    # +---+--------+
    # |let|sum(num)|
    # +---+--------+
    # |  b|       0|
    # |  c|    NULL|
    # |  a|       3|
    # +---+--------+
