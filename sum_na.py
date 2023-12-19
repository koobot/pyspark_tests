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
