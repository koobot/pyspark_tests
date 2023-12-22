import pyspark.sql.functions as F

df = sqlContext.createDataFrame([
    ("a", None, None, 1),
    ("a", "code1", None, 2),
    ("a", "code2", "name2", None),
    ("b", 'code1', 'name1', 3)
], ["id", "code", "name", "val"])

df.show()
  # +---+-----+-----+----+
  # | id| code| name| val|
  # +---+-----+-----+----+
  # |  a| NULL| NULL|   1|
  # |  a|code1| NULL|   2|
  # |  a|code2|name2|NULL|
  # |  b|code1|name1|   3|
  # +---+-----+-----+----+

(df
  .groupby("id")
  .agg(F.collect_set("code"),
       F.collect_list("name"),
       F.sum("val"))
  .show())
  # +---+-----------------+------------------+--------+
  # | id|collect_set(code)|collect_list(name)|sum(val)|
  # +---+-----------------+------------------+--------+
  # |  a|   [code2, code1]|           [name2]|       3|
  # |  b|          [code1]|           [name1]|       3|
  # +---+-----------------+------------------+--------+
