# Cheatsheet for pivoting many columns

import pyspark.sql.functions as F
from pyspark.sql.window import Window

df = spark.createDataFrame(
    [('banana', '2019', '99', '20'),
     ('banana', '2020', '88', '21'),
     ('apple', '2019', '4', '300'),
     ('apple', '2020', '17', '234'),
     ('pear', '2019', '1', '82')
     ],
    ['fruit', 'year', 'pct', 'obs']
)

# pivot
df_t = (df
        .groupBy('fruit')
        .pivot('year')
        .agg(F.first('pct').alias('pct'),
             F.first('obs').alias('obs')))

df.show()
# +------+----+---+---+
# | fruit|year|pct|obs|
# +------+----+---+---+
# |banana|2019| 99| 20|
# |banana|2020| 88| 21|
# | apple|2019|  4|300|
# | apple|2020| 17|234|
# |  pear|2019|  1| 82|
# +------+----+---+---+
df_t.show()
# +------+--------+--------+--------+--------+
# | fruit|2019_pct|2019_obs|2020_pct|2020_obs|
# +------+--------+--------+--------+--------+
# | apple|       4|     300|      17|     234|
# |banana|      99|      20|      88|      21|
# |  pear|       1|      82|    null|    null|
# +------+--------+--------+--------+--------+
