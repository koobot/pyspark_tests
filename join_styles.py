# Different ways to write a join
import pyspark.sql.functions as F
spark.conf.set("spark.sql.session.timeZone", "Australia/Melbourne")
spark.conf.set("spark.databricks.workspace.multipleResults.enabled", True)

df1 = spark.createDataFrame(
    [(1, 4321, "Alice"), (2, 5678, "Bob"), (3, 9876, "Charlie")],
    ['id', 'postcode', 'name']
)
df2 = spark.createDataFrame(
    [(1, 4321, "Chan"), (2, 5678, "Sagot"), (3, 9876, "Xiao")],
    ['id_num', 'postcode', 'l_name']
)
df3 = spark.createDataFrame(
    [(1, 4321, "1990-01-01"), (2, 5678, "1990-01-02"), (3, 9876, "1990-01-03")],
    ['id', 'postcode', 'bday']
)
# NOTE: 'on' CONDITIONS CAN BE SEPARATED OUT
# WORKS
cond = (F.col('a') == F.col('b')) & (F.col('c') == F.col('d')) # useful if you want to use OR
cond = [(F.col('a') == F.col('b')), (F.col('c') == F.col('d'))] # comma acts as &
cond = [F.col('a') == F.col('b'), F.col('c') == F.col('d')]
cond = [F.col('x.a') == F.col('y.b'), F.col('x.c') == F.col('y.d')] # assumes in advance x and y are aliases for df1 and df2

# CONDITIONALLY WORKS (e.g. df and col must exist)
#   cond = [df1.a == df2.b, df1.c == df2.d] # won't work unless those columns exist in the dataframe
cond = [df1.id == df2.id_num, df1.postcode == df2.postcode] 

# DOESN'T WORK
#   cond = F.col('a') == F.col('b') & F.col('c') == F.col('d')

#---------------------------------------------------------------------------
# Style 1: SAME COLUMN NAMES
#---------------------------------------------------------------------------
# style 1.1 - no duplication in columns - BEST
df1.join(df3, on=['id', 'postcode'], how='full').show()
    # +---+--------+-------+----------+
    # | id|postcode|   name|      bday|
    # +---+--------+-------+----------+
    # |  1|    4321|  Alice|1990-01-01|
    # |  2|    5678|    Bob|1990-01-02|
    # |  3|    9876|Charlie|1990-01-03|
    # +---+--------+-------+----------+

# style 1.2 - causes duplication in columns - NOT RECOMMENDED
df1.join(df3, on=[df1.id == df3.id, df1.postcode == df3.postcode], how='full').show()
    # +---+--------+-------+---+--------+----------+
    # | id|postcode|   name| id|postcode|      bday|
    # +---+--------+-------+---+--------+----------+
    # |  1|    4321|  Alice|  1|    4321|1990-01-01|
    # |  2|    5678|    Bob|  2|    5678|1990-01-02|
    # |  3|    9876|Charlie|  3|    9876|1990-01-03|
    # +---+--------+-------+---+--------+----------+

# style 1.3 - with alias
# will NOT work: `on=[a.id == b.id, a.postcode == b.postcode]`
# you will still see duplication in column names, but they can be renamed.
df1.alias('a').join(df3.alias('b'), on=[F.col('a.id') == F.col('b.id'), F.col('a.postcode') == F.col('b.postcode')], how='full') \
    .select(F.col('a.id').alias('id_1'), F.col('b.id').alias('id_2'), 'name', 'bday').show()
    # omitted postcode because lazy
    # +----+----+-------+----------+
    # |id_1|id_2|   name|      bday|
    # +----+----+-------+----------+
    # |   1|   1|  Alice|1990-01-01|
    # |   2|   2|    Bob|1990-01-02|
    # |   3|   3|Charlie|1990-01-03|
    # +----+----+-------+----------+

# SUMMARY: Renaming columns for style 1.2 and 1.3:
#   Following doesn't work as expected:
#       - doesn't rename anything: `.withColumnRenamed('df1.id', 'id_1')` or `.withColumnRenamed('a.id', 'id_1')`
#       - will rename both columns: `.withColumnRenamed('id', 'id_1')`
#       - for style 1.2, this will error as 'df1.id' doesn't exist as a column: `df1.join(df3, on=[df1.id == df3.id, df1.postcode == df3.postcode], how='full').select(F.col('df1.id').alias('id_1')).show()`
#   To rename properly, you need to:
#       - Use style 1.3, and use a select statement to rename (very tedious).
#       - If you use "*" select, it re-adds those duplicated columns (no easy shortcuts here).

#---------------------------------------------------------------------------
# STYLE 2: DIFFERNT COLUMN NAMES
#---------------------------------------------------------------------------
# style 2.1 - specify the df.col
# this won't work: `on=[df1.id == df2.id_num, 'postcode']`
df1.join(df2, on=[df1.id == df2.id_num, df1.postcode == df2.postcode], how='full').show()
    # Still get duplication in column names, e.g. postcode
    # +---+--------+-------+------+--------+------+
    # | id|postcode|   name|id_num|postcode|l_name|
    # +---+--------+-------+------+--------+------+
    # |  1|    4321|  Alice|     1|    4321|  Chan|
    # |  2|    5678|    Bob|     2|    5678| Sagot|
    # |  3|    9876|Charlie|     3|    9876|  Xiao|
    # +---+--------+-------+------+--------+------+

# style 2.2 - use F.col means you don't need to specify the df if names are different (useful for long dataframe names)

# will NOT work, if you have mix of same and different col names. Throws column ambiguous error (will need to use aliases to make it work, see style 2.3):
#   `(F.col('id') == F.col('id_num')) & (F.col('df1.postcode') == F.col('df2.postcode'))`
#   `(F.col('id') == F.col('id_num')) & (F.col('postcode') == F.col('postcode'))`
# will NOT work, if you put & in the list:
#   `[F.col('id') == F.col('id_num') & df1.postcode == df2.postcode]`
df1.join(df2, on=F.col('id') == F.col('id_num'), how='full').show()
    # still get duplication in column names, e.g. postcode
    # +---+--------+-------+------+--------+------+
    # | id|postcode|   name|id_num|postcode|l_name|
    # +---+--------+-------+------+--------+------+
    # |  1|    4321|  Alice|     1|    4321|  Chan|
    # |  2|    5678|    Bob|     2|    5678| Sagot|
    # |  3|    9876|Charlie|     3|    9876|  Xiao|
    # +---+--------+-------+------+--------+------+
# also gets same result, but includes the "same name" column for joining
df1.join(df2, on=[F.col('id') == F.col('id_num'), df1.postcode == df2.postcode], how='full').show()
df1.join(df2, on=[(F.col('id') == F.col('id_num')) & (df1.postcode == df2.postcode)], how='full').show()

# style 2.3 - use aliases - just like 1.3, allows you to select one of the 'duplicated' columns
df1.alias('a').join(df2.alias('b'), on=[F.col('id') == F.col('id_num'), F.col('a.postcode') == F.col('b.postcode')], how='full') \
    .select('id', 'a.postcode', 'name', 'l_name').show()
    # +---+--------+-------+------+
    # | id|postcode|   name|l_name|
    # +---+--------+-------+------+
    # |  1|    4321|  Alice|  Chan|
    # |  2|    5678|    Bob| Sagot|
    # |  3|    9876|Charlie|  Xiao|
    # +---+--------+-------+------+
