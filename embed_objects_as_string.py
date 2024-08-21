# Hacky method of embedding a dataframe -- in a --> dictionary -- in a --> list -- in a --> spark df column
# This is just to preserve more information for retrieval at a later date
# Also has the code to read back the data to a dataframe
# Assume databricks notebook where spark all set up under the hood.

# -------------------------------------------------------------------
# JSON VERSION
# -------------------------------------------------------------------
# Less code than csv version, but columns in embedded dataFrame may be read in out of order
import json

# Create the DataFrame to embed
embed_df = spark.createDataFrame([("x", 1, 3.0), ("y", 2, None), ("z", 3, 5.0)], ["let", "num", "float"])
embed_df.show()
  # +---+---+-----+
  # |let|num|float|
  # +---+---+-----+
  # |  x|  1|  3.0|
  # |  y|  2| NULL|
  # |  z|  3|  5.0|
  # +---+---+-----+

# Convert the DataFrame to JSON and embed in a dictionary
embed_dict = [{"j": "string", "df": embed_df.toJSON().collect()}] # different to CSV version
# preserves : or else will turn to =
embed_dict_string = json.dumps(embed_dict)
print(embed_dict_string) # different output to CSV version
  # [{"j": "string", "df": ["{\"let\":\"x\",\"num\":1,\"float\":3.0}", "{\"let\":\"y\",\"num\":2}", "{\"let\":\"z\",\"num\":3,\"float\":5.0}"]}]

# Create a DataFrame with the embedded dictionary
proper_df = spark.createDataFrame([("a", embed_dict_string)], ["id", "details"])
proper_df.show()
  # +---+--------------------+
  # | id|             details|
  # +---+--------------------+
  # |  a|[{"j": "string", ...|
  # +---+--------------------+

# Extract the embedded dictionary
# get_embed_dict = eval(proper_df.select("details").collect()[0][0]) # same thing - but eval not recommended
get_embed_dict = json.loads(proper_df.select("details").collect()[0][0])
print(type(get_embed_dict))
  # <class 'list'>

# Read back the embedded DataFrame
get_embed_df_string = get_embed_dict[0]['df']
print(get_embed_df_string)
  # ['{"let":"x","num":1,"float":3.0}', '{"let":"y","num":2}', '{"let":"z","num":3,"float":5.0}']

# get_embed_df = sqlContext.read.json(sc.parallelize(get_embed_df_string)) # seems to do same thing as below
get_embed_df = spark.read.json(spark.sparkContext.parallelize(get_embed_df_string)) # different to CSV version
get_embed_df.show() # columns different order compared to original. See CSV version if this is important.
  # +-----+---+---+
  # |float|let|num|
  # +-----+---+---+
  # |  3.0|  x|  1|
  # | NULL|  y|  2|
  # |  5.0|  z|  3|
  # +-----+---+---+


# -------------------------------------------------------------------
# CSV VERSION
# -------------------------------------------------------------------
# Preserves column order - but how neccessary is that property?
# More lines of code, needs pandas library too
import pandas as pd
import json
from io import StringIO

# Create the DataFrame to embed
embed_df = spark.createDataFrame([("x", 1, 3.0), ("y", 2, None), ("z", 3, 5.0)], ["let", "num", "float"])
embed_df.show()

# Convert the DataFrame to CSV and embed in a dictionary
embed_dict = [{"j": "string", "df": embed_df.toPandas().to_csv(index=False)}] # different to JSON version
# preserves : or else will turn to =
embed_dict_string = json.dumps(embed_dict)
print(embed_dict_string) # different output to JSON version
  # [{"j": "string", "df": "let,num,float\nx,1,3.0\ny,2,\nz,3,5.0\n"}]

# Create a DataFrame with the embedded dictionary
proper_df = spark.createDataFrame([("a", embed_dict_string)], ["id", "details"])
proper_df.show()

# Extract the embedded dictionary
get_embed_dict = json.loads(proper_df.select("details").collect()[0][0]) # eval also works here (same as JSON version)
print(type(get_embed_dict))

# Read back the embedded DataFrame
get_embed_df_string = get_embed_dict[0]['df']
print(get_embed_df_string)
  # let,num,float
  # x,1,3.0
  # y,2,
  # z,3,5.0
get_embed_df = spark.createDataFrame(pd.read_csv(StringIO(get_embed_df_string))) # different to JSON version
get_embed_df.show()
  # +---+---+-----+
  # |let|num|float|
  # +---+---+-----+
  # |  x|  1|  3.0|
  # |  y|  2| NULL|
  # |  z|  3|  5.0|
  # +---+---+-----+
