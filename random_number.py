# Testing random number in spark databricks
# Random number is between 0 and 1
import pyspark.sql.functions as F
import pandas as pd

# Large dataset to force it to partition
# Same cluster, set same seed, see if columns come out the same
df = pd.DataFrame(range(1, 100000001), columns=['id'])
df = spark.createDataFrame(df)
df = df.withColumn("rand_seed", F.rand(seed=42))
df = df.withColumn("rand_seed2", F.rand(seed=42))
display(df.filter('rand_seed != rand_seed2'))
  # Query returned no results

# Testing again, but run in separate new dataset, then join to see if differences
df2 = pd.DataFrame(range(1, 100000001), columns=['id'])
df2 = spark.createDataFrame(df2)
df2 = df2.withColumn("rand_seed_4", F.rand(seed=42))

df_test = df.join(df2, on='id')
display(df_test.filter('rand_seed != rand_seed_4'))
  # Query returned no results
  # It must've partitioned it the same way

# Saved out df to delta table (took 2 mins and 3+ jobs to save out!)
# Then jimmy tested it on his cluster. He made a new table, same dimensions, and random number from same seed=42.
# Then joined to the table I made, and compared the random number column
  # Query returned no results

# So for dummy purposes, the random number with the same seed seems to be somewhat deterministic
