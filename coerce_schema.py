# Dummy code will not run (obviously)
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, DoubleType, LongType, DateType
spark.conf.set("spark.sql.session.timeZone", "Australia/Melbourne")

def coerce_schema(df, target_schema, timestamp_format=None, date_format=None):
    '''
    Assumes spark dataframe is read in as strings. Cocerces that to target schema.
    For timestamp_format and date_format, see spark documentation.
    Doesn't enforce nullable.
    Doesn't throw errors if coercion fails, so have to manually inspect it did what you want.
    '''
    coerced_df = df

    for field in target_schema.fields:
        column_name = field.name
        target_type = field.dataType
        
        if column_name in df.columns:
            if isinstance(target_type, TimestampType):
                coerced_df = coerced_df.withColumn(column_name, F.to_timestamp(F.col(column_name), timestamp_format))
            elif isinstance(target_type, DateType):
                coerced_df = coerced_df.withColumn(column_name, F.to_date(F.col(column_name), date_format))
            else:
                coerced_df = coerced_df.withColumn(column_name, F.col(column_name).cast(target_type))
    
    return coerced_df

  
# Read in CSV without any schema (all string types)
raw = (spark.read.format("csv")
  .option('header', 'true')
  .option('delimiter', ';')
  .load('s3://bucket/dir/*/*/foobar.csv')
  .withColumn('source_file', F.input_file_name()))

schema = StructType([
  StructField('calendar_day', DateType(), False),
  StructField('id', IntegerType(), False),
  StructField('start_time', TimestampType(), True),
  StructField('end_time', TimestampType(), True),
  StructField('latitude', DoubleType(), True),
  StructField('longitude', DoubleType(), True),
])

good_df = coerce_schema(df=raw, target_schema=schema)
raw.show()
good_df.show()
