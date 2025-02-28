# Technically this is for databricks, not just pyspark.
# Assuming a databricks python notebook
#----------------------------------------------------------------
# Initialize Spark session
spark = SparkSession.builder.appName("StopLineVisualization").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Australia/Melbourne")
# Get S3 details
secret_scope = "edp-adval-kv-01"
AWS_ACCESS_KEY_ID = dbutils.secrets.get(scope=secret_scope, key="s3-access-key-id")
AWS_SECRET_ACCESS_KEY = dbutils.secrets.get(scope=secret_scope, key="s3-secret-access-key")


# For pandas ---------------------------------------------------
# First, set up AWS credentials if not already configured in Databricks
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_key'

# Write pandas DataFrame to S3
import pandas as pd
import boto3
from io import StringIO

def write_pandas_df_to_s3(df, bucket_name, file_path):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_path,
        Body=csv_buffer.getvalue()
    )
    
    print(f"File written to: s3://{bucket_name}/{file_path}")

# Example usage
write_pandas_df_to_s3(
    your_pandas_df, 
    "your-bucket-name", 
    "path/to/your/file.csv"
)

# For pyspark ----------------------------------------------------
spark.conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
spark.conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
