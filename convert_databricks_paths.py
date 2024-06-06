# can make this more generalised in future...

def convert_dbfs_path(path: str, format='posix'):
    '''
    Converts a DBFS path to the specified format style.
    In order to read from the DBFS root, native databricks utilities (e.g. dbutils) need a different path format to python libraries (e.g. os, zipfile).
    
    format:
    choose 'posix' for python libraries like os, zip, pandas
    choose 'uri' for native databricks access, e.g. pyspark, dbutils

    For more information on DBFS paths, visit:
    https://learn.microsoft.com/en-us/azure/databricks/files/
    '''
    if format == 'posix' and path.startswith("dbfs:"):
        return path.replace("dbfs:", "/dbfs")
    if format == 'uri' and path.startswith("/dbfs"):
        return path.replace("/dbfs", "dbfs:")
    print(f"Path unchanged...{path}")
    return path
