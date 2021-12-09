Welcome to the Deltatools libary!
--------------
-- Overview --
--------------

This library is a PySpark-only library, designed to work with Databricks. 

Automates source to target loads from data lake to Delta Lake tables.

Library name: deltatools
Author: Thomas Bailey (thomasjohnbailey@gmail.com)

-------------------------------
-- Installation instructions --
-------------------------------


Download the deltatools '.whl' file in the 'dist' folder.  

In your Databricks UI, navigate:

1. Compute
2. Select your cluster
3. Libraries
4. Install new
5. Library source: Upload
6. Library Type: Python Whl
7. Drop the .whl file into the area marked 'Drop WHL here'.
8. Install
9. Restart your cluster.

--------------
Features 
--------------

The current available classes & methods are:

-- deltatools.functions.verify(path)

Contains methods to run checks against data lake storage using dbutils.
'path' is the location you are checking, e.g. path = "dbfs/mnt/data/contoso"

Available methods:

    deltatools.functions.verify(path).check_path() 
        Checks whether 'path' exists, returns True if so else False.

-- deltatools.functions.load(source_path,target_path,primary_key,database_name,table_name)

Performs delta lake insert/update/delete operations.  Parquet files only, please convert any source files to parquet.

Parameter defintions:

    source_path
        Data lake source location (can be folder/container level)
    target_path
        Delta lake table location (can be folder/container level, as would be defined in a USING DELTA LOCATION statement)
    primary_key
        The primary key for the delta lake table, expects an array e.g. ["id"] or ["customer_id","order_date"]
    database_name
        The database in the Databricks workspace as found in the Data UI,
    table_name
        The delta lake talbe in the Databricks workspace as found in the database.

Available methods:

    deltatools.functions.load(source_path,target_path,primary_key,database_name,table_name).info()
        Returns the 'source_path', 'target_path', 'primary_key' parameters and how a merge join statement would be cosntructed.

    Example call:

        from deltatools import functions as f

        src="/mnt/data/source/contoso/sales"
        tgt="/mnt/data/delta/contoso/sales"
        keys=["id"]
        db = "contoso"
        tbl = "sales"

        f.load(src,tgt,keys,db,tbl).info()


    deltatools.load(source_path,target_path,primary_key,database_name,table_name).upsert()
        If table does not exist, creates a delta lake table with the data in 'source_path' and creates a delta lake table.  
        Infers the schema from source and stores in the metastore, so will appear in the Data UI.
        If table exists, runs an upsert statement:
            
            deltaTable.alias("tgt").merge(
            source_deltas.alias("src"),
            merge_join) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll()\
            .execute()

        Example call:

            from deltatools import functions as f

            src="/mnt/data/source/contoso/sales"
            tgt="/mnt/data/delta/contoso/sales"
            keys=["id"]
            db = "contoso"
            tbl = "sales"

            f.load(src,tgt,keys,db,tbl).upsert()

    deltatools.load(source_path,target_path,primary_key,database_name,table_name).delete()
        If table exists, creates temporary view of 'source_path' and deletes where not exists in 'target_path'.
        Build Spark SQL  WHERE NOT EXISTS() statement and executes via SparkSession.

        Example call:

            from deltatools import functions as f

            src="/mnt/data/source/contoso/sales"
            tgt="/mnt/data/delta/contoso/sales"
            keys=["id"]
            db = "contoso"
            tbl = "sales"

            f.load(src,tgt,keys,db,tbl).delete()
