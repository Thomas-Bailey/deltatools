Welcome to the deltatools library!
--------------
Overview
--------------

This library is a PySpark-only library, designed to work with Databricks. 

Automates source to target loads from data lake to Delta Lake tables.

Library name: deltatools
Author: Thomas Bailey (thomasjohnbailey@gmail.com)

-------------------------------
Installation instructions
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


functions
---

The current available methods in 'functions' are:

**check_path(path)**

Returns `True` if `path` can be found, else `False`.

Sample call:

    from deltatools import functions as dtf

    path = "dbfs/mnt/data/source/contoso/sales"
    dtf.check_path(path)


**hash(source_path)**

Creates a dataframe from the source path, returns a dataframe with a `row_hash` column (SHA-256)  .
Source path must be in parquet format.

Sample call:

    from deltatools import functions as dtf

    path = "dbfs/mnt/data/source/contoso/sales"
    dtf.hash(path)


**upsert(source_dataset,database_name,table_name,target_path,primary_key)**

Performs an upsert statement against the delta lake table in the `target_path` with the dataframe passed through as `source_dataset`. Executes `whenMatchedUpdateAll()` and `whenNotMatchedInsertAll()` as part of a `merge()`.  Will run the `merge()` based on the `primary_key`, which is submitted as an array to permit composite keys.

If the table does not exist, creates a delta table at the `target_path` and creates an entry in the metastore with the `database_name` and corresponding `table_name`.

Sample call:

        from deltatools import functions as dtf

        src="/mnt/data/source/contoso/sales"
        tgt="/mnt/data/delta/contoso/sales"
        keys=["id","customer_id"]
        db = "contoso"
        tbl = "sales"

        df = dtf.hash(src)

        dtf.upsert(df,db,tbl,tgt,keys)

**delete(source_dataset,database_name,table_name,target_path,primary_key)**

Performs an delete statement against the delta lake table at `database_name.table_name` with the dataframe passed through as `source_dataset`.  Executes a SparkSQL session to call `delete from database_name.table_name as tgt where not exists (select * from source_dataset_temporary_view as src where tgt.id = src.id)`.

If no table exists at the `target_path`, returns "Table does not exist - no deletions performed" message.

Sample call:

        from deltatools import functions as dtf

        src="/mnt/data/source/contoso/sales"
        tgt="/mnt/data/delta/contoso/sales"
        keys=["id","customer_id"]
        db = "contoso"
        tbl = "sales"

        df = dtf.hash(src)

        dtf.delete(df,db,tbl,tgt,keys)