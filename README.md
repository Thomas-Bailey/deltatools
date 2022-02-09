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

When updating versions, uninstall the previous deltatools .whl file and then proceed with the above instructions.


functions
---

The current available methods in 'functions' are:

**check_path(path)**

Returns `True` if `path` can be found, else `False`.

Sample call:

    from deltatools import functions as dtf

    path = "dbfs/mnt/data/source/contoso/sales"
    dtf.check_path(path)

**rebuild_path(path)**

Drops (`dbutils.fs.rm`) and recreates (`dbutils.fs.mkdirs`) parent & existing child folder structures.  
*Warning: this will remove ALL files in child folders*

Sample call:

    from deltatools import functions as dtf

    path = "dbfs/mnt/data/source"
    dtf.rebuild_path(path)

**hash_path(source_path)**

Creates a dataframe from the source path, returns a dataframe with a `row_hash` column (SHA-256)  .
Source path must be in parquet format.

Sample call:

    from deltatools import functions as dtf

    path = "dbfs/mnt/data/source/contoso/sales"
    dtf.hash_path(path)

**hash_dataframe(dataframe)**

Takes the input dataframe and returns a dataframe with a `row_hash` column (SHA-256).
Sample call:

    from deltatools import functions as dtf

    df = spark.read.json([json_object])
    dtf.hash_dataframe(df)



**upsert(source_dataset,database_name,table_name,target_path,primary_key,condition=None)**

Performs an upsert statement against the delta lake table in the `target_path` with the dataframe passed through as `source_dataset`. Executes `whenMatchedUpdateAll()` and `whenNotMatchedInsertAll()` as part of a `merge()`.  Will run the `merge()` based on the `primary_key`, which is submitted as an array to permit composite keys.

If the table does not exist, creates a delta table at the `target_path` and creates an entry in the metastore with the `database_name` and corresponding `table_name`.

You can pass an optional condition parameter.  Remember that source and target will always be aliased as 'src' and 'tgt' respectively.  This should be passed as a string and multiple conditions can be added with 'AND' or 'OR'.

Sample call without a condition:

        from deltatools import functions as dtf

        src="/mnt/data/source/contoso/sales"
        tgt="/mnt/data/delta/contoso/sales"
        keys=["id","customer_id"]
        db = "contoso"
        tbl = "sales"

        df = dtf.hash_dataframe(src)

        dtf.upsert(df,db,tbl,tgt,keys)

Sample call with a condition:

        from deltatools import functions as dtf

        src="/mnt/data/source/contoso/sales"
        tgt="/mnt/data/delta/contoso/sales"
        keys=["id","customer_id"]
        db = "contoso"
        tbl = "sales"
        condition = "tgt.row_hash <> src.row_hash"

        df = dtf.hash_dataframe(src)

        dtf.upsert(df,db,tbl,tgt,keys,condition)

**soft_delete(id_list,database_name,table_name,target_path,key_columns,column)**

Performs a soft delete statement against the delta lake table at `database_name.table_name` with a list passed through as `id_list`, which should be an array.  Executes a SparkSQL session to call `"update {database_name}.{table_name} as tgt set {column} = True where exists (select * from {view} as src where {merge_join})"`, where `view` is a conversion of `id_list` to a temporary SQL view.

If no table exists at the `target_path`, returns "Table does not exist - no soft deletes performed" message.

Sample call:

        from deltatools import functions as dtf

        id_list = ['1000','2000','3000']
        tgt="/mnt/data/delta/contoso/sales"
        keys=["id"]
        db = "contoso"
        tbl = "sales"
        column = 'is_deleted'

        dtf.soft_delete(id_list,db,tbl,tgt,keys,column)

**delete(source_dataset,database_name,table_name,target_path,primary_key)**

Performs a hard delete statement against the delta lake table at `database_name.table_name` with the dataframe passed through as `source_dataset`.  Executes a SparkSQL session to call `delete from database_name.table_name as tgt where not exists (select * from source_dataset_temporary_view as src where tgt.id = src.id)`.

If no table exists at the `target_path`, returns "Table does not exist - no deletions performed" message.

Sample call:

        from deltatools import functions as dtf

        src="/mnt/data/source/contoso/sales"
        tgt="/mnt/data/delta/contoso/sales"
        keys=["id","customer_id"]
        db = "contoso"
        tbl = "sales"

        df = dtf.hash_dataframe(src)

        dtf.delete(df,db,tbl,tgt,keys)

**create_database(database_name)**

Creates the database in the metastore if it does not exist.  Returns message stating whether or not it has been created.

Sample call:

        from deltatools import functions as dtf

        dtf.create_database('conotoso',spark)

**database_exists(database_name)**

Verifies if the database exists.  Returns boolean (if exists, True, else False).

Sample call:

        from deltatools import functions as dtf

        dtf.database_exists('conotoso',spark)


**drop_database(database_name)**

Drops the database from the metastore if it exists.  Returns message stating whether or not it has been dropped.  This does not currently delete the data files from the associated tables in storage.

Sample call:

        from deltatools import functions as dtf

        dtf.delete_database('conotoso',spark)

**create_table(database_name,table_name,schema,path)**

Creates  a table name with an object name of *database_name.table_name* in the storage *path* (e.g. data lake mount point) using the provided DDL *schema*  string (e.g. 'id int, description string, created_date timestamp). Runs check to see if the table already exists.  Returns confirmation message.

Sample call:

        from deltatools import functions as dtf

        schema = 'id int, description string, created_date timestamp'

        loc = '/mnt/data/constoso/product_type'

        dtf.delete_database('conotoso','product_type',schema,loc,spark)

**table_exists(database_name,table_name)**

Verifies if the table exists.  Returns boolean (if exists, True, else False).

Sample call:

        from deltatools import functions as dtf

        dtf.table_exists('conotoso','product_type',spark)


**drop_table(database_name,table_name,drop_files_location=False)**

Drops table if it exists.  Returns a confirmation message.
If `drop_files_location` is set to `True`, then the location (and any sub-folders) is also dropped from the storage location. Default is False.

Sample call:

        from deltatools import functions as dtf

        dtf.drop_table('conotoso','product_type',spark)
