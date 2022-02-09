from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql import SparkSession as s
from pyspark.sql.types import StructType,StructField, StringType
from delta.tables import *
from deltatools import functions as dtf
import IPython

dbutils = IPython.get_ipython().user_ns["dbutils"]

#check if path exists
def check_path(path):
  try:  
    dbutils.fs.ls(path)
    return True
  except:
    return False

def rebuild_path(path):
  try:
    list_of_tables = dbutils.fs.ls(path)
     
    for file in list_of_tables:
      if file.name.endswith('/'):
        list_of_tables.extend(dbutils.fs.ls(file.path))
        dbutils.fs.rm(file.path, recurse=True)
        dbutils.fs.mkdirs(file.path)
      else:
        dbutils.fs.rm(file.path[0:file.path.rfind('/')+1], recurse=True)
        dbutils.fs.mkdirs(file.path[0:file.path.rfind('/')+1])
  except Exception as e:
    print('Searching for file path failed : '+str(file))
    raise e

def hash_path(source_path):
  #Start Spark session
  sesh = s.builder.getOrCreate()

  #Read delta file as a data frame
  source_deltas = sesh.read.parquet(source_path)
  
  # Add a SHA2 row hash column to enable delta indentification
  source_deltas = source_deltas.withColumn("row_hash",sha2(concat_ws("||", *source_deltas.columns), 256))

  return source_deltas

def hash_dataframe(dataframe):

  # Add a SHA2 row hash column to enable delta indentification
  hashed_dataframe = dataframe.withColumn("row_hash",sha2(concat_ws("||", *dataframe.columns), 256))

  return hashed_dataframe

#Run merge from source to target
def upsert(source_dataset,database_name,table_name,target_path,primary_key,condition=None):
  #Start Spark session
  sesh = s.builder.getOrCreate()
    
  #Determine if merge or new load
  # Check if the delta lake table exists
  bool_test = check_path(target_path)

  # If statement evaluating boolean variable.  If the path is True (i.e. we have already created the delta table), then stage in the temp directory. Otherwise, write the table to the lake.
  if bool_test is True:
    print("Delta table exists. Merge needed.") #Log message
      #Run merge
    deltaTable = DeltaTable.forPath(sesh, target_path)
    
    merge_join = None
    
    for index,key_column in enumerate(primary_key):
      if index == 0:
        merge_join = ('tgt.'+key_column+' = src.'+key_column)
      else:
        merge_join = merge_join + (' and tgt.'+key_column+' = src.'+key_column)
    if condition == None:
      deltaTable.alias("tgt").merge(
          source_dataset.alias("src"),
          merge_join) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll()\
        .execute()
      print('Merge completed successfully, merge join was: "'+ merge_join+'"')
    else:
      deltaTable.alias("tgt").merge(
          source_dataset.alias("src"),
          merge_join) \
        .whenMatchedUpdateAll(condition) \
        .whenNotMatchedInsertAll()\
        .execute()
      print('Merge completed successfully, merge join was: "'+ merge_join+'"')


  else:
    print("No delta lake files found. Creating delta lake table & metastore object.")
    print('Schema is as follows:')
    source_dataset.printSchema()
    #Create table
    delta_table = database_name+'.'+table_name
    source_dataset.write.format("delta").mode("overwrite").saveAsTable(delta_table,mode='overwrite',path=target_path)
    print("Table created and in metastore")
  
def soft_delete(id_list,database_name,table_name,target_path,key_columns,column):
    try:
      #Start Spark session
      sesh = s.builder.getOrCreate() 
      # Check if the delta lake table exists
      bool_test = dtf.check_path(target_path)
      if bool_test is True:
        # check if the list have more than 1 key column to create dataframe
        if len(key_columns) > 1:
          rdd = sesh.sparkContext.parallelize(id_list)
          source_dataset = sesh.createDataFrame(rdd).toDF(*key_columns)
        else:
          id_list =  zip(id_list)
          source_dataset = sesh.createDataFrame(id_list, StructType([StructField("id", StringType(), True)]))
        #create a temporary SQL view from the source deltas data frame
        view = "src_"+table_name
        source_dataset.createOrReplaceTempView(view)
        #Build delete SQL
        merge_join = None
        for index,key_column in enumerate(key_columns):
          if index == 0:
            merge_join = ('tgt.'+key_column+' = src.'+key_column)
          else:
            merge_join = merge_join + (' and tgt.'+key_column+' = src.'+key_column)
        sql = f"update {database_name}.{table_name} as tgt set {column} = True where exists (select * from {view} as src where {merge_join})"
        print("Update statement: "+sql)
        deletes = sesh.sql(sql)
        deletes.show()
        print("Soft Delete finished.")
      else:
        print("Table does not exist - no soft deletes performed.")
        pass
    except Exception as e:
      print(str(e))

def delete(source_dataset,database_name,table_name,target_path,primary_key):
  #Start Spark session
  sesh = s.builder.getOrCreate()
  
  # Check if the delta lake table exists
  bool_test = check_path(target_path)
  
  if bool_test is True:
    #Concat delta database & table name
    delta_table = database_name+'.'+table_name
    #create a temporary SQL view from the source deltas data frame
    view = "src_"+table_name
    source_dataset.createOrReplaceTempView(view) 
    #Build delete SQL
    
    merge_join = None

    for index,key_column in enumerate(primary_key):
      if index == 0:
        merge_join = ('tgt.'+key_column+' = src.'+key_column)
      else:
        merge_join = merge_join + (' and tgt.'+key_column+' = src.'+key_column)
    
    sql = "delete from " + delta_table + " as tgt where not exists (select * from " + view + " as src where " + merge_join + ")"
    print("delete statement: "+sql)
    deletes = sesh.sql(sql)
    deletes.show()
    print("Deletes finished.")
  else:
    print("Table does not exist - no deletions performed.")
    pass

def create_database(database_name): 
  #Start Spark session
  sesh = s.builder.getOrCreate()
  if not sesh._jsparkSession.catalog().databaseExists(database_name):
    sesh.sql(f"CREATE DATABASE {database_name};")
    return print(f"Database '{database_name}' has been created in the metastore")
  else:
    return print(f"Database '{database_name}' already exists.")

def database_exists(database_name):  
  #Start Spark session
  sesh = s.builder.getOrCreate()
  if sesh._jsparkSession.catalog().databaseExists(database_name):
    return True
  else:
    return False

def drop_database(database_name):  
  #Start Spark session
  sesh = s.builder.getOrCreate() 
  if sesh._jsparkSession.catalog().databaseExists(database_name):
    sesh.sql(f"DROP DATABASE {database_name};")
    return print(f"Database '{database_name}' has been dropped from the metastore")
  else:
    return print(f"Database '{database_name}' already exists.")

def create_table(database_name,table_name,schema,path):
  #Start Spark session
  sesh = s.builder.getOrCreate()
  if not sesh._jsparkSession.catalog().tableExists(database_name,table_name):
    sesh.sql(f"CREATE TABLE {database_name}.{table_name} ({schema}) USING DELTA LOCATION '{path}';")
    return print(f"Table '{database_name}.{table_name}' has been created in the metastore at path '{path}'.")
  else:
    return print(f"Table '{database_name}.{table_name}' already exists. Cannot create table.")

def table_exists(database_name,table_name):
  #Start Spark session
  sesh = s.builder.getOrCreate()
  if sesh._jsparkSession.catalog().tableExists(database_name,table_name):
    return True
  else:
    return False

def drop_table(database_name,table_name,drop_files_location=False):
  #Start Spark session
  sesh = s.builder.getOrCreate()
  if sesh._jsparkSession.catalog().tableExists(database_name,table_name) and not drop_files_location:
    sesh.sql(f"DROP TABLE {database_name}.{table_name};")
    return print(f"Table '{database_name}.{table_name}' has been dropped from the metastore")
  elif sesh._jsparkSession.catalog().tableExists(database_name,table_name) and drop_files_location:
    loc = sesh.sql(f"describe detail {database_name}.{table_name}").collect()[0]['location']
    sesh.sql(f"DROP TABLE {database_name}.{table_name};")
    dbutils.fs.rm(loc, recurse=True)
    return print(f"Table '{database_name}.{table_name}' has been dropped from the metastore")
  else:
    return print(f"Table '{database_name}.{table_name}' does not exist. Cannot drop table.")
