from pyspark.sql.functions import lit, substring, sha2, concat_ws
from pyspark.sql import SparkSession as s
from delta.tables import *
import IPython

dbutils = IPython.get_ipython().user_ns["dbutils"]


#check if path exists
def check_path(path):
  try:  
    dbutils.fs.ls(path)
    return True
  except:
    return False

def hash(source_path):
  #Start Spark session
  sesh = s.builder.getOrCreate()

  #Read delta file as a data frame
  source_deltas = sesh.read.parquet(source_path)
  
  # Add a SHA2 row hash column to enable delta indentification
  source_deltas = source_deltas.withColumn("row_hash",sha2(concat_ws("||", *source_deltas.columns), 256))

  return source_deltas

#Run merge from source to target
def upsert(source_dataset,database_name,table_name,target_path,primary_key):
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

    deltaTable.alias("tgt").merge(
        source_dataset.alias("src"),
        merge_join) \
      .whenMatchedUpdateAll() \
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
    
