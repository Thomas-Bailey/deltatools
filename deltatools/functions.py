from pyspark.sql.functions import lit, substring, sha2, concat_ws
from pyspark.sql import SparkSession
from delta.tables import *


# Functions to run checks against the data lake
class verify_lake:
  
  #Initialise variables
  def __init__(self,path):
    self.path = path
  
  #check if path exists
  def check_path(self):
    try:  
      dbutils.fs.ls(self.path)
      return True
    except:
      return False

class load_deltas:
  
  # Initialise class variables
  def __init__(self,source_path,target_path,primary_key,database_name,table_name):
    self.source_path = source_path
    self.target_path = target_path
    self.primary_key = primary_key
    self.database_name = database_name
    self.table_name = table_name

  #This method is just to show some validations of what will be running under other methods.
  def info(self):
    print('source_path : '+self.source_path)
    print('target_path : '+self.target_path)
    print('key_column(s) : '+self.primary_key)
    
    
    merge_join = ''
    
    for index,key_column in enumerate(self.primary_key):
      if index == 0:
        merge_join = ('tgt.'+key_column+' = src.'+key_column)
      else:
        merge_join = merge_join + (' AND tgt.'+key_column+' = src.'+key_column)
      
    print('merge join is: '+merge_join)

  #Run merge from source to target
  def upsert(self):
    #Read delta file as a data frame
    source_deltas = spark.read.parquet(self.source_path)
    # Add a SHA2 row hash column to enable delta indentification
    source_deltas = source_deltas.withColumn("row_hash",sha2(concat_ws("||", *df.columns), 256))
    
    #Determine if merge or new load
    # Check if the delta lake table exists
    bool_test = verify_lake(self.target_path).check_path()

    # If statement evaluating boolean variable.  If the path is True (i.e. we have already created the delta table), then stage in the temp directory. Otherwise, write the table to the lake.
    if bool_test is True:
      print("Delta table exists. Merge needed.") #Log message
       #Run merge
      deltaTable = DeltaTable.forPath(spark, self.target_path)

      merge_join = None

      for index,key_column in enumerate(self.primary_key):
        if index == 0:
          merge_join = ('tgt.'+key_column+' = src.'+key_column)
        else:
          merge_join = merge_join + (' AND tgt.'+key_column+' = src.'+key_column)

      deltaTable.alias("tgt").merge(
          source_deltas.alias("src"),
          merge_join) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll()\
        .execute()
      print('Merge completed successfully, merge join was: "'+ merge_join+'"')

    else:
      print("No delta lake files found. Creating parquet structure, you will need to run a CREATE TABLE statement in Databricks with the same schema.")
      print('Schema is as follows:')
      source_deltas.printSchema()
      #Create table
      delta_table = self.database_name+'.'+self.table_name
      source_deltas.write.format("delta").mode("overwrite").saveAsTable(delta_table,mode='overwrite',path=self.target_path)
      print("Table created and in metastore")
      
      
       
  def delete(self):
    # Check if the delta lake table exists
    bool_test = verify_lake(self.target_path).check_path()
    if bool_test is True:
      #Concat delta database & table name
      delta_table = self.database_name+'.'+self.table_name
      #create a temporary SQL view from the source deltas data frame
      view = "src_"+self.table_name
      source_deltas = spark.read.parquet(self.source_path).createOrReplaceTempView(view) 
      #Build delete SQL
      merge_join = None

      for index,key_column in enumerate(self.primary_key):
        if index == 0:
          merge_join = ('tgt.'+key_column+' = src.'+key_column)
        else:
          merge_join = merge_join + (' and tgt.'+key_column+' = src.'+key_column)
      
      sql = "delete from " + delta_table + " as tgt where not exists (select * from " + view + " as src where " + merge_join + ")"
      print("delete statement: "+sql)
      sesh = SparkSession.builder.getOrCreate()
      deletes = sesh.sql(sql)
      deletes.show()
      print("Deletes finished.")
    else:
      print("Table does not exists - no deletions performed.")
      pass
      
