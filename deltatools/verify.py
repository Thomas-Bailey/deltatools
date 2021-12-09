# Functions to run checks against the data lake
class verify:
  
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