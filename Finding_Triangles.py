import findspark #Since PySpark isn't on sys.path by default
findspark.init() #Adds pyspark to sys.path at runtime

import pyspark
from pyspark import SparkContext #SparkContext class' object is used to connect to a cluster or create RDDs
from pyspark.sql.types import * #To perform SQL operations
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import HiveContext #It is a super-set of the SQLContext, it allows the execution of SQL as well as Hive commands

sc = SparkContext() #Object of class SparkContext is created as sc
sc.setSystemProperty('spark.memory.fraction','0.7') #The purpose of this config is to set aside memory for internal metadata
sc.setSystemProperty('spark.executor.memory','4g') #Defines the amount of memory to use per executor process

sqlContext = HiveContext(sc);
lines = sc.textFile("/data/bitcoin/vin.csv") #RDD creation
header = lines.first() #Accessing the first line of the RDD which is the header
fields = [StructField(field_name, StringType(), True) for field_name in header.split(',')] #Separating the header into fields of type Struct on encountering a comma
schema = StructType(fields)
parts = lines.map(lambda l: l.split(","))
rowRDD = parts.map(lambda p: (p[0], p[1],p[2])) #
sqlContext.createDataFrame(rowRDD,schema).repartition(700).registerTempTable("vin") #Creating the DataFrame with the above schema defined.Repartitioning the DataFrame to increase parallelism.View is also created upon which the SORT BY and GROUP BY operations can be performed.
lines = sc.textFile("/data/bitcoin/vout.csv")
header = lines.first()
fields = [StructField(field_name, StringType(), True) for field_name in header.split(',')]
schema = StructType(fields)
parts = lines.map(lambda l: l.split(","))
rowRDD = parts.map(lambda p: (p[0], p[1],p[2],p[3]))
sqlContext.createDataFrame(rowRDD,schema).repartition(700).registerTempTable("vout")

vn_vt = sqlContext.sql("select vn.txid,vt.pubkey from vin vn JOIN vout vt ON vn.tx_hash = vt.hash and vn.vout = vt.n ") #Joining the two DataFrames(vin and vout) on 2 conditions to get the transaction id and the publicKey associated with that transaction(which is the sender's wallet addresses)
vn_vt.registerTempTable("vn_vt") #Creating a table for the joined DataFrame
df = sqlContext.sql("select DISTINCT vnt.pubkey AS col1, vt.pubkey AS col2 from vn_vt vnt JOIN vout vt ON vnt.txid = vt.hash and vnt.pubkey != vt.pubkey") #Performing a join of the new DataFrame with vout to obtain the receiver's wallet addresses
df = df.sample(False,0.25) #Performing the execution on 25% of the data present on the cluster.
finaldf = df.withColumn('vertA', F.when(df.col2 < df.col1, df.col2).otherwise(df.col1)).withColumn('vertB', F.when(df.col2 < df.col1, df.col1).otherwise(df.col2)).distinct() #Creating a DataFrame with 2 columns to create directed edges by adding condition of src<dst and making the records distinct
dfAB = finaldf.select(finaldf.vertA, finaldf.vertB).repartition(700) # Selecting only the src and dst column and repartition to increase parallelism

dfBC1 = dfAB.select(dfAB.vertA.alias("vertB"),dfAB.vertB.alias("vertC1")) #Creating a new DataFrame by selecting its columns and renaming the columns of this DataFrames to later join them with columns of other DataFrames for forming triangles
dfAC2 = dfAB.select(dfAB.vertA.alias("vertZ"),dfAB.vertB.alias("vertC2"))

dfABC1 = dfAB.join(dfBC1,"vertB").distinct().repartition(700) #Joining the AB and BC DataFrames on a common column which is column B
dfABC1C2 = dfABC1.join(dfAC2,(dfABC1.vertA == dfAC2.vertZ) & (dfABC1.vertC1 ==dfAC2.vertC2))#Joining the DataFrames ABC1 and AC2 on a common column which is A and accumulating common records of column C1 and C2 as C1
dfFinal = dfABC1C2.select(dfABC1C2.vertA,dfABC1C2.vertB,dfABC1C2.vertC1).distinct() #Selecting only the required columns and getting distinct records from final join
dfFinal.write.format('com.databricks.spark.csv').save('triangles_formed.csv') #Storing the results in a .csv file
print(dfFinal.count()) #Display number of triangles formed
