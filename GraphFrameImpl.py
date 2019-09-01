import findspark
findspark.init()

import pyspark
from pyspark import SparkContext
import graphframes
from graphframes import *

from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import HiveContext

sc = SparkContext()
sc.setSystemProperty('spark.memory.fraction','0.7')
sc.setSystemProperty('spark.executor.memory','4g')

sqlContext = HiveContext(sc);
lines = sc.textFile("/data/bitcoin/vin.csv")
header = lines.first()
fields = [StructField(field_name, StringType(), True) for field_name in header.split(',')]
schema = StructType(fields)
parts = lines.map(lambda l: l.split(","))
rowRDD = parts.map(lambda p: (p[0], p[1],p[2]))
sqlContext.createDataFrame(rowRDD,schema).repartition(700).registerTempTable("vin")
lines = sc.textFile("/data/bitcoin/vout.csv")
header = lines.first()
fields = [StructField(field_name, StringType(), True) for field_name in header.split(',')]
schema = StructType(fields)
parts = lines.map(lambda l: l.split(","))
rowRDD = parts.map(lambda p: (p[0], p[1],p[2],p[3]))
sqlContext.createDataFrame(rowRDD,schema).repartition(700).registerTempTable("vout")
vn_vt = sqlContext.sql("select vn.txid,vt.pubkey from vin vn JOIN vout vt ON vn.tx_hash = vt.hash and vn.vout = vt.n ")
vn_vt.registerTempTable("vn_vt")
df = sqlContext.sql("select DISTINCT vnt.pubkey AS col1, vt.pubkey AS col2 from vn_vt vnt JOIN vout vt ON vnt.txid = vt.hash and vnt.pubkey != vt.pubkey")
finaldf = df.withColumn('src', F.when(df.col2 < df.col1, df.col2).otherwise(df.col1)).withColumn('dst', F.when(df.col2 < df.col1, df.col1).otherwise(df.col2)).distinct()#Creating a DataFrame with two columns 'src' and 'dst', since these are the names of columns accepted by GraphFrame by default
edges_df = finaldf.select(finaldf.src, finaldf.dst)#Creating the new DataFrame with 2 columns 'src' and 'dst' to be defined as edges
vert_df = finaldf.select(finaldf.src).unionAll(finaldf.select(finaldf.dst)).distinct()#Geting all the records of the 'src' and 'dst' columns into one column to get a list of all the vertices
vert_df = vert_df.select(finaldf.src.alias("id"))#Renaming the name of the column as 'id', since it is the default name accepted by GraphFrame
g = GraphFrame(vert_df, edges_df)#Passing the 2 DataFrames as arguments and creating a GraphFrame
triangles = g.find("(A)-[]->(B);(B)-[]->(C);(A)-[]->(C)").distinct()#Using find() to process through the GraphFrame and finding out triangular transaction.
print(triangles.count())#Printing the number of triangles formed.
