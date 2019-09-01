import findspark
findspark.init()

import pyspark
from pyspark import SparkContext

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
df = sqlContext.sql("select vnt.pubkey AS sender,SUM(vt.value) AS amount from vn_vt vnt JOIN vout vt ON vnt.txid = vt.hash where vt.pubkey = '{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}' GROUP BY vnt.pubkey ORDER BY amount DESC") #Selecting the PublicKey from the vout DataFrame which is the senders wallet address, along with the amount and cumulating the amount. Performing a join function where ever the publicKey in the vout DataFrame(Wallet address of the receiver) matches the wallet address of Wikileaks and ordering the result in descending order.
df.write.format('com.databricks.spark.csv').save('wikileaks_results.csv') #Saving the result in a .csv file
df.head(30)#Getting the wallet addresses  of the top 30 donors
