# import pyspark
# from pyspark.sql import SparkSession

# spark = SparkSession.builder\
#            .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0')\
#            .getOrCreate()

# df = spark.read.format('jdbc') \
#         .options(driver='org.sqlite.JDBC', dbtable='Country',
#                  url='jdbc:sqlite:databse.sqlite')\
#         .load()

# print("*" * 100)
# print(df)

from pyspark import SparkConf, SparkContext, SQLContext
conf = SparkConf().setAll([('spark.driver.extraClassPath','sqlite-jdbc-3.36.0.3.jar')])


sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.format('jdbc').\
     options(url='jdbc:sqlite:database.sqlite',\
     dbtable='Player',driver='org.sqlite.JDBC').load()

df.printSchema()