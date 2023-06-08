# Databricks notebook source
# MAGIC %md # SA coding assessment: Data Engineering, Baby Names
# MAGIC ## Version 2022.02
# MAGIC
# MAGIC What you'll do:
# MAGIC * We provide the dataset. You will load it into dataframes, and perform some data cleansing and transformation tasks.
# MAGIC * You will answer a series of questions to show insights from the data.
# MAGIC * There are also some written-answer questions.
# MAGIC
# MAGIC *We care about the process, not the result.*  I.e., we're looking for proper use of data engineering techniques and understanding of the code you've written.  
# MAGIC
# MAGIC This Data Engineering section is scored out of 50 points.

# COMMAND ----------

# DBTITLE 1,Setup Env
# This folder is for you to write any data as needed. Write access is restricted elsewhere. You can always read from dbfs.
aws_role_id = "AROAUQVMTFU2DCVUR57M2"
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
userhome = f"s3a://e2-interview-user-data/home/{aws_role_id}:{user}"
print(userhome)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Baby Names Data Set
# MAGIC
# MAGIC This dataset comes from a website referenced by [Data.gov](http://catalog.data.gov/dataset/baby-names-beginning-2007). It lists baby names used in the state of NY from 2007 to 2018.
# MAGIC
# MAGIC Run the following two cells to copy this file to a usable location.

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC
# MAGIC val tmpFile = new File("/tmp/rows.json")
# MAGIC FileUtils.copyURLToFile(new URL("https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json?accessType=DOWNLOAD"), tmpFile)

# COMMAND ----------

# https://docs.python.org/3/library/hashlib.html#blake2
from hashlib import blake2b

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
h = blake2b(digest_size=4)
h.update(user.encode("utf-8"))
display_name = "user_" + h.hexdigest()
print("Display Name: " + display_name)

# dbutils.fs.cp('file:/tmp/rows.json', userhome + '/rows.json')
# dbutils.fs.cp(userhome + '/rows.json' ,f"dbfs:/tmp/{display_name}/rows.json")

# SRH - 6 Jun 2023 - copy the data from file:/tmp/rows.json to dbfs:/tmp/{display_name}/rows.json
# not sure why it needed to be put here: userhome + '/rows.json'
dbutils.fs.cp('file:/tmp/rows.json', f"dbfs:/tmp/{display_name}/rows.json") # copy from file to this location directly
baby_names_path = f"dbfs:/tmp/{display_name}/rows.json"

print("Baby Names Path: " + baby_names_path)
dbutils.fs.head(baby_names_path)

# Ensure you use baby_names_path to answer the questions. A bug in Spark 2.X will cause your read to fail if you read the file from userhome. 
# Please note that dbfs:/tmp is cleaned up daily at 6AM pacific

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Baby Names Question 1 - Nested Data [15 Points]
# MAGIC
# MAGIC
# MAGIC Use Spark SQL's native JSON support to read the baby names file into a dataframe. Use this dataframe to create a temporary table containing all the nested data columns ("sid", "id", "position", "created_at", "created_meta", "updated_at", "updated_meta", "meta", "year", "first_name", "county", "sex", "count") so that they can be queried using SQL. 
# MAGIC
# MAGIC Hint: you can use ```dbutils.fs.head(baby_names_path)``` to take a look at the dataset before reading it in. 
# MAGIC
# MAGIC Suggested Steps:
# MAGIC 1. Read in the JSON data
# MAGIC 2. Pull all columns in the nested data column to top level, following the schema specified above. There are [built-in Spark SQL functions](https://spark.apache.org/docs/latest/api/sql/index.html) that will accomplish this.
# MAGIC 3. Create a temp table from this expanded dataframe using createOrReplaceTempView()
# MAGIC

# COMMAND ----------

#    debug - showing schema
# SELECT schema_of_json(baby_names_path);
mydataframe = spark.read.json(baby_names_path, multiLine=True)
mydataframe.printSchema


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I used a another cell at various times to run debugging checks like the following to view schema:
# MAGIC
# MAGIC   mydataframe = spark.read.json(baby_names_path, multiLine=True)
# MAGIC   mydataframe.printSchema
# MAGIC
# MAGIC
# MAGIC The dbutils.fs.head(baby_names_path) command does not show enough of the JSON to see what is going on. I downloaded the JSON from source in browser to examine it: 
# MAGIC https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json?accessType=DOWNLOAD .
# MAGIC
# MAGIC I saw how there was data at the end under the data structure that coincided with the columns defined in the meta structure. I used the columns from the meta struct to determine the ordinal position of the data in the 'data' struct:
# MAGIC
# MAGIC "columns" : 
# MAGIC [ 
# MAGIC {        "id" : -1,        "name" : "sid",        "dataTypeName" : "meta_data", "fieldName" : ":sid",
# MAGIC {        "id" : -1,        "name" : "id",        "dataTypeName" : "meta_data",        "fieldName" : ":id"
# MAGIC {   "id" : -1,        "name" : "position",        "dataTypeName" : "meta_data", "fieldName" : ":position",        
# MAGIC { "id" : -1,        "name" : "created_at",    "dataTypeName" : "meta_data",    "fieldName" : ":created_at",   
# MAGIC { "id" : -1,        "name" : "created_meta",        "dataTypeName" : "meta_data",        "fieldName" : ":created_meta", 
# MAGIC { "id" : -1,        "name" : "updated_at",        "dataTypeName" : "meta_data",        "fieldName" : ":updated_at", 
# MAGIC {  "id" : -1,        "name" : "updated_meta",        "dataTypeName" : "meta_data",        "fieldName" : ":updated_meta",  
# MAGIC { "id" : -1,  "name" : "meta",        "dataTypeName" : "meta_data",        "fieldName" : ":meta",        "position"  
# MAGIC {  "id" : 361599813,        "name" : "Year",        "dataTypeName" : "number",          
# MAGIC
# MAGIC
# MAGIC Here is a mapping of the column definitions in the meta structure to the actual columns in the data structure and the ordinal positions: 
# MAGIC
# MAGIC        sid [0]                       id [1]                position [2]    
# MAGIC "row-emfw_sfk5_5wtx", "00000000-0000-0000-3154-4394D27F2559", 0,
# MAGIC
# MAGIC created_at[3]  created_meta[4] updated_at[5]   updated_meta [6]
# MAGIC 1682529128,     null,         1682529128,         null, 
# MAGIC
# MAGIC meta[7]      Year [8]    First Name [9]     County [10]   sex [11]     Count [12]
# MAGIC "{ }",       "2007",        "ZOEY",        "KINGS",       "F",       "11" ]
# MAGIC

# COMMAND ----------

# DBTITLE 1,Code Answer
# Please provide your code answer for Question 1 here

# Use Spark SQL's native JSON support to read the baby names file into a dataframe

#Use this dataframe to create a temporary table containing all the nested data columns ("sid", "id", "position", "created_at", "created_meta", "updated_at", "updated_meta", "meta", "year", "first_name", "county", "sex", "count") so that they can be queried using SQL.

from pyspark.sql import*                  # needed for reading the file created in cells 4 and 5 into a dataframe
from pyspark.sql.functions import col     # needed to be able to refer to col within a dataframe
from pyspark.sql.functions import explode # needed for the explode function

# created entry point
spark = SparkSession.builder.appName("MyBabyNames").getOrCreate()

#read JSON file into a dataframe 
mydataframe = spark.read.json(baby_names_path, multiLine=True)

#obtain nested data using explode into a second dataframe - names of columns are not identified in schema but defined within the Meta structure 
NestedDataFrame = mydataframe.select(explode(col("data").alias("mydata")))

# Create a temp table from the nested dataframe and name it baby_names
NestedDataFrame.createOrReplaceTempView("baby_names")

# Query the baby_names temp table by refering to the ordinal positions of the columns (since the schema offers no further guidence)
bndataframe = spark.sql("SELECT baby_names.col[0] as sid,  baby_names.col[1] as id, baby_names.col[2] as position, baby_names.col[3] as created_at, baby_names.col[4] as created_meta, baby_names.col[5] as updated_at, baby_names.col[6] as updated_meta, baby_names.col[7] as meta, baby_names.col[8] as year, baby_names.col[9] as first_name, baby_names.col[10] as county, baby_names.col[11] as sex,  baby_names.col[12] as count FROM baby_names")

# create another temp table with the alias column names 
bndataframe.createOrReplaceTempView("temp_baby_names")

# Show the results - using display(df) instead of df.show() 
display(spark.sql("SELECT * from temp_baby_names limit 5"))


# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC Please provide your brief, written description of your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 2 - Multiple Languages [10 Points]
# MAGIC
# MAGIC Using the temp table you created in the question above, write a SQL query that gives the most popular baby name for each year in the dataset. Then, write the same query using either the Scala or Python dataframe APIs.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT first_name, year, namecount
# MAGIC FROM (
# MAGIC   SELECT first_name, year, COUNT(first_name) as namecount,
# MAGIC   RANK() OVER (PARTITION BY year ORDER BY COUNT(first_name) DESC) as rank
# MAGIC   FROM temp_baby_names
# MAGIC   GROUP BY year, first_name
# MAGIC ) as subquery
# MAGIC WHERE rank = 1  
# MAGIC ORDER BY year;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Code Answer
from pyspark.sql.functions import count, col, rank 
from pyspark.sql.window import Window

# Load the temp baby table into a DataFrame
df = spark.table("temp_baby_names")

# Calculate the number of occurrences of each first_name for each year and store in a dataframe
namecount_df = df.groupBy("year", "first_name").agg(count("first_name").alias("namecount"))

# Assign a rank to each row based on the count within each year from the namecount_df dataframe
ranked_df = namecount_df.withColumn("rank", rank().over(Window.partitionBy("year").orderBy(col("namecount").desc())))

# Filter rows where the rank is equal to 1 - and put the 1's into a dataframe
result_df = ranked_df.filter(col("rank") == 1).orderBy("year")

#  Display the ranks = 1 dataframe contents
display(result_df)



# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions.{count, col, rank}
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC
# MAGIC // Load the temp baby table into a DataFrame
# MAGIC val df = spark.table("temp_baby_names")
# MAGIC
# MAGIC // Calculate the number of occurrences of each first_name for each year and store in a dataframe
# MAGIC val namecount_df = df.groupBy("year", "first_name").agg(count("first_name").alias("namecount"))
# MAGIC
# MAGIC // Assign a rank to each row based on the count within each year from the namecount_df dataframe
# MAGIC val ranked_df = namecount_df.withColumn("rank", rank().over(Window.partitionBy("year").orderBy(col("namecount").desc)))
# MAGIC
# MAGIC // Filter rows where the rank is equal to 1 - and put the 1's into a dataframe
# MAGIC val result_df = ranked_df.filter(col("rank") === 1).orderBy("year")
# MAGIC
# MAGIC // Display the ranks = 1 dataframe contents
# MAGIC display(result_df)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC
# MAGIC I originally tried to use aggregate SQL functions to achieve the result but I found using the Rank() function with OVER to be easier and likely better performing. The SQL solution required the use of a subquery to find the highest counts of a given name for a given year. The top query uses a where condition to only select the subquery results that were ranked the highest, i.e. having a rank value of 1.
# MAGIC
# MAGIC I used the same basic solution for both Python and Scala.
# MAGIC
# MAGIC First, count the occurrences for each name for a given year. In SQL, this was accomplished using a subquery with the rank/over/partition function with year and count and a groupby of year and name. In Scala and Python, the name/occurrence count by year was accomplished with the groupby (on name and year) and agg (count of name) functions.  
# MAGIC
# MAGIC Second, in the SQL, the query used the subquery as a source of ranked counts of names for years so I filtered the results using a Where with rank = 1 and ordered the results for readability. In Scala and Python, the ranked results were put into a new dataframe. Then another dataframe was created with the ranked results filtered to contain only a value of 1 and ordered by year for readability.
# MAGIC
# MAGIC It should be noted that there are ties between some names for some years based on the counts of occurrence so you will see multiple names for a given year for some years e.g. Jacob and Madison for 2008.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 3 - Performance [10 Points]
# MAGIC
# MAGIC Are there any performance considerations when choosing a language API (SQL vs Python vs Scala) in the context of Spark?
# MAGIC
# MAGIC Are there any performance considerations when using different data representations (RDD, Dataframe) in Spark? Please explain, and provide references if possible. No code answer is required.

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC
# MAGIC The observed performance showed the following ranks from fastest to slowest:
# MAGIC 1)	Python
# MAGIC 2)	Scala
# MAGIC 3)	SQL
# MAGIC
# MAGIC My Python and Scala solutions were likely to be inefficient since my code created a dataframe for each data manipulation step. I would have thought SQL would be the most efficient but subqueries do make SQL much less efficient, yet the Python and Scala solutions were performing similar rank and aggregation operations.
# MAGIC
# MAGIC Spark is executing all three forms of code so optimizations could have been gained by caching in memory the temporary table or experimenting with Spark’s optimization options.
# MAGIC https://spark.apache.org/docs/latest/sql-performance-tuning.html
# MAGIC
# MAGIC PySpark is converted to Spark SQL and then executed on a JVM cluster. Spark was written in Scala so it would seem that Scala may require less translation when calling Spark APIs. Use of UDFs significantly slows operations down. Scala, PySpark and Spark SQL are First Class Spark citizens.
# MAGIC
# MAGIC Despite all of these issues, it still seems like PySpark is the fasted both in my observations and based on general user sentiment in online forums. I was not able to locate a recent study (less than 5 years old) providing more guidance on this topic.  
# MAGIC https://stackoverflow.com/questions/32464122/spark-performance-for-scala-vs-python 
# MAGIC https://www.databricks.com/blog/2015/04/24/recent-performance-improvements-in-apache-spark-sql-python-dataframes-and-more.html
# MAGIC https://towardsdatascience.com/faster-spark-queries-with-the-best-of-both-worlds-python-and-scala-7cd0d49b7561
# MAGIC https://mindfulmachines.io/blog/2018/6/apache-spark-scala-vs-java-v-python-vs-r-vs-sql26
# MAGIC
# MAGIC
# MAGIC Use Dataframes/Datasets or Spark SQL is far more optimal than RDD.  
# MAGIC https://phoenixnap.com/kb/rdd-vs-dataframe-vs-dataset
# MAGIC
# MAGIC "A DataFrame is a Dataset organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs. The DataFrame API is available in Scala, Java, Python, and R. In Scala and Java, a DataFrame is represented by a Dataset of Rows. In the Scala API, DataFrame is simply a type alias of Dataset[Row]. While, in Java API, users need to use Dataset<Row> to represent a DataFrame." from https://spark.apache.org/docs/latest/sql-programming-guide.html
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 4 - Nested XML [15 Points]
# MAGIC Imagine that a new upstream system now automatically adds an XML field to the JSON baby dataset.  The added field is called visitors. It contains an XML string with visitor information for a given birth. We have simulated this upstream system by creating another JSON file with the additional field.  
# MAGIC
# MAGIC Using the JSON dataset at dbfs:/interview-datasets/sa/births/births-with-visitor-data.json, do the following:
# MAGIC 0. Read the births-with-visitor-data.json file into a dataframe and parse the nested XML fields into columns and print the total record count.
# MAGIC 0. Find the county with the highest average number of visitors across all births in that county
# MAGIC 0. Find the average visitor age for a birth in the county of KINGS
# MAGIC 0. Find the most common birth visitor age in the county of KINGS
# MAGIC

# COMMAND ----------

visitors_path = "/interview-datasets/sa/births/births-with-visitor-data.json"

# COMMAND ----------

# DBTITLE 1,#1 - Code Answer

import xml.etree.ElementTree as ET
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

# Define the schema for the visitors fields
visitor_schema = StructType([
    StructField("id", StringType()),
    StructField("age", IntegerType()),
    StructField("sex", StringType())
])
visitors_schema = ArrayType(visitor_schema)

# Define a UDF to parse the XML data
def parse_visitors(xml_string):
    root = ET.fromstring(xml_string)
    visitors = []
    for visitor in root.findall("visitor"):
        visitors.append({
            "id": visitor.get("id"),
            "age": int(visitor.get("age")),
            "sex": visitor.get("sex")
        })
    return visitors

parse_visitors_udf = udf(parse_visitors, visitors_schema)

# Load the data into a DataFrame
df = spark.read.option("inferSchema", True).json(visitors_path)
InitialRowCount = df.count() # get count for comparison

# Parse the XML data in the visitors field
df = df.withColumn("visitors", parse_visitors_udf(col("visitors")))

# Select the new columns and existing columns
df = df.select(
    "county", "created_at","first_name","id","meta","name_count", "position", "sex", "sid",
    "updated_at", explode(col("visitors")).alias("visitor"), "year")

#alias the visitor fields so that we know they were for visitors
df = df.select(
    "county", "created_at","first_name","id","meta","name_count", "position", "sex", "sid",
    "updated_at", col("visitor.id").alias("visitor_id"), col("visitor.age").alias("visitor_age"),
    col("visitor.sex").alias("visitor_sex") , "year")

# Show the resulting DataFrame and total count
display(df)
print(f"Began with {InitialRowCount} rows. Row count after visitor rows added: {df.count()}")

# COMMAND ----------

# DBTITLE 1,#2 - Code Answer
## Hint: check for inconsistently capitalized field values. It will make your answer incorrect.

# COMMAND ----------

# DBTITLE 1,#3 - Code Answer
## Hint: check for inconsistently capitalized field values. It will make your answer incorrect.

# COMMAND ----------

# DBTITLE 1,#4 - Code Answer
## Hint: check for inconsistently capitalized field values. It will make your answer incorrect.

# COMMAND ----------

# DBTITLE 1,#4 - Written Answer
# MAGIC %md
# MAGIC Please provide your written answer for Question 4 here
