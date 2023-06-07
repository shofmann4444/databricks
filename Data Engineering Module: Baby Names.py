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

 
help (SparkSession)

# COMMAND ----------

# DBTITLE 1,Code Answer
# Please provide your code answer for Question 1 here

# Use Spark SQL's native JSON support to read the baby names file into a dataframe

#Use this dataframe to create a temporary table containing all the nested data columns ("sid", "id", "position", "created_at", "created_meta", "updated_at", "updated_meta", "meta", "year", "first_name", "county", "sex", "count") so that they can be queried using SQL.

from pyspark.sql import*                  # needed for reading the file created in cells 4 and 5 into a dataframe
from pyspark.sql.functions import col     # needed to be able to refer to col within a dataframe
from pyspark.sql.functions import explode # needed for the explode function

# created entry point
SparkSession 
spark = SparkSession.builder.appName("MyBabyNames").getOrCreate()

#read JSON file into a dataframe 
mydataframe = spark.read.json(baby_names_path, multiLine=True)

#obtain nested data using explode into a second dataframe - names of columns are not identified in schema but defined within the Meta structure 
NestedDataFrame = mydataframe.select(explode(col("data")))

# Create a temp table from the nested dataframe and name it baby_names
NestedDataFrame.createOrReplaceTempView("baby_names")

# Query the baby_names temp table by refering to the ordinal positions of the columns (since the schema offers no further guidence)
results = spark.sql("SELECT baby_names.col[0] as sid,  baby_names.col[1] as id, baby_names.col[2] as position, baby_names.col[3] as created_at, baby_names.col[4] as created_meta, baby_names.col[5] as updated_at, baby_names.col[6] as updated_meta, baby_names.col[7] as meta, baby_names.col[8] as year, baby_names.col[9] as first_name, baby_names.col[10] as county, baby_names.col[11] as sex,  baby_names.col[12] as count FROM baby_names")

# Show the results - Please use display(df) instead of df.show() to display your dataframes.
display(results)


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

# DBTITLE 1,Code Answer
# Please provide your code answer for Question 2 here. You will need separate cells for your SQL answer and your Python or Scala answer.

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC Please provide your brief, written description of your code here.

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
# MAGIC Please write your written answer here.

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
## Hint: the code below will read in the downloaded JSON files. However, the xml column needs to be given structure. Consider using a UDF.
#df = spark.read.option("inferSchema", True).json(visitors_path)


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
