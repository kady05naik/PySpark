'''
Q1:
1. Import SparkSession
2. Make a new SparkSession called "my_spark"
3. Print my_spark to the console to verify it's a SparkSession.
'''

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.appName('my_spark').getOrCreate()

# Print my_spark
print(my_spark)



'''
Q2:
Create a PySpark DataFrame from the "adult_reduced.csv" file using the spark.read.csv() method.
Show the resulting DataFrame
'''

# Read in the CSV
census_adult = spark.read.csv('adult_reduced.csv', header=True)

# Show the DataFrame
census_adult.show()


'''
Q3:
You have a spreadsheet of Data Scientist salaries from companies ranging is size from small to large. 
You want to see if there is a major differencebetween average salaries grouped by company size.
'''

from pyspark.sql.functions import *

salaries_df=spark.read.csv("salaries.csv", header=True, inferSchema=True)
row_count=salaries_df.count()
print(f"Total rows: {row_count}")

salaries_df.groupBy("company_size").agg({"salary_in_usd": "avg"}).show()
salaries_df.groupBy(col('company_size')).agg(avg('salary_in_usd')).show()
salaries_df.show()



'''
Q:4
Imagine you have a census dataset that you know has a header and a schema. Let's load that dataset and 
let PySpark infer the schema. What do you see if you filter on adults over 40?
'''

# Load the dataframe
census_df = spark.read.json("adults.json")

# Filter rows based on age condition
salary_filtered_census = census_df.filter(census_df['age'] > 40)

# Show the result
salary_filtered_census.show()



'''
Q:5
1. Specify the data schema, giving columns names (age,education_num,marital_status,occupation, and income) 
   and column types, setting a comma for the sep= argument.
2. Read data from a comma-delimited file called adult_reduced_100.csv.
3. Print the schema for the resulting DataFrame.
'''

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Fill in the schema with the columns you need from the exercise instructions
schema = StructType([StructField("age", IntegerType()),
                     StructField("education_num", IntegerType()),
                     StructField("marital_status",StringType()),
                     StructField("occupation",StringType()),
                     StructField("income",StringType()),
                    ])

# Read in the CSV, using the schema you defined above
census_adult = spark.read.csv("adult_reduced_100.csv", sep=',', header=False, schema=schema)

# Print out the schema
census_adult.printSchema()



'''
Q:6
You have a lot of missing values in this dataset! Let's clean it up! With the loaded CSV file,
drop rows with any null values, and show the results!

steps:
-Drop any rows with null values in the census_df DataFrame.
-Show the resulting DataFrame.
'''

# Drop rows with any nulls
census_cleaned = census_df.na.drop()

# Show the result
census_cleaned.show()



'''
Q:7
The census dataset is still not quite showing everything you want it to. 
Let's make a new synthetic column by adding a new column based on existing columns, 
and rename it for clarity.

steps:
-Create a new column, "weekly_salary", by dividing the "income" column by 52.
-Rename the "age" column to "years".
-Show the resulting DataFrame.
'''

#approach (Recommended) 1 ->
from pyspark.sql.functions import col
# Create a new column 'weekly_salary'
census_df_weekly = census_df.withColumn("weekly_salary", col("income") / 52)

# Rename the 'age' column to 'years'
census_df_weekly = census_df_weekly.withColumnRenamed("age", "years")

# Show the result
census_df_weekly.show()


#approach 2->
from pyspark.sql.functions import col
# Create a new column 'weekly_salary'
census_df_weekly = census_df.withColumn("weekly_salary", census_df.income / 52)

# Rename the 'age' column to 'years'
census_df_weekly = census_df_weekly.withColumnRenamed("age", "years")

# Show the result
census_df_weekly.show()

'''
Q:8
You've been hired as a data engineer for a global travel company. 
Your first task is to help the company improve its operations by analyzing flight data. 
You have two datasets in your workspace: one containing details about flights (flights) 
and another with information about destination airports (airports), both are already available in your workspace..

Your goal? Combine these datasets to create a powerful dataset that links each flight to its destination airport.

Steps:
-Examine the airports DataFrame. Note which key column will let you join airports to the flights table.
Join the flights with the airports DataFrame on the "dest" column. Save the result as flights_with_airports.
Examine flights_with_airports again. Note the new information that has been added.

'''

#approach 1 (Recommended)->

# Examine the data
airports.show()

# .withColumnRenamed() renames the "faa" column to "dest"
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the new DataFrame
flights_with_airports.show()



#approach 2->
# Examine the data
airports.show()

# .withColumnRenamed() renames the "faa" column to "dest"
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, "dest", "leftouter")

# Examine the new DataFrame
flights_with_airports.show()



'''
Q:9
This exercise covers UDFs, allowing you to understand function creation in PySpark! 
As you work through this exercise, think about what this would replace in a data cleaning workflow.

Steps:
-Register the function age_category as a UDF called age_category_udf.
-Add a new column to the DataFrame df called "category" that applies the UDF to categorize people based on their age. 
The argument for age_category_udf() is provided for you.
-Show the resulting DataFrame.
'''

# Register the function age_category as a UDF
age_category_udf = udf(age_category, StringType())

# Apply your udf to the DataFrame
age_category_df_2 = age_category_df.withColumn("category", age_category_udf(age_category_df["age"]))

# Show df
age_category_df_2.show()



'''
Q:10
This exercise covers Pandas UDFs, so that you can practice their syntax! 
As you work through this exercise, notice the differences between the 
Pyspark UDF from the last exercise and this type of UDF.
'''

# Define a Pandas UDF that adds 10 to each element in a vectorized way
@pandas_udf(DoubleType())
def add_ten_pandas(column):
    return column + 10

# Apply the UDF and show the result
df.withColumn("10_plus", add_ten_pandas(df.value))
df.show()



'''
Q:11
In PySpark, you can create an RDD (Resilient Distributed Dataset) in a few different ways. 
Since you are already familiar with DataFrames, you will set this up using a DataFrame. 

Steps:
-Create a DataFrame from the provided list called df.
-Convert the DataFrame to an RDD.
-Collect and print the resulting RDD.
'''

# Create a DataFrame
df = spark.read.csv("salaries.csv", header=True, inferSchema=True)

# Convert DataFrame to RDD
rdd = df.rdd

# Show the RDD's contents
rdd.collect()
print(rdd)



'''
Q:12
For this exercise, you’ll work with both RDDs and DataFrames in PySpark. 
The goal is to group data and perform aggregation using both RDD operations and DataFrame methods.

You will load a CSV file containing employee salary data into PySpark as an RDD. 
You'll then group by the experience level data and calculate the maximum salary for each experience level from a DataFrame. 
By doing this, you'll see the relative strengths of both data formats.

The dataset you're using is related to Data Scientist Salaries, so finding market trends are in your best interests!

Steps:
-Create an RDD from a DataFrame.
-Collect and display the results of the RDD and DataFrame.
-Group by the "experience_level" and calculate the maximum salary for each.
'''

# Create an RDD from the df_salaries
rdd_salaries = df_salaries.rdd

# Collect and print the results
print(rdd_salaries.collect())

# Group by the experience level and calculate the maximum salary
dataframe_results = df_salaries.groupby("experience_level").agg({"salary_in_usd": 'max'})

# Show the results
dataframe_results.show()



'''
Q:13
In this exercise, you'll practice registering a DataFrame as a temporary SQL view in PySpark. 
Temporary views are powerful tools that allow you to query data using SQL syntax, 
making complex data manipulations easier and more intuitive. 
Your goal is to create a view from a provided DataFrame and run SQL queries against it, 
a common task for ETL and ELT work

-Register a new view called "data_view" from the DataFrame df.
-Run the provided SQL query to calculate total salary by position.
'''

# Register as a view
df.createOrReplaceTempView("data_view")

# Advanced SQL query: Calculate total salary by Position
result = spark.sql("""
    SELECT Position, SUM(Salary) AS Total_Salary
    FROM data_view
    GROUP BY Position
    ORDER BY Total_Salary DESC
    """
)
result.show()



'''
Q:14
DataFrames can be easily manipulated using SQL queries in PySpark. 
The .sql() method in a SparkSession enables applications to run SQL queries programmatically 
and returns the result as another DataFrame. In this exercise, you'll create a temporary table of a 
DataFrame that you have created previously, then construct a query to select the names of the people 
from the temporary table and assign the result to a new DataFrame.

Steps
-Create a temporary table named "people" from the df DataFrame.
-Construct a query to select the names of the people from the temporary table people.
-Assign the result of Spark's query to a new DataFrame called people_df_names.
-Print the top 10 names of the people from people_df_names DataFrame.
'''

# Create a temporary table "people"
df.createOrReplaceTempView("people")

# Select the names from the temporary table people
query = """SELECT name FROM people"""

# Assign the result of Spark's query to people_df_names
people_df_names = spark.sql(query)

# Print the top 10 names of the people
people_df_names.show(10)



'''
Q:15
SQL queries are concise and easy to run compared to DataFrame operations. 
But in order to apply SQL queries on a DataFrame first, you need to create a temporary 
view of the DataFrame as a table and then apply SQL queries on the created table.

-Create temporary table "salaries_table" from salaries_df DataFrame.
-Construct a query to extract the "job_title" column from company_location in Canada ("CA").
-Apply the SQL query and create a new DataFrame canada_titles.
'''

# Create a temporary view of salaries_table
salaries_df.createOrReplaceTempView('salaries_table')

# Construct the "query"
query = '''SELECT job_title, salary_in_usd FROM salaries_table 
WHERE company_location == "CA"'''

# Apply the SQL "query"
canada_titles = spark.sql(query)

# Generate basic statistics
canada_titles.describe().show()



'''
Q:16
Now you're ready to do some aggregating of your own! You're going to use a salary dataset that you have already used. 
Let's see what aggregations you can create! A SparkSession called spark is already in your workspace, 
along with the Spark DataFrame salaries_df.
-Find the minimum salary at a US, Small company - 
    performing the filtering by referencing the column directly ("salary_in_usd"), not passing a SQL string.
-Find the maximum salary at a US, Large company, denoted by a "L" - 
    performing the filtering by referencing the column directly ("salary_in_usd"), not passing a SQL string.
'''
