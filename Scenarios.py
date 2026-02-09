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


'''