'''
Q:1
A SparkContext represents the entry point to Spark functionality. It's like a key to your car. 
When we run any Spark application, a driver program starts, which has the main function and your SparkContext 
gets initiated here.

steps:
-Print the version of SparkContext in the PySpark shell.
-Print the Python version of SparkContext in the PySpark shell.
-What is the master of SparkContext in the PySpark shell?
'''

# Print the version of SparkContext
print("The version of Spark Context in the PySpark shell is", sc.version)

# Print the Python version of SparkContext
print("The Python version of Spark Context in the PySpark shell is", sc.pythonVer)

# Print the master of SparkContext
print("The master of Spark Context in the PySpark shell is", sc.master)



'''
Q:2
Spark comes with an interactive Python shell in which PySpark is already installed. 
PySpark shell is useful for basic testing and debugging and is quite powerful. 
The easiest way to demonstrate the power of PySpark’s shell is with an exercise. 
In this exercise, you'll load a simple list containing numbers ranging from 1 to 100 in the PySpark shell.

steps:
-Create a Python list named numb containing the numbers 1 to 100.
-Load the list into Spark using Spark Context's parallelize method and assign it to a variable spark_data.
'''

# Create a Python list of numbers from 1 to 100 
numb = range(1, 100)

# Load the list into PySpark  
spark_data = sc.parallelize(numb)



'''
Q:3
In PySpark, we express our computation through operations on distributed collections that are automatically 
parallelized across the cluster. In the previous exercise, you have seen an example of loading a list as 
parallelized collections and in this exercise, you'll load the data from a local file in PySpark shell.

Steps:
Load a local text file README.md in PySpark shell.
'''

# Load a local file into PySpark shell
lines = sc.textFile(file_path)



'''
Q:4
The map() function in Python returns a list of the results after applying the given function to each item of a given 
iterable (list, tuple etc.). The general syntax of map() function is map(fun, iter). We can also use lambda functions 
with map(). Refer to slide 5 of video 1.7 for general help of map() function with lambda().

Steps:
-Print my_list which is available in your environment.
-Square each item in my_list using map() and lambda().
-Print the result of map function.
'''

# Print my_list in the console
print("Input list is", my_list)

# Square all numbers in my_list
squared_list_lambda = list(map(lambda x: x*x, my_list))

# Print the result of the map function
print("The squared numbers are", squared_list_lambda)



'''
Q:5
Another function that is used extensively in Python is the filter() function. 
The filter() function in Python takes in a function and a list as arguments. 
Similar to the map(), filter() can be used with lambda function. 
Refer to slide 6 of video 1.7 for general help of the filter() function with lambda().

Steps:
-Print my_list2 which is available in your environment.
-Filter the numbers divisible by 10 from my_list2 using filter() and lambda().
-Print the numbers divisible by 10 from my_list2.
'''

# Print my_list2 in the console
print("Input list is:", my_list2)

# Filter numbers divisible by 10
filtered_list = list(filter(lambda x: (x%10 == 0), my_list2))

# Print the numbers divisible by 10
print("Numbers divisible by 10 are:", filtered_list)



'''
Q:6
Resilient Distributed Dataset (RDD) is the basic abstraction in Spark. It is an immutable distributed collection of objects. 
Since RDD is a fundamental and backbone data type in Spark, it is important that you understand how to create it.

-Create a RDD named RDD from a Python list of words.
-Confirm the object created is RDD.

'''

# Create an RDD from a list of words
RDD = sc.parallelize(["Spark", "is", "a", "framework", "for", "Big Data processing"])

# Print out the type of the created object
print("The type of RDD is", type(RDD))



'''
Q:7
PySpark can easily create RDDs from files that are stored in external storage devices, such as HDFS 
(Hadoop Distributed File System), Amazon S3 buckets, etc. 
However, the most common method of creating RDD's is from files stored in your local file system. 
This method takes a file path and reads it as a collection of lines. 
In this exercise, you'll create an RDD from the file path (file_path) with the file name README.md

-Print the file_path in the PySpark shell.
-Create a RDD named fileRDD from a file_path.
-Print the type of the fileRDD created.
'''

# Print the file_path
print("The file_path is", file_path)

# Create a fileRDD from file_path
fileRDD = sc.textFile(file_path)

# Check the type of fileRDD
print("The file type of fileRDD is", type(fileRDD))



'''
Q:8
SparkContext's textFile() method takes an optional second argument called minPartitions for specifying the minimum 
number of partitions. In this exercise, you'll create a RDD named fileRDD_part with 5 partitions and then compare 
that with fileRDD that you created in the previous exercise. Refer to the "Understanding Partition" slide in video 2.1 
to know the methods for creating and getting the number of partitions in a RDD.

-Find the number of partitions that support fileRDD RDD.
-Create an RDD named fileRDD_part from the file path but create 5 partitions.
-Confirm the number of partitions in the new fileRDD_part RDD.
'''

# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD.getNumPartitions())

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = sc.textFile(file_path, minPartitions = 5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())



'''
Q:9

'''