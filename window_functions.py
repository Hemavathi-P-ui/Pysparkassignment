import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import col, when,year,month,dayofweek,row_number,to_timestamp,coalesce,dayofmonth,date_format,date_add,date_sub
from pyspark.sql.functions import weekofyear,quarter,current_date,add_months,round,date_trunc,expr,dense_rank
from pyspark.sql.functions import col, initcap,when, to_date, datediff, lit,sum,avg,count, min,max,lead,lag,rank,desc,last_day,months_between
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = "C:/Users/002PV2744/Documents/Python/Python37/python.exe"
conf = SparkConf()
conf.set("spark.app.name","newcode")
conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

data = [
("Karthik", "Sales", 2023, 1200),
("Ajay", "Marketing", 2022, 2000),
("Vijay", "Sales", 2023, 1500),
("Mohan", "Marketing", 2022, 1500),
("Veer", "Sales", 2021, 2500),
("Ajay", "Finance", 2023, 1800),
("Kiran", "Sales", 2023, 1200),
("Priya", "Finance", 2023, 2200),
("Karthik", "Sales", 2022, 1300),
("Ajay", "Marketing", 2023, 2100),
("Vijay", "Finance", 2022, 2100),
("Kiran", "Marketing", 2023, 2400),
("Mohan", "Sales", 2022, 1000)
]
data_df = spark.createDataFrame(data,["employees","department","year","salary"])

# 1. For each department, assign ranks to employees based on their salary in descending order for each year.
# ===============================================================================================================
window = Window.partitionBy("department","year").orderBy(col("salary").desc())
df = data_df.withColumn("rank",rank().over(window)).show()


# 2. In the "Sales" department, rank employees based on their salary. If two employees have the same salary, they should share the same rank.
# ===============================================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").desc())
df = data_df.filter(col("department") == "Sales").withColumn("rank",rank().over(window)).show()

# 3. For each department, assign a row number to each employee based on the year, ordered by salary in descending order.
# ===============================================================================================================================
window = Window.partitionBy("department","year").orderBy(col("salary").desc())
df = data_df.withColumn("row",row_number().over(window)).show()

# 4. Rank employees across all departments based on their salary within each year.
# ===============================================================================================================================
window = Window.partitionBy("year").orderBy(col("salary").desc())
df = data_df.withColumn("rank",rank().over(window)).show()

# 5. Rank employees by their salary in descending order. If multiple employees have the same salary, ensure they receive unique rankings without gaps.
# ====================================================================================================================================================
window = Window.orderBy(col("salary").desc())
df = data_df.withColumn("dense_rank",dense_rank().over(window)).show()


# 6. For each year, rank employees in the "Marketing" department based on salary, but without any gaps in ranks if salaries are tied.
# ===============================================================================================================================
window = Window.partitionBy("year").orderBy(col("salary").desc())
df = data_df.filter(col("department") == "Marketing").withColumn("dense_rank",dense_rank().over(window)).show()

# 7. For each year, assign row numbers to employees based on salary in ascending order within each department.
# ===============================================================================================================================
window = Window.partitionBy("year","department").orderBy(col("salary"))
df = data_df.withColumn("row",row_number().over(window)).show()


# 8. Within each department, assign a dense rank to employees based on their salary for each year.
# ===============================================================================================================================
window = Window.partitionBy("year","department").orderBy(col("salary"))
df = data_df.withColumn("denserank",dense_rank().over(window)).show()

# 9. Identify the top 3 highest-paid employees in each department for the year 2023 using any ranking function.
# ===============================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").desc())
df = data_df.filter(col("year") == 2023).withColumn("rank",rank().over(window)).filter(col("rank") <=3 ).show()

# 10. For the "Finance" department, list employees in descending order of salary, showing their relative ranks without any gaps.
# ===============================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").desc())
df = data_df.filter(col("year") =="Finance").withColumn("denserank",rank().over(window)).limit(3).show()

# 11. Rank employees across departments based on their salary, grouping by year, and sort by department as the secondary sorting criteria.
# ===============================================================================================================================
window = Window.partitionBy("year").orderBy("department",col("salary").desc())
df = data_df.withColumn("rank",rank().over(window)).show()

# 12. In each department, assign a rank to employees based on their salaries within each year. If two employees have the same salary, they should get the same rank without any gaps.
# ===============================================================================================================================
window = Window.partitionBy("department","year").orderBy(col("salary").desc())
df = data_df.withColumn("denserank",dense_rank().over(window)).show()

# 13. For each department, assign row numbers to employees based on year, ordering by salary in descending order.
# ===============================================================================================================================
window = Window.partitionBy("department","year").orderBy(col("salary").desc())
df = data_df.withColumn("row",row_number().over(window)).show()

# 14. Find the lowest-ranked employees in each department for the year 2022 based on salary.
# ===============================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").asc())
df = data_df.filter(col("year") == 2022).withColumn("rank",rank().over(window)).show()


# 15. In each department, rank employees based on salary and year. If employees have the same salary, they should share the same rank.
# ===============================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary"),"year")
df = data_df.withColumn("rank",rank().over(window)).show()

# 16. Assign row numbers to employees across all departments, ordered by salary, with ties in salary broken by alphabetical order of employee names.
# ===============================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").desc(),col("employees"))
df = data_df.withColumn("row",row_number().over(window)).show()

# 17. For each department, assign ranks to employees by year. Use a ranking function that assigns the same rank for ties and has no gaps.
# ===============================================================================================================================
window = Window.partitionBy("department","year").orderBy(col("salary").desc())
df = data_df.withColumn("denserank",dense_rank().over(window))

# 18. List employees ranked in descending order of their salaries within each department. If employees have the same salary, they should receive consecutive ranks without gaps.
# ================================================================================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").desc())
df = data_df.withColumn("denserank",dense_rank().over(window)).show()

# 19. Assign a dense rank to employees in the "Sales" department based on salary across all years.
# ===============================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").desc())
df = data_df.filter(col("department") == "Sales").withColumn("denserank",dense_rank().over(window)).show()


# 20. For each department and year, assign a row number to employees ordered by salary,showing only the top 2 in each department and year.
# ===============================================================================================================================
window = Window.partitionBy("department","year").orderBy(col("salary").desc())
df = data_df.withColumn("row",row_number().over(window)).filter(col("row") <=2).show()


# 21. In each department, rank employees based on their salary. Show ranks as consecutive numbers even if salaries are tied.
# ===============================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").desc())
df = data_df.withColumn("rank",rank().over(window)).show()

# 22. List employees with the top 2 highest salaries in the "Finance" department for each year.
# ===============================================================================================================================
window = Window.partitionBy("year").orderBy(col("salary").desc())
df = data_df.filter(col("department") == "Finance").withColumn("row",row_number().over(window)).filter(col("row") <=2).show()

# 23. For each department, rank employees based on their salary within each year. Display only employees ranked in the top 3 for each department and year.
# ===============================================================================================================================
window = Window.partitionBy("department","year").orderBy(col("salary").desc())
df = data_df.withColumn("rank",rank().over(window)).filter(col("denserank") <= 3).show()

# 24. Within the "Marketing" department, assign a row number to employees based on salary in descending order across all years.
# ===============================================================================================================================
window = Window.partitionBy("department").orderBy(col("salary").desc())
df = data_df.filter(col("department") == "Marketing").withColumn("row",row_number().over(window)).show()

# 25. Rank all employees based on their salaries in descending order across departments. Handle ties in a way that avoids gaps between ranks.
# ===============================================================================================================================
window = Window.orderBy(col("salary").desc())
df = data_df.withColumn("denserank",dense_rank().over(window)).show()