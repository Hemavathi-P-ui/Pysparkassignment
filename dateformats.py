import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when,year,month,dayofweek,row_number,to_timestamp,coalesce,dayofmonth,date_format,date_add,date_sub
from pyspark.sql.functions import weekofyear,quarter,current_date,add_months,round,date_trunc,expr
from pyspark.sql.functions import col, initcap,when, to_date, datediff, lit,sum,avg,count, min,max,lead,lag,rank,desc,last_day,months_between

os.environ["PYSPARK_PYTHON"] = "C:/Users/002PV2744/Documents/Python/Python37/python.exe"

confg = SparkConf()
confg.set("spark.app.name","new-pgm")
confg.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf= confg).getOrCreate()

# 1. Extract the day of the month from a date column.
# #===========================================================

data = [("2024-01-15",), ("2024-02-20",), ("2024-03-25",)]
data_df = spark.createDataFrame(data,["date"])

df = data_df.withColumn("dayofmonth", dayofmonth("date")).show()


# 2. Get the weekday name (e.g., Monday, Tuesday) from a date column.
#=====================================================================

data = [("2024-04-02",), ("2024-04-03",), ("2024-04-04",)]
data_df = spark.createDataFrame(data,["date"])

df = data_df.withColumn("date1",to_date(col("date"),"yyyy-MM-dd")).withColumn("month",date_format(col("date1"),"MMMM")).show()


# 3. Calculate the number of days between two dates.
#===========================================================
data = [("2024-01-01", "2024-01-10"), ("2024-02-01", "2024-02-20")]
data_df = spark.createDataFrame(data,["start_date", "end_date"])

df = data_df.withColumn("diff",datediff(col("end_date"),col("start_date")))
df.show()


# 4. Add 10 days to a given date column.
#===========================================================
data = [("2024-05-01",), ("2024-05-15",)]
data_df = spark.createDataFrame(data,["date"])

df = data_df.withColumn("adddate", date_add(col("date"),10)).show()


# 5. Subtract 7 days from a given date column.
# #===========================================================
data = [("2024-06-10",), ("2024-06-20",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("subdate", date_sub(col("date"),7)).show()


# 6.Filter rows where the year in a date column is 2023.
# #===========================================================
data = [("2023-08-12",), ("2024-08-15",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("year", year(col("date"))).filter((col("year")) == 2023).show()


# 6. Get the last day of the month for a given date column.
# #===========================================================
data = [("2024-07-10",), ("2024-07-25",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("lastday", last_day(col("date"))).show()


# 7. Extract only the year from a date column.
# #===========================================================
data = [("2024-01-01",), ("2025-02-01",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("year", year(col("date"))).show()


# 8. Calculate the number of months between two dates.
#===========================================================
data = [("2024-01-01", "2024-04-01"), ("2024-05-01", "2024-08-01")]
data_df = spark.createDataFrame(data,["start_date", "end_date"])
df = data_df.withColumn("monthbetween",months_between("start_date", "end_date")).show()

# 9. Get the week number from a date column.
# #===========================================================
data = [("2024-08-15",), ("2024-08-21",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("week", weekofyear(col("date"))).show()


# 10. Format a date column to "dd-MM-yyyy" format.
# #===========================================================
data = [("2024-09-01",), ("2024-09-10",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("date", date_format(col("date"),"dd-MM-yyyy")).show()

# 11.Find if a given date falls on a weekend.
# #===========================================================
data = [("2024-10-12",), ("2024-10-13",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("day", dayofweek(col("date"))).filter((col("day") == 1) | (col("day") == 7)).show()


# 11. Check if the date is in a leap year.
#===========================================================
data = [("2024-02-29",), ("2023-02-28",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("year", year(col("date")))\
    .withColumn("leapyear",(col("year") % 4 == 0) & ((col("year") % 100 != 0) | (col("year") % 400 == 0))).show()


# 12. Extract the quarter (1-4) from a date column.
# #===========================================================
data = [("2024-03-15",), ("2024-06-20",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("date1",to_date("date","yyyy-MM-dd"))
df1 = df.withColumn("month",quarter("date1")).show()

# 13. Display the month as an abbreviation (e.g., "Mar" for March).
# #=================================================================
data = [("2024-03-10",), ("2024-04-15",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("date1",to_date("date")).withColumn("month",date_format(col("date1"),"MMM")).show()

# 14. Add 5 months to a date column.
# #===========================================================
data = [("2024-01-01",), ("2024-02-15",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("newdate",to_date("date","yyyy-MM-dd"))
df1 = df.withColumn("month",add_months("newdate",5)).show()


# 15. Subtract 1 year from a date column.
# #===========================================================
data = [("2024-08-10",), ("2025-10-20",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("newdate",to_date("date","yyyy-MM-dd"))
df1 = df.withColumn("nextyear",add_months("newdate",-12)).show()


# # 16. Round a timestamp column to the nearest hour.
# # #===========================================================
data = [("2024-03-10 12:25:30",), ("2024-03-10 12:55:45",)]
data_df = spark.createDataFrame(data,["timestamp"])
df = data_df.withColumn("time",to_timestamp("timestamp"))
df1 = df.withColumn("timer", expr("time + interval 30 minute"))\
     .withColumn("nearest-hr", date_trunc("hour","timer")).show()


# 17. Calculate the date 100 days after a given date.
# #===========================================================
data = [("2024-04-01",), ("2024-05-10",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("next100",date_add("date",100)).show()

# 18. Calculate the difference in weeks between two dates.
# #===========================================================
data = [("2024-01-01", "2024-02-01"), ("2024-03-01", "2024-04-01")]
data_df = spark.createDataFrame(data,["start_date", "end_date"])
df = data_df.withColumn("diff",(datediff("end_date","start_date")/7).cast("int")).show()
# without cast it will give ans in decimal


# 19. Check if a date falls in the current year.
# #===========================================================
data = [("2024-01-15",), ("2023-12-25",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("year",year(current_date())).withColumn("present-year",col("year") == year(to_date("date")))
df.show()


# 20. Convert a date to timestamp and format it as "yyyy-MM-dd HH:mm".
# #====================================================================
data = [("2024-09-30",), ("2024-10-01",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("format",date_format("date","yyyy-MM-dd HH:mm")).show()


# 21. Find the date of the next Sunday for a given date.
# #===========================================================
data = [("2024-10-10",), ("2024-10-15",)]
data_df = spark.createDataFrame(data,["date"])
df= data_df.withColumn("newdate",to_date("date"))
df1= df.withColumn("nextsunday",date_add("newdate",((7-dayofweek("newdate"))%7)+1))
df1.show()

# 22. Display the date as "MMM dd, yyyy" format.
# #===========================================================
data = [("2024-11-15",), ("2024-12-20",)]
data_df = spark.createDataFrame(data,["date"])
df = data_df.withColumn("format",date_format("date","MMM dd, yyyy")).show()


# 23. Calculate the date 30 business days after a given date.
# #===========================================================
data = [("2024-01-01",), ("2024-02-10",)]
data_df = spark.createDataFrame(data,["date"])
df= data_df.withColumn("next30",date_add("date",30)).show()






