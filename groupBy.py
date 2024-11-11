import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when,year,month,dayofweek,row_number,stddev,countDistinct
from pyspark.sql.functions import col, initcap,when, to_date, datediff, lit,sum,avg,count, min,max,lead,lag,rank,desc
from itertools import combinations
os.environ["PYSPARK_PYTHON"] = "C:/Users/002PV2744/Documents/Python/Python37/python.exe"

confg = SparkConf()
confg.set("spark.app.name","new-pgm")
confg.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf= confg).getOrCreate()

 #1. Count Items per Category
 #===========================================================
 data = [("Electronics" , "Laptop"),
 ("Electronics" , "Phone"),
 ("Clothing" , "T-Shirt"),
 ("Clothing" , "Jeans"),
 ("Furniture", "Chair")]
 data_df = spark.createDataFrame(data, ["category","product"])

 window = Window.partitionBy(col("category"))
 df1 = data_df.withColumn("count", count(col("product")).over(window)).show()

 #2. Find Minimum, Maximum, and Average Price per Product
 #==========================================================
 data = [( "Laptop",  1000),
 ( "Phone",  500  ),
 ( "T-Shirt", 20  ),
 ( "Jeans", 50    ),
 ( "Chair", 150   )]
 data_df = spark.createDataFrame(data,["product","price"])

 window = Window.groupBy("product")
 df = (data_df.withColumn("min_price", min(col("price")).over(window)).withColumn("max_price", max(col("price")).over(window))
       .withColumn("avg_price", avg(col("price")).over(window)).show())

 #3. Group Sales by Month and Year
 #=========================================================
 data =[("2023-01-01", "New York", 100 ),
 ("2023-02-15", "London", 200 ),
 ("2023-03-10", "Paris", 300 ),
 ("2023-04-20", "Berlin", 400 ),
 ("2023-05-05", "Tokyo", 500 )]
 data_df = spark.createDataFrame(data, ["order_date", "city", "amount" ])

 df = data_df.withColumn("date", to_date("order_date","yyyy-MM-dd"))
 df1 = df.withColumn("year",year("date")).withColumn("month",month("date"))
 df3 = df1.groupBy("year","month").agg(sum("amount")).alias("total").show()

 #Find Top N Products by Sales
 #=========================================================
 data =[("Laptop", "order_1",2),
 ("Phone","order_2",1),
 ("T-Shirt","order_1",3),
 ("Jeans","order_3",4),
 ("Chair","order_2",2)]
 data_df = spark.createDataFrame(data,["product","order_id","quantity"])
 df = data_df.groupBy("product").agg(sum("quantity").alias("total_quantity"))
 df1 = df.orderBy(desc("total_quantity")).limit(5).show()


 #Calculate Average Rating per User
 #=========================================================
 data = [(1,1,4),
 (1,2,5),
 (2,1,3),
 (2,3,4),
 (3,2,5)]
 data_df = spark.createDataFrame(data,["user_id","product_id","rating"])

 df = data_df.groupBy("user_id").agg(avg("rating")).show()

 #Group Customers by Country and Calculate Total Spend
 #==========================================================
 data = [(1,"USA","order_1",100),
 (1,"USA","order_2",200),
 (2,"UK","order_3",150),
 (3,"France","order_4",250),
 (3,"France","order_5",300)]
 data_df = spark.createDataFrame(data,["customer_id","country","order_id","amount"])

 df= data_df.groupBy("country","customer_id").agg(sum("amount").alias("total")).show()

 #Find Products with No Sales in a Specific Time Period
 #=================================================================
 data= [("Laptop", "2023-01-01"),
 ("Phone","2023-02-15"),
 ("T-Shirt","2023-03-10"),
 ("Jeans","2023-04-20")]
 data_df = spark.createDataFrame(data,["product","order_date"])

 df = data_df.withColumn("date",to_date("order_date", "yyyy-MM-dd"))
 df1 =df.filter(col("date").between("2023-02-01","2023-03-31"))
 df2 = df1.select("product").distinct()
 df3 = df.select("product").distinct()
 df4 = df3.subtract(df2).show()


 #Group Orders by Weekday and Calculate Average Order Value (when-otherwise)
 #=============================================================================
 data =[("2023-04-10",1,100),
 ("2023-04-11",2,200),
 ("2023-04-12",3,300),
 ("2023-04-13",1,400),
 ("2023-04-14",2,500)]
 data_df = spark.createDataFrame(data,["order_date","customer_id","amount"])

 df = data_df.withColumn("date",to_date("order_date", "yyyy-MM-dd"))
 df1 = df.withColumn("day",when((dayofweek(col("date")) >=2) & (dayofweek(col("date")) <=6), "Weekday").otherwise("Weekend"))
 df2 = df1.groupBy("day").agg(avg(col("amount"))).show()


 #10. Filter Products Starting with "T" and Group by Category with Average Price
 #==================================================================================

 data =[("T-Shirt","Clothing",20),
 ("Table","Furniture",150),
 ("Jeans","Clothing",50),
 ("Chair","Furniture",100)]
 data_df = spark.createDataFrame(data,["product","category","price"])

 df = data_df.filter(col("product").startswith("T")).groupBy("category").agg(avg("price"))
 df.show()

 #11. Find Customers Who Spent More Than $200 in Total
 #===============================================================
 data = [(1,"order_1",100),
 (1,"order_2",150),
 (2,"order_3",250),
 (3,"order_4",100),
 (3,"order_5",120)]
 data_df= spark.createDataFrame(data,["customer_id","order_id","amount"])
 df = data_df.groupBy("customer_id")
 df1 = df.agg(sum(col("amount")).alias("sum"))
 df2 = df1.filter(col("sum") > 200).show()

 #12. Create a New Column with Order Status ("High" for > $100, "Low" Otherwise)
 #====================================================================================
 data = [("order_1",150),
 ("order_2",80 ),
 ("order_3",220 ),
 ("order_4",50 )]
 data_df = spark.createDataFrame(data,["order_id","amount"])
 df1 =data_df.withColumn("order_status", when(col("amount")> 100, "High").otherwise("Low"))
 df1.show()

 #13. Select Specific Columns and Apply GroupBy with Average
 #===================================================================
 data = [("Laptop", "Electronics",1000, 2),
 ("Phone","Electronics",500,1),
 ("T-Shirt","Clothing",20,3),
 ("Jeans","Clothing",50,4)]
 data_df =spark.createDataFrame(data,["product","category","price","quantity"])
 df = data_df.select(col("product"),col("price")).groupBy("product").agg(avg(col("price")).alias("avg")).show()

 #14. Count Orders by Year and Month with Aggregation Functions (count, sum)
 #===============================================================================
 data = [("2023-01-01",1,100),
 ("2023-02-15",2,200),
 ("2023-03-10",3,300),
 ("2023-04-20",1,400),
 ("2023-05-05",2,500)]
 data_df = spark.createDataFrame(data,["order_date","customer_id","amount"])

 df = data_df.withColumn("date",to_date("order_date","yyyy-MM-dd")).withColumn("year",year("date")).withColumn("month",month("date"))
 df1 = df.groupBy("year","month").agg(count("order_date"),sum("amount")).show()


 #15. Find Products with Highest and Lowest Sales in Each Category (Top N)
 #====================================================================================

 data =[("Laptop","Electronics",2),
 ("Phone","Electronics",1),
 ("T-Shirt","Clothing",3),
 ("Jeans","Clothing",4),
 ("Chair","Furniture",2),
 ("Sofa","Furniture",1)]
 data_df = spark.createDataFrame(data,["product","category","quantity"])

 df = data_df.groupBy("category","product").agg(sum("quantity").alias("sum"))
 window = Window.partitionBy("category").orderBy(col("sum").desc())
 df1=df.withColumn("rank", row_number().over(window)).filter(col("rank") <=2).show()

 #16. Calculate Average Rating per Product, Weighted by Quantity Sold
 #===================================================================================
 data = [(1,"order_1",4,2),
 (1,"order_2",5,1),
 (2,"order_3",3,4),
 (2,"order_4",4,3),
 (3,"order_5",5,1)]
 data_df = spark.createDataFrame(data,["product_id","order_id","rating","quantity"])
 df = data_df.withColumn("weighted_rating", col("rating") * col("quantity")).groupBy("product_id").agg(sum(col("weighted_rating"))/sum("quantity")).orderBy(col("product_id")).show()


#17. Find Customers Who Placed Orders in More Than Two Different Months
#============================================================================
 data = [(1,"2023-01-01"),
 (1,"2023-02-15"),
 (2,"2023-03-10"),
 (2,"2023-03-20"),
 (3,"2023-04-20"),
 (3,"2023-05-05")]
 data_df = spark.createDataFrame(data,["customer_id","order_date"])
 df = data_df.withColumn("date",to_date("order_date","yyyy-MM-dd")).withColumn("month",month(col("date")))
 df1 = df.groupBy("customer_id").agg(countDistinct("month").alias("count")).filter(col("count") > 2)
 df2 = df1.show()


#18. Group by Country and Calculate Total Sales, Excluding Orders Below $50
#============================================================================
 data = [("USA","order_1",100),
 ("USA","order_2",40),
 ("UK","order_3",150),
 ("France","order_4",250),
 ("France","order_5",30)]
 data_df = spark.createDataFrame(data,["country","order_id","amount"])
 df = data_df.filter(col("amount") > 50).groupBy("country").agg(sum("amount").alias("sum")).show()

# 19. Find Products Never Ordered Together (Pairwise Co-occurrence) (doubt)
#============================================================================
 data = [("order_1",1,2),
 ("order_2",1,3),
 ("order_3",2,4),
 ("order_4",3,1)]
 product_ids = [1, 2, 3, 4]
 itertools.combinations(iterable, r) used when non repeatative pairs required
 data1= list(combinations(product_ids, 2))
 data_df = spark.createDataFrame(data1,["product_id1","product_id2"])
 df1 = data_df.select(col("product_id1"),col("product_id2")).union(data_df.select(col("product_id2"),col("product_id1"))).distinct()
 df = df1.show()
 df2 = data_df.join(df1,on=["product_id1", "product_id2"],how="left_anti")
 df3 = df2.show()

# 20. Group by Category and Calculate Standard Deviation of Price
#============================================================================
 data= [("Laptop","Electronics",1000),
 ("Phone","Electronics",500),
 ("T-Shirt","Clothing",20),
 ("Jeans","Clothing",50),
 ("Chair","Furniture",150),
 ("Sofa","Furniture",200)]
 data_df = spark.createDataFrame(data,["product","category","price"])

 df = data_df.groupBy("category").agg(stddev("price").alias("std")).show()

 #21. Find Most Frequent Customer City Combinations
 #=========================================================================
 data = [(1,"New York"),
 (1,"New York"),
 (2,"London"),
 (2,"Paris"),
 (3,"Paris"),
 (3,"Paris")]
 data_df = spark.createDataFrame(data,["customer_id","city"])
 df = data_df.groupBy("customer_id","city").agg(count("city").alias("count"))
 window=Window.partitionBy("customer_id").orderBy(col("count").desc())
 df1 = df.withColumn("rank",rank().over(window))
 df2 = df1.filter(col("rank") == 1).drop("rank").show()


 #22. Calculate Customer Lifetime Value (CLTV) by Year
 #======================================================================
 data = [(1,"2022-01-01",100),
 (1,"2023-02-15",200),
 (2,"2022-03-10",300),
 (2,"2023-04-20",400),
 (3,"2022-05-05",500),
 (3,"2023-06-06",600)]
 data_df = spark.createDataFrame(data,["customer_id","order_date","amount"])

 df = data_df.withColumn("date",to_date(col("order_date"),"yyyy-MM-dd")).withColumn("year",year("date"))
 df1 = df.groupBy("customer_id","year").agg(sum("amount")).show()


 #23. Find Products with a Decline in Average Rating Compared to Previous Month
 #===============================================================================
 data =[(1,"2023-01-01",4),
 (1,"2023-02-15",3),
 (2,"2023-01-10",5),
 (2,"2023-02-20",4),
 (3,"2023-01-20",4),
 (3,"2023-02-25",5)]
 data_df = spark.createDataFrame(data,["product_id","order_date","rating"])

 df = data_df.withColumn("date",to_date(col("order_date"),"yyyy-MM-dd")).withColumn("month",month("date"))
 df1 = df.groupBy("product_id","month").agg(avg("rating").alias("avg_rating"))
 window = Window.partitionBy("product_id").orderBy("month")
 df2 = df1.withColumn("prev_avg", lag("avg_rating",1).over(window))
 df3 = df2.filter(col("prev_avg") >col("avg_rating"))
 df3.show()

 #24. Group Orders by Weekday and Find Peak Hour for Orders
 #=====================================================================
 data = [("order_1","2023-04-10",10),
 ("order_2","2023-04-11",15),
 ("order_3","2023-04-12",12),
 ("order_4","2023-04-13",11),
 ("order_5","2023-04-14",18)]
 data_df = spark.createDataFrame(data,["order_id","order_date","hour"])

 df = data_df.withColumn("date",to_date(col("order_date"),"yyyy-MM-dd")).withColumn("day",dayofweek("date"))
 df1 = df.groupBy("day","hour").agg(count("order_id").alias("order_count"))
 window = Window.partitionBy("day").orderBy(col("order_count").desc())
 df2 = df1.withColumn("rank", rank().over(window))
 df2.show()
 df3 = df2.filter(col("rank")== 1).drop("rank")
 df3.show()

 #25. Calculate Average Order Value by Country, Excludhouring Cancelled Orders
 #=================================================================================

data = [("USA","order_1", 100,"Shipped"),
("USA","order_2", 40 ,"Cancelled"),
("UK","order_3",150,"Completed"),
("France","order_4",250,"Pending"),
("France","order_5",30,"Shipped")]
data_df = spark.createDataFrame(data,["country","order_id","amount","status"])

df = data_df.filter(col("status") != "Cancelled")
df1 = df.groupBy("country").agg(avg(col("amount")).alias("avg"))
df1.show()
