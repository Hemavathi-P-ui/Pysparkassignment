import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, datediff, initcap, lit, max, min, sum, to_date, when}

object Assignment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Practice3").master("local").getOrCreate()
    import spark.implicits._


    // Question 1: Employee Status Check
    val emp = Seq(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")
    )
    val currentDate = "2024-11-03"
    val empDF = spark.createDataFrame(emp).toDF("name", "last_checkin")
    empDF.select(
      initcap(col("name")).alias("name"),
      when(datediff(to_date(lit(currentDate)), to_date(col("last_checkin"))) <= 7, "Active").otherwise("Inactive").alias("Status")
    ).show()

    // Question 2: Sales Performance by Agent
    val sales = Seq(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)
    )
    val salesDF = spark.createDataFrame(sales).toDF("name", "total_sales")
    salesDF.select(
      initcap(col("name")).alias("name"),
      when(col("total_sales") > 50000, "Excellent")
        .when(col("total_sales").between(25000, 50000), "Good")
        .otherwise("Need Improvement").alias("performance_status")
    ).show()

    // Question 3: Project Allocation and Workload Analysis
    val workload = Seq(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
      ("neha", "ProjectC", 80),
      ("neha", "ProjectD", 30),
      ("priya", "ProjectE", 110),
      ("mohan", "ProjectF", 40),
      ("ajay", "ProjectG", 70),
      ("vijay", "ProjectH", 150),
      ("veer", "ProjectI", 190),
      ("aatish", "ProjectJ", 60),
      ("animesh", "ProjectK", 95),
      ("nishad", "ProjectL", 210),
      ("varun", "ProjectM", 50),
      ("aadil", "ProjectN", 90)
    )
    val workloadDF = spark.createDataFrame(workload).toDF("name", "project", "hours")
    val df1 = workloadDF.groupBy("name").agg(sum("hours").alias("total_hr"))
    val df2 = df1.select(
      initcap(col("name")).alias("name"),
      col("total_hr"),
      when(col("total_hr") > 200, "Overloaded")
        .when(col("total_hr").between(100, 200), "Balanced")
        .otherwise("Underutilized").alias("work_load")
    )
    val df3 = df2.groupBy("work_load").count()
    df3.show()

    // Overtime Calculation for Employees
    val employees = Seq(
      ("karthik", 62),
      ("neha", 50),
      ("priya", 30),
      ("mohan", 65),
      ("ajay", 40),
      ("vijay", 47),
      ("veer", 55),
      ("aatish", 30),
      ("animesh", 75),
      ("nishad", 60)
    )
    val employeesDF = spark.createDataFrame(employees).toDF("name", "hours_worked")
    val overtimeDF = employeesDF.select(
      initcap(col("name")).alias("name"),
      when(col("hours_worked") > 60, "Excessive Overtime")
        .when(col("hours_worked").between(45, 60), "Standard Overtime")
        .otherwise("No Overtime").alias("status")
    )
    overtimeDF.show()
    val df2Overtime = overtimeDF.groupBy("status").count()
    df2Overtime.show()

    // Customer Age Grouping
    val customers = Seq(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    )
    val customersDF = spark.createDataFrame(customers).toDF("name", "age")
    val ageGroupDF = customersDF.select(
      initcap(col("name")).alias("name"),
      when(col("age") < 25, "Youth")
        .when(col("age").between(25, 45), "Adult")
        .otherwise("Senior").alias("group")
    )
    ageGroupDF.show()
    val ageGroupCountDF = ageGroupDF.groupBy("group").count().show()

    // Vehicle Mileage Analysis
    val vehicles = Seq(
      ("CarA", 30),
      ("CarB", 22),
      ("CarC", 18),
      ("CarD", 15),
      ("CarE", 10),
      ("CarF", 28),
      ("CarG", 12),
      ("CarH", 35),
      ("CarI", 25),
      ("CarJ", 16)
    )
    val vehiclesDF = spark.createDataFrame(vehicles).toDF("vehicle_name", "mileage")
    val mileageAnalysisDF = vehiclesDF.select(
      col("vehicle_name"),
      when(col("mileage") > 25, "High Efficiency")
        .when(col("mileage").between(15, 25), "Moderate Efficiency")
        .otherwise("Low Efficiency").alias("efficiency")
    ).show()

    // Student Grade Classification
    val students = Seq(
      ("karthik", 95),
      ("neha", 82),
      ("priya", 74),
      ("mohan", 91),
      ("ajay", 67),
      ("vijay", 80),
      ("veer", 85),
      ("aatish", 72),
      ("animesh", 90),
      ("nishad", 60)
    )
    val studentsDF = spark.createDataFrame(students).toDF("name", "score")
    val studentGradesDF = studentsDF.select(
      when(col("score") >= 90, "Excellent")
        .when(col("score").between(75, 89), "Good")
        .otherwise("Needs Improvement").alias("status")
    )
    val gradeCountDF = studentGradesDF.groupBy("status").count().show()

    // Product Inventory Check
    val inventory = Seq(
      ("ProductA", 120),
      ("ProductB", 95),
      ("ProductC", 45),
      ("ProductD", 200),
      ("ProductE", 75),
      ("ProductF", 30),
      ("ProductG", 85),
      ("ProductH", 100),
      ("ProductI", 60),
      ("ProductJ", 20)
    )
    val inventoryDF = spark.createDataFrame(inventory).toDF("product_name", "stock_quantity")
    val inventoryStatusDF = inventoryDF.select(
      when(col("stock_quantity") > 100, "Overstocked")
        .when(col("stock_quantity").between(50, 100), "Normal")
        .otherwise("Low Stock").alias("total_stock")
    )
    val inventoryCountDF = inventoryStatusDF.groupBy("total_stock").count().show()

    // Employee Bonus Calculation Based on Performance and Department
    val employeeBonuses = Seq(
      ("karthik", "Sales", 85),
      ("neha", "Marketing", 78),
      ("priya", "IT", 90),
      ("mohan", "Finance", 65),
      ("ajay", "Sales", 55),
      ("vijay", "Marketing", 82),
      ("veer", "HR", 72),
      ("aatish", "Sales", 88),
      ("animesh", "Finance", 95),
      ("nishad", "IT", 60)
    )
    val employeeBonusesDF = spark.createDataFrame(employeeBonuses).toDF("name", "department", "performance_score")
    val bonusDF = employeeBonusesDF.select(
      col("name"), col("department"), col("performance_score"),
      when((col("department") === "Sales" || col("department") === "Marketing") && col("performance_score") > 80, 0.20)
        .when(col("performance_score") > 70, 0.15)
        .otherwise(0).alias("bonus_percentage"),
      (col("performance_score") *
        when((col("department") === "Sales" || col("department") === "Marketing") && col("performance_score") > 80, 0.20)
          .when(col("performance_score") > 70, 0.15)
          .otherwise(0)).alias("bonus")
    )
    bonusDF.show()
    val bonusByDepartmentDF = bonusDF.groupBy("department").agg(sum("bonus").alias("bonusamt")).show()

    // Product Return Analysis with Multi-Level Classification
    val productsData = Seq(
      ("Laptop", "Electronics", 120, 45),
      ("Smartphone", "Electronics", 80, 60),
      ("Tablet", "Electronics", 50, 72),
      ("Headphones", "Accessories", 110, 47),
      ("Shoes", "Clothing", 90, 55),
      ("Jacket", "Clothing", 30, 80),
      ("TV", "Electronics", 150, 40),
      ("Watch", "Accessories", 60, 65),
      ("Pants", "Clothing", 25, 75),
      ("Camera", "Electronics", 95, 58)
    )
    val productsDF = spark.createDataFrame(productsData).toDF("product_name", "category", "return_count", "satisfaction_score")
    val returnRateDF = productsDF.select(
      col("product_name"),
      when((col("return_count") > 100) && (col("satisfaction_score") < 50), "High Return Rate")
        .when((col("return_count").between(50, 100)) && (col("satisfaction_score").between(50, 70)), "Moderate Return Rate")
        .otherwise("Low Return Rate").alias("Rate")
    )
    returnRateDF.show()
    val returnRateCountDF = returnRateDF.groupBy("Rate").count().show()

    // Customer Spending Pattern Based on Age and Membership Level
    val customerData = Seq(
      ("karthik", "Premium", 1050, 32),
      ("neha", "Standard", 800, 28),
      ("priya", "Premium", 1200, 40),
      ("mohan", "Basic", 300, 35),
      ("ajay", "Standard", 700, 25),
      ("vijay", "Premium", 500, 45),
      ("veer", "Basic", 450, 33),
      ("aatish", "Standard", 600, 29),
      ("animesh", "Premium", 1500, 60),
      ("nishad", "Basic", 200, 21)
    )
    val customersSpendingDF = spark.createDataFrame(customerData).toDF("name", "membership", "spending", "age")
    val spendingCategoryDF = customersSpendingDF.select(
      col("name"), col("membership"),
      when((col("spending") > 1000) && (col("membership") === "Premium"), "High Spender")
        .when((col("spending").between(500, 1000)) && (col("membership") === "Standard"), "Average Spender")
        .otherwise("Low Spender").alias("spending category")
    )
    spendingCategoryDF.show()
    val avgSpendingDF = customersSpendingDF.groupBy("membership").agg(avg("spending").alias("avg_spending"))
    avgSpendingDF.show()
    val joinedDF = spendingCategoryDF.join(avgSpendingDF, "membership")
    joinedDF.select("name", "spending category", "avg_spending").show()

    // E-commerce Order Fulfillment Timeliness Based on Product Type and Location
    val ordersData = Seq(
      ("Order1", "Laptop", "Domestic", 2),
      ("Order2", "Shoes", "International", 8),
      ("Order3", "Smartphone", "Domestic", 3),
      ("Order4", "Tablet", "International", 5),
      ("Order5", "Watch", "Domestic", 7),
      ("Order6", "Headphones", "International", 10),
      ("Order7", "Camera", "Domestic", 1),
      ("Order8", "Shoes", "International", 9),
      ("Order9", "Laptop", "Domestic", 6),
      ("Order10", "Tablet", "International", 4)
    )
    val ordersDF = spark.createDataFrame(ordersData).toDF("order_id", "product_type", "origin", "delivery_days")
    val orderTimelinessDF = ordersDF.select(
      col("order_id"),
      col("product_type"),
      when((col("delivery_days") > 7) && (col("origin") === "International"), "Delayed")
        .when((col("delivery_days").between(3, 7)), "On-Time")
        .when((col("delivery_days") < 3), "Fast").alias("category")
    )
    orderTimelinessDF.show()
    val orderCountDF = orderTimelinessDF.groupBy("product_type", "category").agg(count("order_id")).alias("count_prod").orderBy("product_type", "category").show()

    // Financial Risk Level Classification for Loan Applicants
    val loanApplicants = Seq(
      ("karthik", 60000, 120000, 590),
      ("neha", 90000, 180000, 610),
      ("priya", 50000, 75000, 680),
      ("mohan", 120000, 240000, 560),
      ("ajay", 45000, 60000, 620),
      ("vijay", 100000, 100000, 700),
      ("veer", 30000, 90000, 580),
      ("aatish", 85000, 85000, 710),
      ("animesh", 50000, 100000, 650),
      ("nishad", 75000, 200000, 540)
    )
    val loanApplicantsDF = spark.createDataFrame(loanApplicants).toDF("name", "income", "loan_amount", "credit_score")
    val riskClassificationDF = loanApplicantsDF.select(
      col("name"),
      col("income"),
      col("credit_score"),
      when(col("income") < 50000, "< 50k")
        .when(col("income").between(50000, 100000), "50-100k")
        .otherwise("> 100k").alias("income_range"),
      when((col("loan_amount") > 2 * col("income")) && (col("credit_score") < 600), "High Risk")
        .when((col("loan_amount").between(col("income"), 2 * col("income"))) && (col("credit_score").between(600, 700)), "Moderate Risk")
        .otherwise("Low Risk").alias("Risk_level")
    )
    val avgCreditScoreDF = riskClassificationDF.groupBy("income_range", "Risk_level").agg(avg("credit_score").alias("avg_creditscore")).show()
    val filteredRiskDF = avgCreditScoreDF.filter(col("avg_creditscore") < 650).show()


    // Scenario 15: Customer Purchase Recency Categorization
    val customerPurchases = Seq(
      ("karthik", "Premium", 50, 5000),
      ("neha", "Standard", 10, 2000),
      ("priya", "Premium", 65, 8000),
      ("mohan", "Basic", 90, 1200),
      ("ajay", "Standard", 25, 3500),
      ("vijay", "Premium", 15, 7000),
      ("veer", "Basic", 75, 1500),
      ("aatish", "Standard", 45, 3000),
      ("animesh", "Premium", 20, 9000),
      ("nishad", "Basic", 80, 1100)
    )

    val customerPurchasesDF = spark.createDataFrame(customerPurchases).toDF("name", "membership", "days_since_last_purchase", "total_purchase_amount")

    val df1 = customerPurchasesDF.select(
      col("name"),
      col("membership"),
      col("total_purchase_amount"),
      when(col("days_since_last_purchase") < 30, "Frequent")
        .when(col("days_since_last_purchase").between(30, 60), "Occasional")
        .otherwise("Rare").alias("purchase")
    )

    df1.groupBy("membership", "purchase").count().show()

    val df2 = df1.filter(col("purchase") === "Frequent" && col("membership") === "Premium")
    df2.agg(avg("total_purchase_amount").alias("avg")).show()

    val df4 = df1.filter(col("purchase") === "Rare").groupBy("membership").agg(min("total_purchase_amount").alias("min_purchase"))
    df4.show()

    // Scenario 16: Electricity Consumption and Rate Assignment
    val electricityUsage = Seq(
      ("House1", 550, 250),
      ("House2", 400, 180),
      ("House3", 150, 50),
      ("House4", 500, 200),
      ("House5", 600, 220),
      ("House6", 350, 120),
      ("House7", 100, 30),
      ("House8", 480, 190),
      ("House9", 220, 105),
      ("House10", 150, 60)
    )

    val electricityUsageDF = spark.createDataFrame(electricityUsage).toDF("household", "kwh_usage", "total_bill")

    val df1 = electricityUsageDF.select(
      col("household"),
      col("kwh_usage"),
      col("total_bill"),
      when(col("kwh_usage") > 500 && col("total_bill") > 200, "High usage")
        .when(col("kwh_usage").between(200, 500) && col("total_bill").between(100, 200), "Medium usage")
        .otherwise("Low usage").alias("usage")
    )

    df1.groupBy("usage").count().show()

    val df2 = df1.filter(col("usage") === "High usage").agg(max("total_bill").alias("billmax"))
    df2.show()

    val df3 = df1.filter(col("usage") === "Medium usage").agg(avg("kwh_usage").alias("avg_bill"))
    df3.show()

    val df4 = df1.filter(col("usage") === "Low usage" && col("kwh_usage") > 300).count()

    // Scenario 17: Employee Salary Band and Performance Classification
    val employees = Seq(
      ("karthik", "IT", 110000, 12, 88),
      ("neha", "Finance", 75000, 8, 70),
      ("priya", "IT", 50000, 5, 65),
      ("mohan", "HR", 120000, 15, 92),
      ("ajay", "IT", 45000, 3, 50),
      ("vijay", "Finance", 80000, 7, 78),
      ("veer", "Marketing", 95000, 6, 85),
      ("aatish", "HR", 100000, 9, 82),
      ("animesh", "Finance", 105000, 11, 88),
      ("nishad", "IT", 30000, 2, 55)
    )

    val empDF = spark.createDataFrame(employees).toDF("name", "department", "salary", "experience", "performance_score")

    val df = empDF.select(
      col("name"),
      col("department"),
      col("salary"),
      col("experience"),
      col("performance_score"),
      when(col("salary") > 100000 && col("experience") > 10, "Senior")
        .when(col("salary").between(50000, 100000) && col("experience").between(5, 10), "Mid-Level")
        .otherwise("Junior").alias("salary_band")
    )

    df.groupBy("department", "salary_band").count().show()

    val df2 = df.groupBy("salary_band").agg(avg("performance_score").alias("avg_score"))
    df2.filter(col("avg_score") > 80).show()

    val df4 = df.filter(col("salary_band") === "Mid-Level" && col("performance_score") > 85 && col("experience") > 7)
    df4.show()

    // Scenario 18: Product Sales Analysis
    val productSales = Seq(
      ("Product1", 250000, 5),
      ("Product2", 150000, 8),
      ("Product3", 50000, 20),
      ("Product4", 120000, 10),
      ("Product5", 300000, 7),
      ("Product6", 60000, 18),
      ("Product7", 180000, 9),
      ("Product8", 45000, 25),
      ("Product9", 70000, 15),
      ("Product10", 10000, 30)
    )

    val productSalesDF = spark.createDataFrame(productSales).toDF("product_name", "total_sales", "discount")

    val df = productSalesDF.select(
      col("product_name"),
      col("total_sales"),
      col("discount"),
      when(col("total_sales") > 200000 && col("discount") < 10, "Top Seller")
        .when(col("total_sales").between(100000, 200000), "Moderate Seller")
        .otherwise("Low Seller").alias("Seller")
    )

    df.groupBy("Seller").count().show()

    val df2 = df.filter(col("Seller") === "Top Seller").agg(max("total_sales").alias("max_sales"))
    df2.show()

    val df3 = df.filter(col("Seller") === "Moderate Seller").agg(min("discount").alias("min_discount"))
    df3.show()

    val df4 = df.filter(col("Seller") === "Low Seller" && col("total_sales") > 50000 && col("discount") > 15)
    df4.show()

    // Scenario 19: Customer Loyalty Analysis
    val customerLoyalty = Seq(
      ("Customer1", 25, 700),
      ("Customer2", 15, 400),
      ("Customer3", 5, 50),
      ("Customer4", 18, 450),
      ("Customer5", 22, 600),
      ("Customer6", 2, 80),
      ("Customer7", 12, 300),
      ("Customer8", 6, 150),
      ("Customer9", 10, 200),
      ("Customer10", 1, 90)
    )

    val customerLoyaltyDF = spark.createDataFrame(customerLoyalty).toDF("customer_name", "purchase_frequency", "average_spending")

    val df = customerLoyaltyDF.select(
      col("customer_name"),
      col("purchase_frequency"),
      col("average_spending"),
      when(col("purchase_frequency") > 20 && col("average_spending") > 500, "Highly Loyal")
        .when(col("purchase_frequency").between(10, 20), "Moderately Loyal")
        .otherwise("Low Loyalty").alias("loyalty")
    )

    df.groupBy("loyalty").count().show()

    val df2 = df.agg(
      avg(when(col("loyalty") === "Highly Loyal", col("average_spending"))).alias("avg_spend"),
      min(when(col("loyalty") === "Moderately Loyal", col("average_spending"))).alias("min_spend")
    )
    df2.show()

    val df3 = df.filter(col("loyalty") === "Low Loyalty" && col("average_spending") < 100 && col("purchase_frequency") < 5)
    df3.show()

    // Scenario 20: E-commerce Return Rate Analysis
    val ecommerceReturn = Seq(
      ("Product1", 75, 25),
      ("Product2", 40, 15),
      ("Product3", 30, 5),
      ("Product4", 60, 18),
      ("Product5", 100, 30),
      ("Product6", 45, 10),
      ("Product7", 80, 22),
      ("Product8", 35, 8),
      ("Product9", 25, 3),
      ("Product10", 90, 12)
    )

    val ecommerceReturnDF = spark.createDataFrame(ecommerceReturn).toDF("product_name", "sale_price", "return_rate")

    val df = ecommerceReturnDF.select(
      col("product_name"),
      col("sale_price"),
      col("return_rate"),
      when(col("return_rate") > 20, "High Return")
        .when(col("return_rate").between(10, 20), "Medium Return")
        .otherwise("Low Return").alias("returns")
    )

    df.groupBy("returns").count().show()

    val df2 = df.agg(
      avg(when(col("returns") === "High Return", col("sale_price"))).alias("avg_sales"),
      max(when(col("returns") === "Medium Return", col("return_rate"))).alias("max_returns")
    )
    df2.show()

    val df3 = df.filter(col("returns") === "Low Return" && col("sale_price") < 50 && col("return_rate") < 5)
    df3.show()

    // Scenario 21: Employee Productivity Scoring
    val employeeProductivity = Seq(
      ("Emp1", 85, 6),
      ("Emp2", 75, 4),
      ("Emp3", 40, 1),
      ("Emp4", 78, 5),
      ("Emp5", 90, 7),
      ("Emp6", 55, 3),
      ("Emp7", 80, 5),
      ("Emp8", 42, 2),
      ("Emp9", 30, 1),
      ("Emp10", 68, 4)
    )

    val employeeProductivityDF = spark.createDataFrame(employeeProductivity).toDF("employee_id", "productivity_score", "project_count")

    val df = employeeProductivityDF.select(
      col("employee_id"),
      col("productivity_score"),
      col("project_count"),
      when(col("productivity_score") > 80 && col("project_count") > 5, "High Performer")
        .when(col("productivity_score").between(60, 80), "Average Performer")
        .otherwise("Low Performer").alias("Performance")
    )

    df.groupBy("Performance").count().show()

    val df2 = df.agg(
      avg(when(col("Performance") === "High Performer", col("productivity_score"))).alias("avg_score"),
      min(when(col("Performance") === "Average Performer", col("productivity_score"))).alias("min_score")
    )
    df2.show()

    val df3 = df.filter(col("Performance") === "Low Performer" && col("productivity_score") < 50 && col("project_count") < 2)
    df3.show()

    // Scenario 22: Banking Fraud Detection
    val transactions = Seq(
      ("Account1", "2024-11-01", 12000, 6, "Savings"),
      ("Account2", "2024-11-01", 8000, 3, "Current"),
      ("Account3", "2024-11-02", 2000, 1, "Savings"),
      ("Account4", "2024-11-02", 15000, 7, "Savings"),
      ("Account5", "2024-11-03", 9000, 4, "Current"),
      ("Account6", "2024-11-03", 3000, 1, "Current"),
      ("Account7", "2024-11-04", 13000, 5, "Savings"),
      ("Account8", "2024-11-04", 6000, 2, "Current"),
      ("Account9", "2024-11-05", 20000, 8, "Savings"),
      ("Account10", "2024-11-05", 7000, 3, "Savings")
    )

    val transactionsDF = spark.createDataFrame(transactions).toDF("account_id", "transaction_date", "amount", "frequency", "account_type")

    val df = transactionsDF.select(
      col("account_id"),
      col("transaction_date"),
      col("amount"),
      col("frequency"),
      col("account_type"),
      when(col("amount") > 10000 && col("frequency") > 5, "High Risk")
        .when(col("amount").between(5000, 10000) && col("frequency").between(2, 5), "Moderate Risk")
        .otherwise("Low Risk").alias("Risk")
    )

    df.groupBy("Risk").count().show()

    val df2 = df.filter(col("Risk") === "High Risk").groupBy("account_id").agg(sum("amount").alias("total"))
    df2.show()

    val df3 = df.filter(col("Risk") === "Moderate Risk" && col("account_type") === "Savings" && col("amount") < 7500)
    df3.show()

    // Scenario 23: Hospital Patient Readmission Analysis
    val patients = Seq(
      ("Patient1", 62, 10, 3, "ICU"),
      ("Patient2", 45, 25, 1, "General"),
      ("Patient3", 70, 8, 2, "ICU"),
      ("Patient4", 55, 18, 3, "ICU"),
      ("Patient5", 65, 30, 1, "General"),
      ("Patient6", 80, 12, 4, "ICU"),
      ("Patient7", 50, 40, 1, "General"),
      ("Patient8", 78, 15, 2, "ICU"),
      ("Patient9", 40, 35, 1, "General"),
      ("Patient10", 73, 14, 3, "ICU")
    )

    val patientsDF = spark.createDataFrame(patients).toDF("patient_id", "age", "readmission_interval", "icu_admissions", "admission_type")

    val df = patientsDF.select(
      col("patient_id"),
      col("age"),
      col("readmission_interval"),
      col("icu_admissions"),
      col("admission_type"),
      when(col("readmission_interval") < 15 && col("age") > 60, "High Readmission Risk")
        .when(col("readmission_interval").between(15, 30), "Moderate Risk")
        .otherwise("Low Risk").alias("category")
    )

    df.groupBy("category").count().show()

    val df2 = df.filter(col("category") === "High Readmission Risk").agg(avg(col("readmission_interval")).alias("avg_read"))
    df2.show()

    val df3 = df.filter(col("category") === "Moderate Risk" && col("admission_type") === "ICU" && col("icu_admissions") > 2)
    df3.show()

    // Scenario 24: Student Graduation Prediction
    val students = Seq(
      ("Student1", 70, 45, 60, 65, 75),
      ("Student2", 80, 55, 58, 62, 67),
      ("Student3", 65, 30, 45, 70, 55),
      ("Student4", 90, 85, 80, 78, 76),
      ("Student5", 72, 40, 50, 48, 52),
      ("Student6", 88, 60, 72, 70, 68),
      ("Student7", 74, 48, 62, 66, 70),
      ("Student8", 82, 56, 64, 60, 66),
      ("Student9", 78, 50, 48, 58, 55),
      ("Student10", 68, 35, 42, 52, 45)
    )

    val studentsDF = spark.createDataFrame(students).toDF("student_id", "attendance_percentage", "math_score", "science_score", "english_score", "history_score")

    val df = studentsDF.select(
      col("student_id"),
      col("attendance_percentage"),
      col("math_score"),
      col("science_score"),
      col("english_score"),
      col("history_score"),
      ((col("math_score") + col("science_score") + col("english_score") + col("history_score")) / 4).alias("avg_score"),
      when(col("attendance_percentage") < 75 && ((col("math_score") + col("science_score") + col("english_score") + col("history_score")) / 4 < 50), "At Risk")
        .when(col("attendance_percentage").between(75, 85), "Moderate Risk")
        .otherwise("Low Risk").alias("Risk")
    )

    df.groupBy("Risk").count().alias("total_students").show()

    val df2 = df.filter(col("Risk") === "At Risk").agg(avg("avg_score").alias("avg_read"))
    df2.show()

    val df3 = df.filter(col("Risk") === "Moderate Risk").select(
      col("student_id"),
      ((col("math_score") > 70).cast("int") +
        (col("science_score") > 70).cast("int") +
        (col("english_score") > 70).cast("int") +
        (col("history_score") > 70).cast("int")).alias("scores_count")
    )
    df3.show()

    val df4 = df3.filter(col("scores_count") >= 3).show()



  }
}