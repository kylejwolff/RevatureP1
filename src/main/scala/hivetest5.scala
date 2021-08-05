import org.apache.spark.sql.SparkSession

object hivetest5 {

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      //.config("spark.sql.warehouse.dir", "hdfs://localhost/user/hive/warehouse")
      //.config("spark.sql.hive.metastore.jars","/home/kylej/apache-hive-3.1.2-bin/lib")
      //.config("spark.sql.hive.metastore.version", "3.1.2")
      //.appName("SparkTest1")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    // Create tables
    spark.sql("DROP TABLE IF EXISTS branch")
    spark.sql("DROP TABLE IF EXISTS bevcount")
    spark.sql("CREATE TABLE IF NOT EXISTS branch(beverage STRING, branch STRING) row format delimited fields terminated by ','")
    spark.sql("CREATE TABLE IF NOT EXISTS bevcount(beverage STRING, count INT) row format delimited fields terminated by ','")

    // Load Data Into Tables
    spark.sql("LOAD DATA LOCAL INPATH 'C:/tmp/hive/KyleJ/Bev_BranchA.txt' INTO TABLE branch")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/tmp/hive/KyleJ/Bev_BranchB.txt' INTO TABLE branch")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/tmp/hive/KyleJ/Bev_BranchC.txt' INTO TABLE branch")

    spark.sql("LOAD DATA LOCAL INPATH 'C:/tmp/hive/KyleJ/Bev_ConscountA.txt' INTO TABLE bevcount")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/tmp/hive/KyleJ/Bev_ConscountB.txt' INTO TABLE bevcount")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/tmp/hive/KyleJ/Bev_ConscountC.txt' INTO TABLE bevcount")

    println("Problem Scenario 1")

    println("What is the total number of consumers for Branch1?")
    spark.sql("SELECT SUM(t1.count) " +
      "FROM( " +
      "SELECT DISTINCT(b.beverage), b.branch, c.count " +
      "FROM branch AS b LEFT JOIN bevcount AS c ON (b.beverage == c.beverage) " +
      "WHERE b.branch = 'Branch1') AS t1").show()

    println("What is the number of consumers for the Branch2?")
    spark.sql("SELECT SUM(t1.count) " +
      "FROM( " +
      "SELECT DISTINCT(b.beverage), b.branch, c.count " +
      "FROM branch AS b LEFT JOIN bevcount AS c ON (b.beverage == c.beverage) " +
      "WHERE b.branch = 'Branch2') AS t1").show()

    println("Problem Scenario 2")

    println("What is the most consumed beverage on Branch1?")
    spark.sql("SELECT b.beverage, Sum(c.count) AS count " +
      "FROM branch b JOIN bevcount c ON (b.beverage = c.beverage) " +
      "WHERE b.branch = 'Branch1' " +
      "GROUP BY b.beverage " +
      "ORDER BY count DESC " +
      "LIMIT 1").show()

    println("What is the least consumed beverage on Branch2?")
    spark.sql("SELECT b.beverage, Sum(c.count) AS count " +
      "FROM branch b JOIN bevcount c ON (b.beverage = c.beverage) " +
      "WHERE b.branch = 'Branch2' " +
      "GROUP BY b.beverage " +
      "ORDER BY count ASC " +
      "LIMIT 1").show()

    println("What is the Average consumed beverage of Branch2?")
    spark.sql("DROP TABLE IF EXISTS branch2count")
    spark.sql("CREATE TABLE IF NOT EXISTS branch2count AS SELECT SUM(c.count) AS count " +
      "FROM branch b JOIN bevcount c ON (b.beverage = c.beverage) " +
      "WHERE b.branch = 'Branch2' " +
      "GROUP BY b.beverage")
    spark.sql("SELECT AVG(count) FROM branch2count").show()

    println("Problem Scenario 3")

    println("What are the beverages available on Branch10, Branch8, and Branch1?")
    spark.sql("SELECT DISTINCT(beverage) " +
      "FROM branch " +
      "WHERE branch = 'Branch10' OR branch = 'Branch8' OR branch = 'Branch1'").show()

    println("What are the common beverages available in Branch4,Branch7?")
    spark.sql("SELECT beverage " +
      "FROM branch " +
      "WHERE branch = 'Branch4' " +
      "UNION " +
      "SELECT beverage " +
      "FROM branch " +
      "WHERE branch = 'Branch7'").show()

    println("Problem Scenario 4")

    println("Create a partition, View for Scenario 3.")
    spark.sql("DROP VIEW IF EXISTS bevb4b7")
    spark.sql("CREATE VIEW bevb4b7 AS " +
      "SELECT beverage " +
      "FROM branch " +
      "WHERE branch = 'Branch4' " +
      "UNION " +
      "SELECT beverage " +
      "FROM branch " +
      "WHERE branch = 'Branch7'")
    spark.sql("DROP VIEW IF EXISTS bevb10b8b1")
    spark.sql("CREATE VIEW bevb10b8b1 AS " +
      "SELECT DISTINCT(beverage) " +
      "FROM branch " +
      "WHERE branch = 'Branch10' OR branch = 'Branch8' OR branch = 'Branch1'")
    spark.sql("DROP TABLE IF EXISTS branch_part")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict USING hive")
    spark.sql("CREATE TABLE branch_part(beverage STRING) PARTITIONED BY (branch STRING)")
    spark.sql("INSERT OVERWRITE TABLE branch_part PARTITION(branch) SELECT beverage, branch FROM branch")

    println("Problem Scenario 5")

    println("Alter the table properties to add 'note','comment'")
    spark.sql("ALTER TABLE branch SET TBLPROPERTIES('note'='that\\'s why you always leave a note')")
    spark.sql("ALTER TABLE branch SET TBLPROPERTIES('comment'='from the peanut gallery')")

    println("Problem Scenario 6")

    println("Remove a row from any Senario")
    spark.sql("SELECT DISTINCT(beverage) " +
      "FROM branch " +
      "WHERE (branch = 'Branch10' OR branch = 'Branch8' OR branch = 'Branch1') " +
      "AND beverage <> 'Cold_Coffee'").show()

    println("Select all beverage totals from Branch 5")
    spark.sql("SELECT b.beverage, Sum(c.count) AS count " +
      "FROM branch b " +
      "JOIN bevcount c ON (b.beverage = c.beverage) " +
      "WHERE b.branch = 'Branch5' " +
      "GROUP BY b.beverage " +
      "ORDER BY b.beverage ASC").show()

    println("Select the total Large Mocha sold for each branch")
    spark.sql("SELECT b.branch, SUM(c.count) " +
      "FROM branch b " +
      "JOIN bevcount c ON (b.beverage = c.beverage) " +
      "WHERE b.beverage = 'LARGE_MOCHA' " +
      "GROUP BY b.branch " +
      "ORDER BY b.branch ASC").show()
  }
}
