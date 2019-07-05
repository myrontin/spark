package spark_phoenix

import java.sql.DriverManager
import org.apache.spark._

object simple_job {

  val csv_file1_path = "/user/spark/csv1.csv"

  val csv_file2_path = "/user/spark/csv2.csv"

  val spark = SparkSession.builder().master("local").getOrCreate()

  val df_csv1 = spark.read.format("com.databricks.spark.csv").
    option("header", "true").
    option("inferSchema", "true").
    load(csv_file1_path)
    
  val df_csv2 = spark.read.format("com.databricks.spark.csv").
    option("header", "true").
    option("inferSchema", "true").
    load(csv_file2_path)

}