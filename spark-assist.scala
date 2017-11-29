import org.apache.spark.SparkConf
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io._

import java.io._

val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
import hiveContext.implicits._


/*
 * Takes two parameters, a dataframe and no. of rows to show. Default is 20.
 * Usage: rowsPerPartition(myDF) or rowsPerPartiton(myDf, Some(100))
 * Output: No. of partitions in the dataframe and a table view of the
 * partition no. and no. of rows in ascedning order of the partitions
*/
def rowsPerPartition(df: DataFrame, rows: Option[Int] = None) = {
  // Total partitions in the dataframe
  val totalPartitions = df.rdd.partitions.size
  println("Total Parttions: " + totalPartitions + "\n")
  val rowCount = rows getOrElse 20
  val showPartitions = df.groupBy(spark_partition_id)
                         .count
                         .orderBy("SPARK_PARTITION_ID()")
                         .show(rowCount,false)
}
/*
  Sample Output

  Total Parttions: 9011
  +--------------------+-----+
  |SPARK_PARTITION_ID()|count|
  +--------------------+-----+
  |1238                |19089|
  |1591                |17404|
  |1088                |17870|
  |1645                |20982|
  |833                 |18906|
  |1580                |17012|
  |1342                |16984|
  |858                 |17296|
  |1522                |17240|
  +--------------------+-----+
*/


/*
 * Finds the lowest, maximum and average of row count per partition in a
 * dataframe.
*/
def partitionStats(df: DataFrame) = {
  // Total partitions in the dataframe
  val totalPartitions = df.rdd.partitions.size
  println("Total Parttions: " + totalPartitions + "\n")
  val partitionStats = df.groupBy(spark_partition_id)
                         .count
                         .agg(max("count").alias("MAX"),
                              min("count").alias("MIN"),
                              avg("count").alias("AVERAGE"))
                        .show(false)
}
/*
    Sample output:

    Total Parttions: 9011

    +------+-----+------------------+
    |MAX   |MIN  |AVERAGE           |
    +------+-----+------------------+
    |135695|87694|100338.61149653122|
    +------+-----+------------------+
*/


def countBelow(df: DataFrame, min: Long) = {
  // Total partitions in the dataframe
  val totalPartitions = df.rdd.partitions.size
  println("Total Parttions: " + totalPartitions + "\n")
  val showPartitions = df.groupBy(spark_partition_id).count
  val findBelow = showPartitions.filter(col("count") < min).count
  println("Partition with less than " + min  + " rows: " + findBelow)
}

def countAbove(df: DataFrame, max: Long) = {
  // Total partitions in the dataframe
  val totalPartitions = df.rdd.partitions.size
  println("Total Parttions: " + totalPartitions + "\n")
  val showPartitions = df.groupBy(spark_partition_id).count
  val findAbove = showPartitions.filter(col("count") < max).count
  println("Partition with more than " + max  + " rows: " + findAbove)
}

def countBetween(df: DataFrame, max: Long, min: Long) = {
  // Total partitions in the dataframe
  val totalPartitions = df.rdd.partitions.size
  println("Total Parttions: " + totalPartitions + "\n")
  val showPartitions = df.groupBy(spark_partition_id).count
  val findAbove = showPartitions.filter(col("count") < max).count
  val findBelow = showPartitions.filter(col("count") < min).count
  println("Partitions with rows within " + max + "-" + min + ": " + (findAbove - findBelow).abs)
}


def checkColumnHealth(): DataFrame = {

}
