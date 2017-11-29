# Spark-Assist (Beta)
##

Spark assist was built for data engineers to optimize performance before any jobs is ready for
production. Use it on Spark-Shell to figure out issues beforehand and fix them as needed.  
</br>
Upload spark-assist.scala or clone the repo in server/local directory.  
In spark-shell, run ":load 'path to spark-assist.scala'" file to use the functions


## Usage



 * **rowsPerPartiton** : Shows number of rows per partition.  
                         _Note:_ By default, shows 20 rows. 2nd optional parameter lets you choose
                         how many rows to show. See Example 2 below.

```
# Example 1:
scala>> rowsPerPartiton(df)

  Console Output:
  ---------------

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


# Example 2:
scala>> rowsPerPartition(df,Some(5))

  Console Output:
  ---------------

  Total Parttions: 9011
  +--------------------+-----+
  |SPARK_PARTITION_ID()|count|
  +--------------------+-----+
  |1238                |19089|
  |1591                |17404|
  |1088                |17870|
  |1645                |20982|
  |833                 |18906|
  +--------------------+-----+

```         
</br>


 * **partitionStats** : Finds the lowest, maximum and average of row count per partition in a
 dataframe.

```
# Example 1:
scala>> partitionStats(df)

    console output:
    ---------------

    Total Parttions: 9011

    +------+-----+------------------+
    |MAX   |MIN  |AVERAGE           |
    +------+-----+------------------+
    |135695|87694|100338.61149653122|
    +------+-----+------------------+

```
</br>

 * **countBelow/CountAbove/countBetween** : Find how many partitions contain a specific count of
 rows below, above or between a specific number.

```
# Example 1:
scala>>	 countBelow(df, 50000)
    console output:
    ---------------

    Total Parttions: 9011
	Partition with less than 50000 rows: 2000

# Example 2:
scala>>	 countAbove(df, 100000)

     console output:
    ---------------

    Total Parttions: 9011
	Partition with more than than 100000 rows: 2000

# Example 3:
scala>>	 countBetween(df, 20000,30000)

     console output:
    ---------------

    Total Parttions: 9011
	Partitions with rows within 20000-50000: 600   

```

</br>
