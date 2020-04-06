package com.davidretana

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */
object Coronaton {
  
  def main(args : Array[String]): Unit = {

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession
      .builder()
      .appName("Coronaton")
      .master("local[*]")
      .getOrCreate()

    // read raw datasets
    val natality = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sampleSize", 10)
      .csv(inputPath + "/natalidad*.csv")
    val race = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sampleSize", 10)
      .csv(inputPath + "/race.csv")
//    val sex = spark.read.option("header", "true").csv("/home/dretana/coronaton/sex.csv")

    // limpiar el dataset natality y añadir la columna decada
    val natalityClean = natality
      .filter(col("year")>=1970)
      .filter("state is not null")
      .withColumn("decade", concat(substring(col("year"), 0, 3), lit("0")))
      .persist()

    // nacimientos por estado y decada
    val bornsByDecadeAndState = natalityClean
      .groupBy("state", "decade").count()
      .groupBy("state").pivot("decade").agg(first("count"))
      .withColumnRenamed("1970", "B70")
      .withColumnRenamed("1980", "B80")
      .withColumnRenamed("1990", "B90")
      .withColumnRenamed("2000", "B00")

    // raza con mayor numero de nacimientos por estado y decada
    val temp1 = natalityClean.groupBy("state", "decade", "child_race").count()
    val temp2 = temp1.groupBy("state", "decade").agg(max("count").alias("count"))
    val aux = temp2.join(temp1, Seq("state", "decade", "count"), "left").drop("count")
      .join(race, col("child_race").equalTo(col("id_race")), "left").drop("child_race", "id_race")
    val biggestRaceByDecadeAndState = aux.groupBy("state").pivot("decade").agg(first("race"))
      .withColumnRenamed("1970", "Race70")
      .withColumnRenamed("1980", "Race80")
      .withColumnRenamed("1990", "Race90")
      .withColumnRenamed("2000", "Race00")

    // nacimientos de hombres por estado entre los años 1970 y 2010
    val males = natalityClean
      .filter(col("is_male") === true)
      .filter(col("year") >= 1970 && col("year") <= 2010)
      .groupBy("state").count()
      .withColumnRenamed("count", "Male")

    // nacimientos de mujeres por estado entre los años 1970 y 2010
    val females = natalityClean
      .filter(col("is_male") === false)
      .filter(col("year") >= 1970 && col("year") <= 2010)
      .groupBy("state").count()
      .withColumnRenamed("count", "Female")

    // peso medio de todos los niños nacidos, por estado
    val meanWeigthByState = natalityClean
      .filter(col("year") >= 1970 && col("year") <= 2010)
      .groupBy("state").mean("weight_pounds")
      .withColumnRenamed("avg(weight_pounds)", "Weight")

    val output = bornsByDecadeAndState
      .join(biggestRaceByDecadeAndState, Seq("state"), "left")
      .join(males, Seq("state"), "left")
      .join(females, Seq("state"), "left")
      .join(meanWeigthByState, Seq("state"), "left")

    output.write
      .option("header", "true")
      .csv(outputPath)
  }

}
