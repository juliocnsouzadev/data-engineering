package br.com.juliocnsouza.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQLDataset {

  case class Person(id: Int, name: String, age: Int, friends: Int)
  case class AgeClassification (classification:String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")


    val peopleClassification = schemaPeople.map(func = person => {
      var ageClassfication = "baby"
      if (person.age >= 3 && person.age <= 12) {
        ageClassfication = "child"
      }
      if (person.age >= 13 && person.age <= 19) {
        ageClassfication = "teenager"
      }
      if (person.age >= 20 && person.age <= 65) {
        ageClassfication = "adult"
      }
      if (person.age > 65) {
        ageClassfication = "elderly"
      }
      return new AgeClassification(ageClassfication);

    })

    schemaPeople.withColumn("ge_classification", peopleClassification("classification"))

    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }
}
