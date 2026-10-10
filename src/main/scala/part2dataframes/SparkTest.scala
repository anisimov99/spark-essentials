package part2dataframes
import org.openjdk.jol.info.{ClassLayout, GraphLayout}
import org.apache.spark.sql.{SparkSession, functions => F, types => T}
import java.time.LocalDateTime

object SparkTest {

  val spark = SparkSession
    .builder()
    .appName("My test")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    case class Person(name: String, age: Int)

    // --- Size of a single object ---
    val p = Person("abcd", 30)

    // Shallow layout: headers, fields, padding of the Person object itself
    println(ClassLayout.parseInstance(p).toPrintable)

    // Deep size: Person + its String + the String's internal char/byte[] array
    println("Total retained bytes: " + GraphLayout.parseInstance(p).totalSize())
    
    
    // ----------------- EXERCISE 1 -----------------------
    val myData = Seq(
      (1, "Nick", LocalDateTime.parse("2026-07-26T12:00:00")),
      (2, "Bob", LocalDateTime.parse("2026-07-26T13:00:00"))
    )
    // create dataFrame #1
    val myDF = spark.createDataFrame(myData)
    myDF.show()

    // create dataFrame #2
    val myDF2 = myData.toDF("id", "name", "lastLogin")
    myDF2.show()

    val bandsDF = spark.read
      .format("json")
      .option("inferSchema", true)
      .load("src/main/resources/data/bands.json")
    bandsDF.show()
    bandsDF.printSchema()
    println(bandsDF.count())

    val mySchema = T.StructType(
      Array(
        T.StructField("id", T.LongType),
        T.StructField("name", T.StringType),
        T.StructField("id", T.TimestampType)
      )
    )

    // ----------------- EXERCISE 2 -----------------------
    val dbConOptions = Map(
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "driver" -> "org.postgresql.Driver",
      "user" -> "docker",
      "password" -> "docker"
    )

    // val postgresDF = spark.read
    //   .format("jdbc")
    //   .options(
    //     dbConOptions ++ Map(
    //       "dbtable" -> "public.employees"
    //     )
    //   )
    //   .load()

    // postgresDF.show()

    val moviesDF = spark.read
      .format("json")
      .option("inferSchema", true)
      .load("src/main/resources/data/movies.json")

    // csv write
    moviesDF.write
      .mode("append")
      .format("csv")
      .option("sep", "\t")
      .save("src/main/resources/data/my_movies.csv")

    // parquet write
    moviesDF.write.mode("append").save("src/main/resources/data/my_movies.parquet")

    // db write
    // moviesDF.write
    //   .mode("append")
    //   .format("jdbc")
    //   .options(
    //     dbConOptions ++ Map(
    //       "dbtable" -> "public.movies"
    //     )
    //   )
    //   .save()

    // ----------------- EXERCISE 3 -----------------------
    val myMoviesDF = spark.read.format("json").load("src/main/resources/data/movies.json")
    myMoviesDF.select("Title", "Release_Date").show()
    myMoviesDF.selectExpr("Title", "coalesce(US_Gross, 0) + coalesce(Worldwide_Gross,0) + coalesce(US_DVD_Sales,0) as Total_Revenue").show()
    myMoviesDF.select("Title", "Major_Genre", "IMDB_Rating").filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").show()
  }
}
