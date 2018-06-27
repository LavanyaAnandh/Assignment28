package Khafka

import org.apache.spark.sql.SparkSession

object Assignment28 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Mlib Assignment")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    println("Spark Session Object created")

    //Now load the data from the file
    val Delayed_Flight = spark.read.format("csv")
      .option("header", "true")
      .load("D:\\Lavanya\\DelayedFlights.csv").toDF()
    println("Delayed_Flight Data->>" + Delayed_Flight.count()) //data count o check he nmber ofrows presentin the file

    Delayed_Flight.registerTempTable("FlightDetails") //Registering as temporary table FlightDetails.
    println("Dataframe Registered as table !")
    Delayed_Flight.show()
    //Query to get the top 5 most visited destination.
    val destination = spark.sql("select Dest , count(Dest) from FlightDetails group by Dest order by count(Dest) desc").toDF().take(5)
    println(s"Gives the top 5 most visited  Destination by the user  ${destination.foreach(println)} ")

    //Query to get the month that has maximium cancellation due to bad weather.
    val cancelled = spark.sql("select Month , count(cancelled) from FlightDetails where CancellationCode = 'B' and Cancelled = 1 group by Month order by count(cancelled) desc").toDF().take(1)
    println(s"Gives the month that has maximium cancellation due to bad weather  ${cancelled.foreach(println)} ")

    //Query to get the route that has maximium diversions.
    val Route = spark.sql("select Origin, Dest , count(Diverted) from FlightDetails where Diverted = 1 group by Origin, Dest order by count(Diverted) desc").toDF().take(1)
    println(s"Gives the route that has seen the maximium diversions  ${Route.foreach(println)} ")

  }
}
