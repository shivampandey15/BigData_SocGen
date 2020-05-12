package problem2_WithSpark_Driver
import implement.{WithSpark, WithoutSpark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object InitiateProblem2 {
  def main(args: Array[String]): Unit = {

    //Enter the absolute path of the auth.csv/file which you have to use as your source path (local path)
    println("Please enter your auth.csv absolute path")
    val path = readLine().toString

    while(true) {
      //take input from user 1 for to run WithoutSpark, 2 for to run WithSpark and 3 to exit
      print("Please enter your choice -->\n1) Execute without Spark\n2)Execute with Spark\n3)Exit\nInput=")
      val read_choice = readInt()

      read_choice match {
        //Case 1 initiates process for without spark
        case 1 => {
          val obj_WithoutSpark = new WithoutSpark()
          obj_WithoutSpark.initiateProcess(path)
        }
        //Case 2 initiates process for with Spark
        case 2 => {
          val obj_WithSpark = new WithSpark()
          obj_WithSpark.initiateProcess(path)
        }
        case 3 => System.exit(0)
        case _ => println("Please enter the correct choice")
      }
    }
    }
  }
