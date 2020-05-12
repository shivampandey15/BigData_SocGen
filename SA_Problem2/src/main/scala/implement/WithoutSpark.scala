package implement
import additionalFunctions.CSVFileRead
import org.apache.spark.sql.SparkSession

class WithoutSpark extends Implement {
 def initiateProcess(path: String): Unit = {
   val dataSource = new CSVFileRead().withoutsparkCSVRead(path)  //call the UDF for reading a file
   println("Service Agency")
   for (line <- dataSource.getLines.drop(1)) {
     val cols = line.split(",").map(_.trim)
     //output the Service Agency for which AUA is greater than 650000, SA is numeric and resident state is not Delhi
         if(cols(2).matches("^[0-9]+$") && cols(3).matches("^[0-9]+$") && cols(128)!="Delhi"){
          println(cols(3))
         }
   }
 }

}
