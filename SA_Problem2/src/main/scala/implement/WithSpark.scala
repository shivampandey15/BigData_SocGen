package implement

import additionalFunctions.CSVFileRead
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

class WithSpark extends Implement {

  def initiateProcess(path:String): Unit = {

    //Initialise the spark session
    Logger.getLogger("org").setLevel(Level.OFF)
    val ss = SparkSession.builder().master("local").getOrCreate()
    ss.sparkContext.setLogLevel("OFF")
    Logger.getLogger("org").setLevel(Level.OFF)

    //System property HADOOP_HOME is set to save the output in the local disk
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    try {
      //read the csv file to a DataFrame and select only required columns
      val obj_CSVFileRead = new CSVFileRead()
      val header_status = "true"
      val read_aut = obj_CSVFileRead.csvread(ss,path,header_status).select("aua", "sa", "res_state_name")

      // Filter the DataFrame where aua is numeric,sa is also numeric and res_state_name is not Delhi
      val filtered_data_sa = read_aut.filter((read_aut("aua") rlike "^[0-9]+$") && (read_aut("sa") rlike "^[0-9]+$") && read_aut("res_state_name") != "Delhi").select("sa")

      //Store the DataFrame to local/HDFS
      //show() for the top 20 records
      filtered_data_sa.show()

      /* For local
    val filepath="" //absolute path of the output
    filtered_data_sa.coalesce(1).write.option("header", "true").format("csv").mode("append").save(filepath)
     */
    }
    catch {
      case unknown: Exception => {
        println(s"Exception Occurred: $unknown")
      }
    }
  }
}
