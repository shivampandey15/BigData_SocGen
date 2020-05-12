package additionalFunctions
import exceptions.{NoDataExpection, NoFileExpection}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}

import scala.io.BufferedSource

class CSVFileRead {

  def FileExistsOrNot(path:String): Boolean ={
    Files.exists(Paths.get(path))
  }

  def csvread(ss: SparkSession, path: String, header_status: String): DataFrame = {
     if(!FileExistsOrNot(path))
      throw new NoFileExpection("File Not Found" + path)
     else {
       val read_return = ss.read.format("csv").option("header", header_status).load(path)
       if(read_return.rdd.isEmpty())
         throw new NoDataExpection("Empty File" + path)
       else
       read_return
     }
  }

  def withoutsparkCSVRead(path: String): BufferedSource = {
    if (!FileExistsOrNot(path))
      throw new NoFileExpection("File Not Found" + path)
    else {
     val read_return = scala.io.Source.fromFile(path)
      if(read_return.isEmpty)
        throw new NoDataExpection("Empty File" + path)
      else
      read_return
    }
  }
}

