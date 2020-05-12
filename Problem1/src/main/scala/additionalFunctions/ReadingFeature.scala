package additionalFunctions

import java.nio.file.{Files, Paths}

import exceptions.{NoFileException,EmptyFileException}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadingFeature {

  def FileExistsOrNot(path: String): Boolean = {
    Files.exists(Paths.get(path))
  }

  def readExcel_toSpark(ss: SparkSession, file_path: String): DataFrame = {
    if(!FileExistsOrNot(file_path))
      throw new NoFileException("File Not Found" + file_path)
    else {
    val read_data = ss.read
      .format("com.crealytics.spark.excel")
      .option("location", file_path)
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("addColorColumns", "false")
      .load()
      if(read_data.rdd.isEmpty())
        {
        throw new EmptyFileException("File is empty" + file_path)
        }
      else
        read_data
    }
  }
}
