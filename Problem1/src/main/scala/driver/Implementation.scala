package driver

import additionalFunctions.ReadingFeature
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object Implementation {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    //Initialise the spark session
    val ss = SparkSession.builder().master("local").getOrCreate()
    ss.sparkContext.setLogLevel("OFF")
    val object_addfunctions = new ReadingFeature()

    //System property HADOOP_HOME is set to save the output in the local disk
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    if (args.length < 2) {
      throw new ArrayIndexOutOfBoundsException("Required argument(s) <data_file_path> <statename_path>")
    }
    else {
      val data_path=args(0) //1st argument should be absolute path of PovertyEstimates.xlsx
      val statenames=args(1) //2nd argument should be absolute path of StatesName.xlsx

      /*Read data from the file(passed as argument for the data_path variable) and also select required columns
      The selection of rows are based on some of the criteria mentioned below:
      1) Postfix the statename(postal) to Area_name having separator as space
      2) Urban_Influence_Code_2003 is odd.
      3) Rural-urban_Continuum_Code_2013 is even.
       */
      val extract_df_data = object_addfunctions.readExcel_toSpark(ss,data_path)
        .select("Stabr", "Area_name", "Urban_Influence_Code_2003", "Rural-urban_Continuum_Code_2013", "POVALL_2018", "POV017_2018")
        .withColumn("Area_name", concat_ws(" ", col("Area_name"), col("Stabr")))
        .filter(col("Urban_Influence_Code_2003") % 2 !== 0).filter(col("Rural-urban_Continuum_Code_2013") % 2 === 0)

      /*
      Read the data to DataFrame from the file (passed as argument for statename variable)
      Broadcasted the smaller table so that the data is not skewed. It's an optional but should be used if the other dataset(s)
      are having high number of records as compared to smaller tables.
       */
      val extract_df_statenames = object_addfunctions.readExcel_toSpark(ss,statenames).toDF("capital", "postal_abb")
      val statename_broad = ss.sparkContext.broadcast(extract_df_statenames)

      val joined_data = extract_df_data.join(statename_broad.value, trim(extract_df_data("Stabr")) === trim(statename_broad.value("postal_abb")))
        .select(
          statename_broad.value("capital").alias("State"),
          extract_df_data("Area_name"),
          extract_df_data("Urban_Influence_Code_2003"),
          extract_df_data("Rural-urban_Continuum_Code_2013"),
          extract_df_data("POVALL_2018"),
          extract_df_data("POV017_2018")
        )

      //The percentage of the age group>17 can be achived by All_age_count and Age_count_for_0-17.
      val grouped_data = joined_data.groupBy("State", "Area_name", "Urban_Influence_Code_2003", "Rural-urban_Continuum_Code_2013")
        .agg(round((((first(joined_data("POVALL_2018")) - first(joined_data("POV017_2018"))) / first(joined_data("POVALL_2018"))) * 100).cast(FloatType), 2).alias("POV_elder_than17_2018"))
        .select("State", "Area_name", "Urban_Influence_Code_2003", "Rural-urban_Continuum_Code_2013", "POV_elder_than17_2018")

      //The below can be changed to store it to local/HDFS based on requirement
      grouped_data.show()

      /* For local
      val filepath="" //absolute path of the output
      grouped_data.coalesce(1).write.option("header", "true").format("csv").mode("append").save(filepath)
       */

    }
  }

}
