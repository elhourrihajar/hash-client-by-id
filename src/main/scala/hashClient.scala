import scopt.{OParser, OParserBuilder,Read}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.sys.process._
import scala.sys.process.ProcessLogger
import spray.json._
object hashClient extends App {
                      val spark =  SparkSession.builder.appName("delete").getOrCreate()
                      //var df = spark.read.option("header",true).csv("hdfs:/exam_elhourri_bihlal/bronze/GOLD_DATA.csv").coalesce(1)
                      var df = DataControl.checkCSV("hdfs:/exam_elhourri_bihlal/bronze/GOLD_DATA.csv", spark)
                      println("ok")
                      case class Arguments(inputHash:Long =0)
                      val parser = new scopt.OptionParser[Arguments]("Parsing application") {

        
                      opt[Long]('o', "inputHash").required().valueName("").action((value, arguments) => arguments.copy(inputHash = value))

 

}

                       def run(arguments: Arguments): Unit = {
                                  
                                    println("Input Hash:" + arguments.inputHash)
                                    
          





                             //hash
                                    import java.util.UUID

                                         df = df.withColumn("first_name", when(col("id") === arguments.inputHash,UUID.randomUUID.toString).otherwise(col("first_name")))
                                         df = df.withColumn("last_name", when(col("id") ===  arguments.inputHash,UUID.randomUUID.toString).otherwise(col("last_name")))
                                         df = df.withColumn("address", when(col("id") ===  arguments.inputHash,UUID.randomUUID.toString).otherwise(col("address")))
                                         df= df.withColumn("souscription_date", when(col("id") ===  arguments.inputHash,UUID.randomUUID.toString).otherwise(col("souscription_date")))
                                         val df2 = df
                                   
                                         df2.repartition(1).write.format("com.databricks.spark.csv").option("header",true).mode("overwrite").save("hdfs:/exam_elhourri_bihlal/bronze/GOLD_DATA2.csv")

                                         import scala.sys.process._

                                         s"hdfs dfs -rm /exam_elhourri_bihlal/bronze/GOLD_DATA.csv".!
                                         s"hdfs dfs -mv /exam_elhourri_bihlal/bronze/GOLD_DATA2.csv /exam_elhourri_bihlal/bronze/GOLD_DATA.csv".!
                                         df2.show(10,false)
            }

                      parser.parse(args, Arguments()) match {
                               case Some(arguments) => run(arguments)
                                case None =>
    }

}
