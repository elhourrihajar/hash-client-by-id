import scopt.{OParser, OParserBuilder,Read}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.sys.process._
import scala.sys.process.ProcessLogger
object deleteApp extends App {
                      val spark =  SparkSession.builder.appName("delete").getOrCreate()
                      var df = spark.read.option("header",true).csv("hdfs:/exam_elhourri_bihlal/bronze/GOLD_DATA.csv").coalesce(1)
                      println("ok")
                      case class Arguments(inputDir: Long = 0)
                      val parser = new scopt.OptionParser[Arguments]("Parsing application") {

                      opt[Long]('i', "inputDir").
                      required().valueName("").action((value, arguments) => arguments.copy(inputDir = value))
           }
                       def run(arguments: Arguments): Unit = {
                                    println("Input Dir:" + arguments.inputDir)
                                    val df2 = df.filter(" id !="+"'"+arguments.inputDir+"'")
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
