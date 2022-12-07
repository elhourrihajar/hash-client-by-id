

import Models.{JsonObject, JsonStruct}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spray.json._

object DataControl {

  def checkCSV( path:String,sparkSession: SparkSession) : DataFrame = {
    //lire le fichier json...
    val src_json = scala.io.Source.fromFile("/home/nodekamicode/hashClient/src/config/schema.json")
    val string_json = try src_json.mkString finally src_json.close()
    val jsonAst = string_json.parseJson
    print("source  "+jsonAst)

    import Configuration.JsonProtocol._

    val fields = jsonAst.convertTo[JsonStruct[JsonObject]]
    val dataframeSchema = StructType(fields.fields.map(field => {
      field.colType match {
        case "string" => StructField(field.colName, StringType)
        case "Long" => StructField(field.colName,LongType)
        case _ => StructField(field.colName, StringType)
      }
    }))
   val df:DataFrame= sparkSession
     .read
     .option("header",true)
     .schema(dataframeSchema)
     .option("mode", "DROPMALFORMED")  //.option("mode", "FAILFAST")
     .csv(path)
     .coalesce(1)
   // df.show(5)
    return df;
  }
}
