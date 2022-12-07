package Configuration

import Models.{JsonObject, JsonStruct}
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}
import spray.json.DefaultJsonProtocol.jsonFormat2

object JsonProtocol extends DefaultJsonProtocol {
  implicit val configElement: RootJsonFormat[JsonObject] = jsonFormat2(JsonObject.apply)
  implicit def dataFileConfig[JsonObject : JsonFormat]: RootJsonFormat[JsonStruct[JsonObject]] = jsonFormat1(JsonStruct.apply[JsonObject])

}
