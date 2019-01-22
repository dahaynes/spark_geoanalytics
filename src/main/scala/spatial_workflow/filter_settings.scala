package spatial_workflow

import java.io.File

import org.apache.spark.sql.DataFrame

object filter_settings {
  case class filterSettings(name: String,  outDirectory: File, popValue: Int, popField: String)
}
