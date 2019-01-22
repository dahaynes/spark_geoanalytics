package spatial_workflow

import java.io.File

object filter {
  case class settings(name: String, popThreshold: Int, populationField:String, outDirectory: File)
}
