package spatial_workflow

import java.io.File

object filter {
  case class Settings(name: String, popThreshold: Int, populationField:String, outDirectory: File)
  case class InsuranceBeta(age1:Float )
}
