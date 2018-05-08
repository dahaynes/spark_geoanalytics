import sbt.Keys.{libraryDependencies, version}


lazy val root = (project in file(".")).
  settings(
    name := "spatial_spark_analytics",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization := "org.datasyslab",

    publishMavenStyle := true
  )

val SparkVersion = "2.2.1"

val GeoSparkVersion = "1.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % SparkVersion % "compile",
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_2.2" % GeoSparkVersion,
  "com.databricks" % "spark-csv_2.10" % "1.5.0"
)

/*
  assemblyMergeStrategy in assembly := {
  case PathList("org.datasyslab", "geospark", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}*/

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers +=
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"