package spatial_workflow

import java.io.File

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

object adaptive_filters_regression {

  def GetEligiblePopulation(ss: SparkSession, syntheticHouseholdCSV: String, syntheticPeopleCSV: String): DataFrame = {
    // Synthetic Households

    var syntheticPopulation = ss.read.format("csv").option("delimiter",",").option("header","true").load(syntheticHouseholdCSV)
    syntheticPopulation.createOrReplaceTempView("load") // .show(20)
    syntheticPopulation = ss.sql(""" SELECT ST_Point( cast(longitude as Decimal(24,20)), cast(latitude as Decimal(24,20)) ) as geom, sp_id, hh_income, hh_size FROM load """)
    syntheticPopulation.createOrReplaceTempView("households")

    var syntheticPeople = ss.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPeopleCSV)
    syntheticPeople.createOrReplaceTempView("people") //.show(20)

    val eligiblePopulation = ss.sql(
      """SELECT h.sp_id, ST_Transform(geom, 'epsg:4326', 'epsg:26915') as geom, p.sex, p.age, (h.hh_income - 30350 + (10800*h.hh_size)) as adjusted_income, 1 as value, p.race, h.hh_income as income
        |FROM households h INNER JOIN people p ON h.sp_id = p.sp_hh_id
        |WHERE (h.hh_income - 30350 + (10800*h.hh_size) < 0 AND p.sex = 2 AND p.age >= 40)
        | OR
        |(p.sex = 2 AND p.age >= 40 AND p.race IN (3,4,5)
        |)
        |ORDER BY 5 DESC""".stripMargin)
    eligiblePopulation.persist().createOrReplaceTempView("eligible_women")

    syntheticPeople.unpersist()
    (eligiblePopulation)
  }

  def GetSageClients(ss: SparkSession, clientsShapefilePath: String, stateShapefilePath: String  ): DataFrame ={
    /*
    This function gets the clients which are represented as a shapefile
    Needs to be adapted to support different coordinate systems and checks for things like that
    */
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(ss.sparkContext,"/media/sf_data/sage_data/sage_breast_clients")
    var clientsDF = Adapter.toDf(spatialRDD,ss)
    clientsDF.createOrReplaceTempView("load")
    clientsDF = ss.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id FROM load""")
    clientsDF.createOrReplaceTempView("all_clients")

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(ss.sparkContext,"/media/sf_data/sage_data/mn_boundary")
    var boundaryDF = Adapter.toDf(spatialRDD,ss)
    boundaryDF.createOrReplaceTempView("load")
    boundaryDF = ss.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c6 as name FROM load WHERE _c6 = 'Minnesota' """)
    boundaryDF.createOrReplaceTempView("state_boundary")

    var stateClientsDF = ss.sql(""" SELECT ST_Transform(c.geom, 'epsg:4326', 'epsg:26915') as geom, c.id FROM all_clients c INNER JOIN state_boundary b on ST_Intersects(c.geom, b.geom) """)
    stateClientsDF.persist().createOrReplaceTempView("clients")
    clientsDF.unpersist()

    (stateClientsDF)
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //import sparkSession.implicits._

    val sparkSession: SparkSession = SparkSession.builder().
      config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.local.dir", "/media/sf_data").
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).master("local[*]").appName("AdaptiveFilter").getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    //adds Spatial Functions (ST_*)
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    sparkSession.udf.register("ST_SaveAsWKT", (geometry: Geometry) => (geometry.toText))
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    var spatialRDD = new SpatialRDD[Geometry]

    /*This is where you define the how everything works*/

    //Get Eligible Population Data
    val workingDirectory = new File("/media/sf_data")
    val householdFilePath = new File(workingDirectory, "sage_data/synthetic_population/2010_ver1_27_synth_households.txt")
    val personFilePath = new File( workingDirectory, "sage_data/synthetic_population/2010_ver1_27_synth_people.txt")
    val gridFilePath = new File(workingDirectory, "sage_data/regular_grid")
    val sageClientShapefile = new File(workingDirectory, "sage_data/sage_breast_clients")
    val stateBoundaryShapefile = new File(workingDirectory, "sage_data/mn_boundary")



    val eligiblePopulationDF = GetEligiblePopulation(sparkSession, householdFilePath.toString(), personFilePath.toString() )
    eligiblePopulationDF.show(20)

    //Get Grid
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,gridFilePath.toString())
    var gridDF = Adapter.toDf(spatialRDD,sparkSession)
    gridDF.createOrReplaceTempView("load")     //.show(4)
    gridDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape),'epsg:4326', 'epsg:26915') as geom, cast(_c1 as int) as id FROM load """)
    gridDF.createOrReplaceTempView("grid")
    gridDF.show(10)
  }

}
