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
    /*stateClientsDF.persist().createOrReplaceTempView("clients")
    clientsDF.unpersist()*/

    (stateClientsDF)
  }

  def GetInsuranceData(ss:SparkSession, csvFilePath: String, dataFrameName: String): DataFrame ={
    /*
    This function will read a csv file with headers and return a dataframe object
    */

    var df = ss.read.format("csv").option("delimiter",",").option("header","true").load(csvFilePath)
    df.createOrReplaceTempView(dataFrameName) // .show(20)

    (df)

  }

  def PerformCrossJoin(ss: SparkSession, table1Name:String, table1Column:String, table2Name:String): DataFrame ={
    /*
    Function is trying to abstract these two larger cross joins into a single function

    var underinsured_grid_join = sparkSession.sql("""
                                            SELECT g.id, p.sp_id, ST_Distance(p.geom, g.geom) as distance, total_uninsured_population as people, 1 as people_value
                                            FROM insurance_adjusted_population p cross join grid g ORDER BY 1,3
                                             """.stripMargin)
    underinsured_grid_join.createOrReplaceTempView("grid_distance_uninsured") //.show(101)


    var clients_grid_join = sparkSession.sql("""
                                            SELECT g.id, c.id as client_id, ST_Distance(c.geom, g.geom) as distance, 1 as client_value
                                            FROM clients c cross join grid g ORDER BY 1,3
                                             """.stripMargin)
    clients_grid_join.createOrReplaceTempView("grid_distance_clients") //.show(101)

    */

    var crossjoinStatement:String =
      """
        |SELECT g.grid_id, %s, ST_Distance(p.geom, g.geom) as distance, 1 as person_value
        |FROM %s p cross join %s g ORDER BY 1,3
      """.stripMargin.format(table1Column, table1Name, table2Name)

    println(crossjoinStatement)

    var crossJoinStart = System.currentTimeMillis()
    var crossJoinDF = ss.sql(crossjoinStatement)
    var crossJoinStop = System.currentTimeMillis()

    println(s"Time to complete cross join $crossJoinStop-$crossJoinStart")

    (crossJoinDF)


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

    val gitDirectory = new File("/media/sf_data/git/sage_spatial_analysis")
    val boundaryShapefile = new File(gitDirectory, "datasets/shapefiles/zcta")
    val age_sex = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_age_sex_2010_2014_zcta.csv")
    val race = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_race_2010_2014_zcta.csv")
    val income = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_income_2010_2014_zcta.csv")




    val eligiblePopulationDF = GetEligiblePopulation(sparkSession, householdFilePath.toString(), personFilePath.toString() )
    eligiblePopulationDF.show(20)

    //Get Grid
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,gridFilePath.toString())
    var gridDF = Adapter.toDf(spatialRDD,sparkSession)
    gridDF.createOrReplaceTempView("load")     //.show(4)
    gridDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape),'epsg:4326', 'epsg:26915') as geom, cast(_c1 as int) as grid_id FROM load LIMIT 50""")
    gridDF.createOrReplaceTempView("grid")
    gridDF.show(10)

    val clientsDF = GetSageClients(sparkSession, sageClientShapefile.toString(), stateBoundaryShapefile.toString())
    clientsDF.persist().createOrReplaceTempView("clients")
    clientsDF.show(15)


    val ageInsuranceDF = GetInsuranceData(sparkSession, age_sex.toString(), "age_sex_insurance")
    /*
    val raceInsuranceDF = GetInsuranceData(sparkSession, race.toString())
    val incomeInsuranceDF = GetInsuranceData(sparkSession, income.toString())
    */
    ageInsuranceDF.show(20)


    // Get Insurance Spatial Boundary Dataset
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,boundaryShapefile.toString())
    var insuranceBoundaryDF = Adapter.toDf(spatialRDD,sparkSession)
    insuranceBoundaryDF.createOrReplaceTempView("load")
    insuranceBoundaryDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape),'epsg:4326', 'epsg:26915') as geom, cast(_c1 as int) as gid , _c2 as zcta, _c3 as geoid FROM load """)
    insuranceBoundaryDF.createOrReplaceTempView("insurance_boundary")
    insuranceBoundaryDF.show(10)

    //PerformCrossJoin(sparkSession, "clients", "sp_id as synthetic_id", "grid")
    //insuranceBoundaryDF.join(ageInsuranceDF).where("Id2 == geoid")
    //($"Id2"=== $"geiud")

  }

}
