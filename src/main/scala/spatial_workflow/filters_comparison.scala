package spatial_workflow

import java.io.File
import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{min, sum, count}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import filter.Settings

object filters_comparison {

  def GetEligiblePopulation(ss: SparkSession, syntheticHouseholdCSV: String, syntheticPeopleCSV: String): DataFrame = {
    /*
     Synthetic Households
     */


    var syntheticPopulation = ss.read.format("csv").option("delimiter",",").option("header","true").load(syntheticHouseholdCSV)
    syntheticPopulation.createOrReplaceTempView("load")
    //syntheticPopulation.sort("sp_id")show(20)
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
    /*
    eligiblePopulation.persist().createOrReplaceTempView("eligible_women")
    syntheticPeople.unpersist()
    */
    (eligiblePopulation)
  }

  def GetSageClients(ss: SparkSession, clientsShapefilePath: String, stateShapefilePath: String  ): DataFrame ={
    /*
    This function gets the clients which are represented as a shapefile
    Needs to be adapted to support different coordinate systems and checks for things like that
    */
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(ss.sparkContext, clientsShapefilePath)
    var clientsDF = Adapter.toDf(spatialRDD,ss)
    clientsDF.createOrReplaceTempView("load")
    clientsDF = ss.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id FROM load""")
    clientsDF.createOrReplaceTempView("all_clients")

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(ss.sparkContext, stateShapefilePath)
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

  def PerformCrossJoin(ss: SparkSession, table1Name:String, personValue:String, table1Column:String, table2Name:String): DataFrame ={
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
        |SELECT g.grid_id, %s, ST_Distance(p.geom, g.geom) as distance, %s as person_value
        |FROM %s p cross join %s g ORDER BY 1,3
      """.stripMargin.format(table1Column, personValue, table1Name, table2Name)

    println(crossjoinStatement)
    var crossJoinDF = ss.sql(crossjoinStatement)
    (crossJoinDF)

  }

  def CalculateCriteria(ss: SparkSession): DataFrame ={
    /*
    Test query to make sure you have the correct data

    var uninsuredPopulationDF = ss.sql(
      """
        |SELECT sp_id, sex, race, age, income, value
        |FROM eligible_women p INNER JOIN insurance_datasets i ON ST_Intersects(p.geom, i.geom)
        |ORDER by 1
      """.stripMargin)
    uninsuredPopulationDF.show(100)

    */

    var uninsuredPopulationDF = ss.sql(
      """
        |SELECT sp_id, sex, race, age, income, value, geom, priority_population as original, state_uninsured, state_uninsured_underinsured
        |FROM
        |(
          |SELECT sp_id, sex, race, age, income, value, geom, priority_population, state_uninsured, state_uninsured_underinsured
          |FROM
          |(
            |SELECT p.sp_id, p.sex, p.race, p.age, p.income, p.value, p.geom,
            |CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END as priority_population,
            |CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .126 END as state_uninsured,
            |CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .226 END as state_uninsured_underinsured
            |
            |FROM eligible_women p 
          |)
          |dataset
          |ORDER BY 1
          |)result
          """.stripMargin)

    (uninsuredPopulationDF)
  }

  def CalculateAdaptiveFilterSize(ss:SparkSession, popValue:Int, populationField: String, distanceDataFrame: DataFrame): DataFrame ={
    /*
    This function calculates the size of the spatially adaptive filter, based on the input minimum population value (popValue)

    Calculating a window. So that we can determine the number of n as the window increases
    http://xinhstechblog.blogspot.com/2016/04/spark-window-functions-for-dataframes.html
    */

    val distanceOrdering = Window.partitionBy("grid_id").orderBy("distance").rowsBetween(Long.MinValue, 0)
    //distanceDataFrame.show(5)
    //Running a cummulative of n over distance using the window
    val ordered_base_population = distanceDataFrame.withColumn("number_of_people", sum(distanceDataFrame(populationField)).over(distanceOrdering))
    ordered_base_population.createOrReplaceTempView("ordered_grid_base")

    //Calculate the Filter or population size X
    val filterExpression:String = "number_of_people >= %s".format(popValue)
    val ordered_filters = ordered_base_population.filter(filterExpression).
      groupBy("grid_id").
      agg(min("distance")).
      withColumnRenamed("min(distance)", "min_distance").
      orderBy("grid_id")

    //Join the two expressions together
    val filterSize = ordered_base_population.join(ordered_filters, "grid_id")

    (filterSize)

  }

  def CalculateAdaptiveFilterValues(ss:SparkSession, orderedDistanceTableName: String, synthPopulationField: String, outFilePath:String): Unit ={
    /*
    Dropping this as the dataframe looks like this
    |grid_id|synthetic_id|distance|person_value|sp_id|sex|race|age|income|value|geom| uninsured_age_sex| uninsured_income|uninsured_race|original|uninsured_composite| number_of_people|
    */

    var filterJoins = ss.sql(
      """
      SELECT g.grid_id, ST_SaveAsWKT(g.geom) as geom, max(b.min_distance) as distance
      FROM %s b
      INNER JOIN grid g on (g.grid_id = b.grid_id)
      GROUP BY g.grid_id, g.geom
      """.stripMargin.format(orderedDistanceTableName))
    filterJoins.createOrReplaceTempView("filters")

    /*
    filterJoins.agg( count("grid_id")).show()
    filterJoins.show(20)
    */

    var basePopulation = ss.sql(
      """
        |SELECT d.grid_id, sum(d.people)*5 as total_people
        |FROM
        |(
        |SELECT f.grid_id, b.%s as people
        |FROM filters f INNER JOIN %s b ON (f.grid_id = b.grid_id)
        |WHERE b.distance <= f.distance
        |)d
        |GROUP BY d.grid_id""".stripMargin.format(synthPopulationField, orderedDistanceTableName))

    basePopulation.createOrReplaceTempView("denominator")
    //basePopulation.show(20)


    var numerator = ss.sql("""
                             |SELECT grid_id, count(n.client_value) as clients
                             |FROM
                             |(
                             |SELECT f.grid_id, c.person_value as client_value
                             |FROM filters f INNER JOIN grid_distance_clients c ON (f.grid_id = c.grid_id)
                             |WHERE c.distance <= f.distance
                             |)n
                             |GROUP BY grid_id""".stripMargin)
    numerator.createOrReplaceTempView("numerator")
    //numerator.show(300)

    var filterAnalysis = ss.sql(
      """
        |SELECT f.grid_id, f.geom, n.clients, d.total_people, n.clients/d.total_people as ratio, f.distance
        |FROM filters f
        |INNER JOIN denominator d ON f.grid_id = d.grid_id
        |INNER JOIN numerator n ON d.grid_id = n.grid_id
      """.stripMargin)
    //filterAnalysis.createOrReplaceTempView("filter_calculation")
    //filterAnalysis.show(20)

    filterAnalysis.write.
      format("com.databricks.spark.csv").
      option("header", "true").
      mode("overwrite").
      save(outFilePath)

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //import sparkSession.implicits._

    val sparkSession: SparkSession = SparkSession.builder().
      config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.local.dir", "/media/sf_data").
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).master("local[8]").appName("AdaptiveFilter").getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    //adds Spatial Functions (ST_*)
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    sparkSession.udf.register("ST_SaveAsWKT", (geometry: Geometry) => (geometry.toText))
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    var spatialRDD = new SpatialRDD[Geometry]

    /*
    ************************************************************
    This is where you define the how everything works

    All shapefiles or spatial datasets that are loaded need to originate in WGS84 4326
    We will add a parameter that will allow you to project the dataset to a specifc CRS
    ************************************************************
    */

    //Parameters that you need to define
    val workingDirectory = new File("/media/sf_data")
    val householdFilePath = new File(workingDirectory, "sage_data/synthetic_population/2010_ver1_27_synth_households.txt")
    val personFilePath = new File( workingDirectory, "sage_data/synthetic_population/2010_ver1_27_synth_people.txt")
    val gridFilePath = new File(workingDirectory, "sage_data/regular_5000m_grid")
    val sageClientShapefile = new File(workingDirectory, "sage_data/sage_breast_clients")
    val stateBoundaryShapefile = new File(workingDirectory, "sage_data/mn_boundary")

    val gitDirectory = new File("/media/sf_data/git/sage_spatial_analysis")
    val boundaryShapefile = new File(gitDirectory, "datasets/shapefiles/zcta")
    val age_sex = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_age_sex_2010_2014_zcta.csv")
    val race = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_race_2010_2014_zcta.csv")
    val income = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_income_2010_2014_zcta.csv")

    val datasets = List (new filter.Settings("original", 100, "original", new File(workingDirectory, "sage_data/results/original"), "tract") )


    /*new filter.Settings("uninsured_composite", 100, "uninsured_composite", new File(workingDirectory, "sage_data/results/uninsured_composite")),
    new filter.Settings("uninsured_age_sex", 100, "uninsured_age_sex", new File(workingDirectory, "sage_data/results/uninsured_age_sex")),
    new filter.Settings("uninsured_income", 100, "uninsured_income", new File(workingDirectory, "sage_data/results/uninsured_income")),
    new filter.Settings("uninsured_race", 100, "uninsured_race", new File(workingDirectory, "sage_data/results/uninsured_race")) )*/


    //Derive Eligible Synthetic Population
    val eligiblePopulationDF = GetEligiblePopulation(sparkSession, householdFilePath.toString(), personFilePath.toString() )
    eligiblePopulationDF.persist().createOrReplaceTempView("eligible_women") //orderBy("sp_id")
    //eligiblePopulationDF.show(20)

    //Import Clients from shapefile
    val clientsDF = GetSageClients(sparkSession, sageClientShapefile.toString(), stateBoundaryShapefile.toString())
    clientsDF.createOrReplaceTempView("clients")

    priorityClients = CalculateCriteria(sparkSession)
    priorityClients.show(10)

    //Import Grid
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, gridFilePath.toString())
    var gridDF = Adapter.toDf(spatialRDD,sparkSession)
    gridDF.createOrReplaceTempView("load")     //.show(4)
    gridDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape),'epsg:4326', 'epsg:26915') as geom, cast(_c1 as int) as grid_id FROM load """)
    //WHERE _c1 IN (4246,4247,4249,4264,4265,4352,4353,4355,4358,4360,4361,4363,4365,4369,4384,4369,4384,4385,4472)
    gridDF.createOrReplaceTempView("grid")
    //gridDF.persist(StorageLevel.DISK_ONLY)
    //gridDF.show(10)
    //gridDF.agg( count("grid_id")).show()

    for (d  <- datasets){


      if (d.geogUnit == "tract"){
        println("Tract")
        val geogAggPath = new File(workingDirectory, "sage_data/mn_tract")
        spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, geogAggPath.toString() )
        var geoAggregator = Adapter.toDf(spatialRDD,sparkSession)

        sparkSession.sql(
          """
            |SELECT count(c) as num_clients
            |FROM clients c, %s
          """.stripMargin.format(d.geogUnit)
        )


      } else  if (d.geogUnit == "zcta"){

        println("ZCTA")
      }



    }



    


    //Calculating Distance matrix
    val syntheticGridDistance = PerformCrossJoin(sparkSession, "eligible_women", "sp_id as synthetic_id", "grid")
    //syntheticGridDistance.show(24)
    //Joining the insurance data calculations at the person level to the grid cross join
    val syntheticGridDistanceGridId = syntheticGridDistance.join(priorityClients).where("synthetic_id == sp_id")
    
    syntheticGridDistanceGridId.show(30)
    //syntheticGridInsuranceData.groupBy("grid_id").count().show()
    //syntheticGridInsuranceData.groupBy("synthetic_id").count().show()


    val clientsGridDistance = PerformCrossJoin(sparkSession, "clients", "id as client_id", "grid")
    clientsGridDistance.createOrReplaceTempView("grid_distance_clients")
    //clientsGridDistance.persist(StorageLevel.MEMORY_AND_DISK)
    clientsGridDistance.show(25)


    for (d <- datasets){
      val df = CalculateAdaptiveFilterSize(sparkSession, d.popThreshold, d.populationField, syntheticGridDistanceGridId)
      df.createOrReplaceTempView(d.name)
      //df.show(80)
      println(d.outDirectory.toString())
      CalculateAdaptiveFilterValues(sparkSession, d.name, d.populationField, d.outDirectory.toString())
    }

    sparkSession.close()

  }

}