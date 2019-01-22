package spatial_workflow

import java.io.File

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{min, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

object adaptive_filters_regression {

  def GetEligiblePopulation(ss: SparkSession, syntheticHouseholdCSV: String, syntheticPeopleCSV: String): DataFrame = {
    // Synthetic Households

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
        |SELECT sp_id, sex, race, age, income, value, geom, uninsured_age_sex, uninsured_income, uninsured_race, 1 as original,
        |(uninsured_age_sex + uninsured_income + uninsured_race ) as uninsured_composite
        |FROM
        |(
          |SELECT sp_id, sex, race, age, income, value, geom,
          |(value_35_44 + value_45_54 + value_55_64 + value_65_74 + value_75) as uninsured_age_sex,
          |(value_under_25000 + value_25000_50000 + value_50000_75000 + value_50000_75000 + value_75000_100000 + value_over_100000) as uninsured_income,
          |(value_white_nonhispanic + value_black + value_asian + value_pacific_islander + value_sor + value_two_races) as uninsured_race
          |FROM
          |(
            |SELECT p.sp_id, p.sex, p.race, p.age, p.income, p.value, p.geom,

            |CASE WHEN 35 <= p.age AND p.age <= 44 AND p.race NOT IN (3,4,5) THEN value*i.per_f35_44yearsnoinsurance ELSE 0 END as value_35_44,
            |CASE WHEN 45 <= p.age AND p.age <= 54 AND p.race NOT IN (3,4,5) THEN value*i.per_f45_54yearsnoinsurance ELSE 0 END as value_45_54,
            |CASE WHEN 55 <= p.age AND p.age <= 64 AND p.race NOT IN (3,4,5) THEN value*i.per_f55_64yearsnoinsurance ELSE 0 END as value_55_64,
            |CASE WHEN 65 <= p.age AND p.age <= 74 AND p.race NOT IN (3,4,5) THEN value*i.per_f65_74yearsnoinsurance ELSE 0 END as value_65_74,
            |CASE WHEN 75 <= p.age THEN value*i.per_f75yearsandovernoinsurance ELSE 0 END as value_75,

            |CASE WHEN p.income < 25000 THEN value*i.per_under25000noinsurance ELSE 0 END as value_under_25000,
            |CASE WHEN p.income <= 25000 AND p.income <= 49999 THEN value*i.per_25000_49999noinsurance ELSE 0 END as value_25000_50000,
            |CASE WHEN p.income <= 50000 AND p.income <= 74999 THEN value*i.per_50000_74999noinsurance ELSE 0 END as value_50000_75000,
            |CASE WHEN p.income <= 75000 AND p.income <= 99999 THEN value*i.per_75000_99999noinsurance ELSE 0 END as value_75000_100000,
            |CASE WHEN p.income >= 100000 THEN value*i.per_100000ormorenoinsurance ELSE 0 END as value_over_100000,

            |CASE WHEN p.race = 1 THEN value*i.perUninsured_White_notHispanic ELSE 0 END as value_white_nonhispanic,
            |CASE WHEN p.race = 2 THEN value*i.perUninsured_Black ELSE 0 END as value_black,
            |CASE WHEN p.race = 6 THEN value*i.perUninsured_Asian ELSE 0 END as value_asian,
            |CASE WHEN p.race = 7 THEN value*i.perUninsured_PacificIslander ELSE 0 END as value_pacific_islander,
            |CASE WHEN p.race = 8 THEN value*i.perUninsured_SOR ELSE 0 END as value_sor,
            |CASE WHEN p.race = 9 THEN value*i.perUninsured_TwoRaces ELSE 0 END as value_two_races,
            |CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END as priority_population
            |
            |FROM eligible_women p INNER JOIN insurance_datasets i ON ST_Intersects(p.geom, i.geom)
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

    val distance_ordering = Window.partitionBy("grid_id").orderBy("distance").rowsBetween(Long.MinValue, 0)
    //Running a cummulative of n over distance using the window
    val ordered_base_population = distanceDataFrame.withColumn("number_of_people", sum(distanceDataFrame(populationField)).over(distance_ordering))
    ordered_base_population.createOrReplaceTempView("ordered_grid_base")

    //Calculate the Filter or population size X
    val filterExpression:String = "number_of_people >= %s".format(popValue)
    val ordered_filters = ordered_base_population.filter(filterExpression).
      groupBy("grid_id").
      agg(min("distance")).
      withColumnRenamed("min(distance)", "min_distance").
      orderBy("grid_id")

    //Join the two expressions together
    val filtersize = ordered_base_population.join(ordered_filters, "grid_id")

    (filtersize)

  }

  def CalculateAdaptiveFilterValues(ss:SparkSession, orderedDistanceTableName: String, outFilePath:String): Unit ={
    /*
    Dropping this as the dataframe looks like this
    |grid_id|synthetic_id|distance|person_value|sp_id|sex|race|age|income|value|geom|  uninsured_age_sex|   uninsured_income|uninsured_race|original|uninsured_composite|   number_of_people|
    */

    var filterJoins = ss.sql(
      """
      SELECT g.id, ST_SaveAsWKT(g.geom) as geom, b.min_distance
      FROM %s b
      INNER JOIN grid g on (g.grid_id = b.grid_id)
      """.stripMargin.format(orderedDistanceTableName))
      filterJoins.createOrReplaceTempView("filters")

    var basePopulation = ss.sql(
      """
        |SELECT id, geom, sum(d.people)*5 as total_people
        |FROM
        |(
        |SELECT g.id, g.geom, b.people
        |FROM filters g INNER JOIN %s b ON (g.grid_id = b.grid_id)
        |WHERE b.distance <= g.min_distance
        |)d
        |GROUP BY id, geom""".stripMargin.format(orderedDistanceTableName))

    basePopulation.createOrReplaceTempView("denominator")


    var numerator = ss.sql("""
       |SELECT id, geom, count(n.client_value) as clients
       |FROM
       |(
       |SELECT g.id, g.geom, c.client_value
       |FROM filters g INNER JOIN grid_distance_clients c ON (g.id = c.id)
       |WHERE c.distance <= g.min_distance
       |)n
       |GROUP BY id, geom""".stripMargin)
    numerator.createOrReplaceTempView("numerator")
    //numerator.show(300)

    var filterAnalysis = ss.sql(
      """
        |SELECT d.id, ST_SaveAsWKT(d.geom) as geom, n.clients, d.total_people, n.clients/d.total_people as ratio, n.filter_size
        |FROM denominator d INNER JOIN numerator n on d.id=n.id
      """.stripMargin)
    filterAnalysis.createTempView("results")

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
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).master("local[*]").appName("AdaptiveFilter").getOrCreate()

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
    val gridFilePath = new File(workingDirectory, "sage_data/regular_grid")
    val sageClientShapefile = new File(workingDirectory, "sage_data/sage_breast_clients")
    val stateBoundaryShapefile = new File(workingDirectory, "sage_data/mn_boundary")

    val gitDirectory = new File("/media/sf_data/git/sage_spatial_analysis")
    val boundaryShapefile = new File(gitDirectory, "datasets/shapefiles/zcta")
    val age_sex = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_age_sex_2010_2014_zcta.csv")
    val race = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_race_2010_2014_zcta.csv")
    val income = new File(gitDirectory, "datasets/cleaned_insurance_data/ACS_Insurance_income_2010_2014_zcta.csv")

    val outOriginal = new File(workingDirectory,"sage_data/results/original")
    val outAgeSex = new File(workingDirectory,"sage_data/results/age_sex_insurance")
    val outRace = new File(workingDirectory,"sage_data/results/race_insurance")
    val outIncome = new File(workingDirectory,"sage_data/results/income_insurance")
    val outComposite = new File(workingDirectory,"sage_data/results/composite_insurance")


    //Derive Eligible Synthetic Population
    val eligiblePopulationDF = GetEligiblePopulation(sparkSession, householdFilePath.toString(), personFilePath.toString() )
    eligiblePopulationDF.persist().createOrReplaceTempView("eligible_women") //orderBy("sp_id")
    eligiblePopulationDF.show(20)

    //Import Grid
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,gridFilePath.toString())
    var gridDF = Adapter.toDf(spatialRDD,sparkSession)
    gridDF.createOrReplaceTempView("load")     //.show(4)
    gridDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape),'epsg:4326', 'epsg:26915') as geom, cast(_c1 as int) as grid_id FROM load LIMIT 100""")
    gridDF.createOrReplaceTempView("grid")
    gridDF.show(10)

    //Import Clients from shapefile
    val clientsDF = GetSageClients(sparkSession, sageClientShapefile.toString(), stateBoundaryShapefile.toString())
    clientsDF.createOrReplaceTempView("clients")
    //clientsDF.show(15)

    //Get insurance criteria
    val ageInsuranceDF = GetInsuranceData(sparkSession, age_sex.toString(), "age_sex_insurance")
    val raceInsuranceDF = GetInsuranceData(sparkSession, race.toString(), "race_insurance")
    val incomeInsuranceDF = GetInsuranceData(sparkSession, income.toString(), "income_insurance")

    // Get Insurance Spatial Boundary Dataset
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,boundaryShapefile.toString())
    var insuranceBoundaryDF = Adapter.toDf(spatialRDD,sparkSession)
    insuranceBoundaryDF.createOrReplaceTempView("load")
    insuranceBoundaryDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape),'epsg:4326', 'epsg:26915') as geom, cast(_c1 as int) as gid , _c2 as zcta, _c3 as geoid FROM load """)
    insuranceBoundaryDF.createOrReplaceTempView("insurance_boundary")
    //insuranceBoundaryDF.show(10)

    var spatialInsuranceData = insuranceBoundaryDF.join(ageInsuranceDF).where("geoid == Id2").join(raceInsuranceDF,"Id2").join(incomeInsuranceDF,"Id2")
    spatialInsuranceData.createOrReplaceTempView("insurance_datasets")
    spatialInsuranceData = CalculateCriteria(sparkSession)
    spatialInsuranceData.show(100)


    //Calculating Distance matrix
    val syntheticGridDistance = PerformCrossJoin(sparkSession, "eligible_women", "sp_id as synthetic_id", "grid")
    //syntheticGridDistance.show(24)
    //Joining the insurance data calculations at the person level to the grid cross join
    val syntheticGridInsuranceData = syntheticGridDistance.join(spatialInsuranceData).where("synthetic_id == sp_id")
    syntheticGridDistance.persist()
    //syntheticGridInsuranceData.show(30)


    val clientsGridDistance = PerformCrossJoin(sparkSession, "clients", "id as client_id", "grid")
    clientsGridDistance.createOrReplaceTempView("grid_distance_clients")
    clientsGridDistance.persist()



    val adaptiveComposite:DataFrame = CalculateAdaptiveFilterSize(sparkSession, 100, "uninsured_composite", syntheticGridInsuranceData)
    adaptiveComposite.createOrReplaceTempView("composite_dataset")
    adaptiveComposite.show(150)

    /*
    val adaptiveOriginal: DataFrame = CalculateAdaptiveFilterSize(sparkSession, 100, "original", syntheticGridInsuranceData)
    adaptiveOriginal.createOrReplaceTempView("original")
    val adaptiveAgeSex: DataFrame = CalculateAdaptiveFilterSize(sparkSession, 100, "uninsured_age_sex", syntheticGridInsuranceData)
    adaptiveAgeSex.createOrReplaceTempView("uninsured_age_sex")
    val adaptiveIncome:DataFrame = CalculateAdaptiveFilterSize(sparkSession, 100, "uninsured_income", syntheticGridInsuranceData)
    adaptiveIncome.createOrReplaceTempView("uninsured_income")
    val adaptiveRace:DataFrame = CalculateAdaptiveFilterSize(sparkSession, 100, "uninsured_race", syntheticGridInsuranceData)
    adaptiveRace.createOrReplaceTempView("uninsured_race")
    */

    val datasets = List( ("adaptiveComposite", outComposite))
      //("adaptiveOriginal", outOriginal ), ("adaptiveAgeSex", outAgeSex), ("adaptiveIncome", outIncome), ("adaptiveRace", outRace)

    for (d <- datasets){


      CalculateAdaptiveFilterValues(sparkSession, d._1, d._2.toString() )
    }



  }

}