package spatial_workflow

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{min, sum}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

object uninsured_race {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //import sparkSession.implicits._

    val sparkSession: SparkSession = SparkSession.builder().
      config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.local.dir", "/media/sf_data").
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).master("local[*]").appName("AdaptiveFilter").getOrCreate() //.config("geospark.join.numpartition",5000) .config("spark.local.dir","/media/sf_data")

    sparkSession.sparkContext.setLogLevel("ERROR")

    //adds Spatial Functions (ST_*)
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    sparkSession.udf.register("ST_SaveAsWKT", (geometry: Geometry) => (geometry.toText))
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    var spatialRDD = new SpatialRDD[Geometry]

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/media/sf_data/sage_data/regular_grid/")
    var gridDF = Adapter.toDf(spatialRDD,sparkSession)
    gridDF.createOrReplaceTempView("load")     //.show(4)
    gridDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape),'epsg:4326', 'epsg:26915') as geom, cast(_c1 as int) as id FROM load """)
    gridDF.createOrReplaceTempView("grid")

    // Synthetic Households
    val syntheticPopulationCSV = "/media/sf_data/sage_data/synthetic_population/2010_ver1_27_synth_households.txt"
    var syntheticPopulation = sparkSession.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPopulationCSV)
    syntheticPopulation.createOrReplaceTempView("load") // .show(20)
    syntheticPopulation = sparkSession.sql(""" SELECT ST_Point( cast(longitude as Decimal(24,20)), cast(latitude as Decimal(24,20)) ) as geom, sp_id, hh_income, hh_size FROM load """)
    syntheticPopulation.createOrReplaceTempView("households")

    val syntheticPeopleCSV = "/media/sf_data/sage_data/synthetic_population/2010_ver1_27_synth_people.txt"
    var syntheticPeople = sparkSession.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPeopleCSV)
    syntheticPeople.createOrReplaceTempView("people") //.show(20)

    val eligiblePopulation = sparkSession.sql(
      """SELECT h.sp_id, ST_Transform(geom, 'epsg:4326', 'epsg:26915') as geom, p.sex, p.age, (h.hh_income - 30350 + (10800*h.hh_size)) as income, 1 as value, p.race
        |FROM households h INNER JOIN people p ON h.sp_id = p.sp_hh_id
        |WHERE (h.hh_income - 30350 + (10800*h.hh_size) < 0 AND p.sex = 2 AND p.age >= 40)
        | OR
        |(p.sex = 2 AND p.age >= 40 AND p.race IN (3,4,5)
        |)
        |ORDER BY 5 DESC""".stripMargin)
    eligiblePopulation.persist().createOrReplaceTempView("eligible_women")

    syntheticPeople.unpersist()

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/media/sf_data/sage_data/sage_breast_clients")
    var clientsDF = Adapter.toDf(spatialRDD,sparkSession)
    clientsDF.createOrReplaceTempView("load")
    clientsDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id FROM load""")
    clientsDF.createOrReplaceTempView("all_clients")

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/media/sf_data/sage_data/mn_boundary")
    var boundaryDF = Adapter.toDf(spatialRDD,sparkSession)
    boundaryDF.createOrReplaceTempView("load")
    boundaryDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c6 as name FROM load WHERE _c6 = 'Minnesota' """)
    boundaryDF.createOrReplaceTempView("state_boundary")

    var stateClientsDF = sparkSession.sql(""" SELECT ST_Transform(c.geom, 'epsg:4326', 'epsg:26915') as geom, c.id FROM all_clients c INNER JOIN state_boundary b on ST_Intersects(c.geom, b.geom) """)
    stateClientsDF.persist().createOrReplaceTempView("clients")
    clientsDF.unpersist()

    //Reading in insurance age-sex data by census tract
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/media/sf_data/sage_data/insurance_race")
    /*_c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, _c10, _c11
    Id2,	Geography,	perUninsured_one_race	perUninsured_White,	perUninsured_Black,	perUninsured_AmericanIndian,	perUninsured_Asian,	perUninsured_PacificIslander,	perUninsured_SOR,	perUninsured_TwoRaces,	perUninsured_White_notHispanic*/
    var insuranceDF = Adapter.toDf(spatialRDD,sparkSession)
    insuranceDF.createOrReplaceTempView("load")
    insuranceDF.show(15)
    insuranceDF = sparkSession.sql(
      """
        |SELECT ST_Transform(ST_GeomFromWKT(rddshape), 'epsg:4326', 'epsg:26915') as geom,
        |_c3 as one_race, _c4 as hispanic_white, _c5 as black, _c6 as american_indian, _c7 as asian,
        |_c8 as pacific_islander, _c9 as sor, _c10 as two_races, _c11 as white_nonhispanic
        |FROM load """.stripMargin)

    insuranceDF.createOrReplaceTempView("uninsured_by_tract") //insuranceDF.show(15)

    var uninsuredPopulationDF = sparkSession.sql(
      """SELECT sp_id, sex, race, age, income, value, geom,
        |value_white_nonhispanic,value_black,value_asian,value_pacific_islander,value_sor,value_two_races,
        |(value_white_nonhispanic+value_black+value_asian+value_pacific_islander+value_sor+value_two_races+priority_population) as total_uninsured_population
        |FROM
        |(
        |SELECT p.sp_id, p.sex, p.race, p.age, p.income, p.value, p.geom,
        |i.hispanic_white, i.black, i.american_indian, i.asian, i.pacific_islander, i.sor, i.white_nonhispanic,
        |CASE WHEN p.race = 1 THEN value*i.white_nonhispanic ELSE 0 END as value_white_nonhispanic,
        |CASE WHEN p.race = 2 THEN value*i.black ELSE 0 END as value_black,
        |CASE WHEN p.race = 6 THEN value*i.asian ELSE 0 END as value_asian,
        |CASE WHEN p.race = 7 THEN value*i.pacific_islander ELSE 0 END as value_pacific_islander,
        |CASE WHEN p.race = 8 THEN value*i.sor ELSE 0 END as value_sor,
        |CASE WHEN p.race = 9 THEN value*i.two_races ELSE 0 END as value_two_races,
        |CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END as priority_population
        |FROM eligible_women p INNER JOIN uninsured_by_tract i ON ST_Intersects(p.geom, i.geom)
        |) dataset """.stripMargin)

    uninsuredPopulationDF.createOrReplaceTempView("insurance_adjusted_population")
    uninsuredPopulationDF.show(15)

    var underinsured_grid_join = sparkSession.sql("""
                                            SELECT g.id, p.sp_id, ST_Distance(p.geom, g.geom) as distance, total_uninsured_population as people, 1 as people_value
                                            FROM insurance_adjusted_population p cross join grid g ORDER BY 1,3
                                             """.stripMargin)
    underinsured_grid_join.persist().createOrReplaceTempView("grid_distance_uninsured") //.show(101)

    var clients_grid_join = sparkSession.sql("""
                                            SELECT g.id, c.id as client_id, ST_Distance(c.geom, g.geom) as distance, 1 as client_value
                                            FROM clients c cross join grid g ORDER BY 1,3
                                             """.stripMargin)
    clients_grid_join.persist().createOrReplaceTempView("grid_distance_clients") //.show(101)
    uninsuredPopulationDF.unpersist()


    /*
    Calculating a window. So that we can determine the number of n as the window increases
    http://xinhstechblog.blogspot.com/2016/04/spark-window-functions-for-dataframes.html
    */
    val distance_ordering = Window.partitionBy("id").orderBy("distance").rowsBetween(Long.MinValue, 0)
    //Running a cummulative of n over distance using the window
    val ordered_base_population = underinsured_grid_join.withColumn("number_of_people", sum(underinsured_grid_join("people")).over(distance_ordering))
    ordered_base_population.createOrReplaceTempView("ordered_grid_base")

    //ordered_base_population.show(30)

    /*
    This is used for verifying the numerator
    val ordered_clients = clients_grid_join.withColumn("num_clients", sum(clients_grid_join("client_value")).over(distance_ordering))
    ordered_clients.createOrReplaceTempView("ordered_grid_clients")
    ordered_clients.show(20)
    */

    //Calculate the Filter or population size X
    ordered_base_population.filter("number_of_people >= 100").
      groupBy("id").
      agg(min("distance")).
      withColumnRenamed("min(distance)", "min_distance").
      orderBy("id").
      createOrReplaceTempView("ordered_min_distance_base_population")
    ordered_base_population.persist()
    //ordered_base_population.show(40)


    // g.id, g.geom, min(b.distance) as min_distance GROUP BY g.id, g.geom
    var filterJoins = sparkSession.sql(
      """
      SELECT g.id, ST_SaveAsWKT(g.geom) as geom, b.min_distance
      FROM ordered_min_distance_base_population b
      INNER JOIN grid g on (g.id = b.id)
      """.stripMargin)

    filterJoins.createOrReplaceTempView("filters")
    /*
    filterJoins.coalesce(1).write.
    format("com.databricks.spark.csv").
    option("header", "true").
    mode("overwrite").
    save("/media/sf_data/sage_data/results/grid_filters")
    */

    var basePopulation = sparkSession.sql(
      """
        |SELECT id, geom, sum(d.people)*5 as total_people
        |FROM
        |(
        |SELECT g.id, g.geom, b.people
        |FROM filters g INNER JOIN grid_distance_uninsured b ON (g.id = b.id)
        |WHERE b.distance <= g.min_distance
        |)d
        |GROUP BY id, geom""".stripMargin)

    //basePopulation.show(300)
    basePopulation.createOrReplaceTempView("denominator")


    var numerator = sparkSession.sql("""
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

    var filterAnalysis = sparkSession.sql(
      """
        |SELECT d.id, d.geom, f.min_distance, n.clients, d.total_people, n.clients/d.total_people as ratio
        |FROM denominator d
        |INNER JOIN numerator n on d.id=n.id
        |INNER JOIN filters f on d.id=f.id
      """.stripMargin)
    filterAnalysis.createTempView("results")


    filterAnalysis.write.
      format("com.databricks.spark.csv").
      option("header", "true").
      mode("overwrite").
      save("/media/sf_data/sage_data/results/uninsured_race_10percent")

    sparkSession.sparkContext.stop()

  }
}
