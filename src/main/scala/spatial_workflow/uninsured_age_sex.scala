package spatial_workflow

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.apache.spark.sql.functions._



object uninsured_age_sex {

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
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/media/sf_data/sage_data/insurance_age_sex")
    /*_c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, _c10, _c11, _c12, _c13, _c14, _c15, _c16, _c17, _c18, _c19, _c20, _c21, _c22, _c23
    fid, gid, geoid10, id2, geography, per_munder, per_m6_17y, per_m18_24, per_m25_34, per_m35_44, per_m45_54, per_m55_64, per_m65_74, per_m75yea, per_funder, per_f6_17y, per_f18_24, per_f25_34, per_f35_44, per_f45_54, per_f55_64, per_f65_74, per_f75yea */
    var insuranceDF = Adapter.toDf(spatialRDD,sparkSession)
    insuranceDF.createOrReplaceTempView("load") //insuranceDF.show(15)
    insuranceDF = sparkSession.sql(
      """
        |SELECT ST_Transform(ST_GeomFromWKT(rddshape), 'epsg:4326', 'epsg:26915') as geom,
        |_c19 as per_uninsured_35_44, _c20 as per_uninsured_45_54, _c21 as per_uninsured_55_64,
        |_c22 as per_uninsured_65_74, _c23 as per_uninsured_75
        |FROM load """.stripMargin)

    insuranceDF.createOrReplaceTempView("uninsured_by_tract") //insuranceDF.show(15)

    var uninsuredPopulationDF = sparkSession.sql(
      """SELECT sp_id, sex, race, age, income, value, geom,
        |per_uninsured_35_44, per_uninsured_45_54, per_uninsured_55_64, per_uninsured_65_74, per_uninsured_75,
        |(value_35_44+value_45_54+value_55_64+value_65_74+value_75+ priority_population) as total_uninsured_population
        | FROM
        | (
        |SELECT p.sp_id, p.sex, p.race, p.age, p.income, p.value, p.geom,
        |i.per_uninsured_35_44, i.per_uninsured_45_54,i.per_uninsured_55_64, i.per_uninsured_65_74, i.per_uninsured_75,
        |CASE WHEN 35 <= p.age AND p.age <= 44 AND p.race NOT IN (3,4,5) THEN value*i.per_uninsured_35_44 ELSE 0 END as value_35_44,
        |CASE WHEN 45 <= p.age AND p.age <= 54 AND p.race NOT IN (3,4,5) THEN value*i.per_uninsured_45_54 ELSE 0 END as value_45_54,
        |CASE WHEN 55 <= p.age AND p.age <= 64 AND p.race NOT IN (3,4,5) THEN value*i.per_uninsured_55_64 ELSE 0 END as value_55_64,
        |CASE WHEN 65 <= p.age AND p.age <= 74 AND p.race NOT IN (3,4,5) THEN value*i.per_uninsured_65_74 ELSE 0 END as value_65_74,
        |CASE WHEN 75 <= p.age THEN value*i.per_uninsured_75 ELSE 0 END as value_75,
        |CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END as priority_population
        |FROM eligible_women p INNER JOIN uninsured_by_tract i ON ST_Intersects(p.geom, i.geom)
        | ) dataset """.stripMargin)

    uninsuredPopulationDF.createOrReplaceTempView("insurance_adjusted_population")
    //uninsuredPopulationDF.show(15)

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
/*    filterJoins.coalesce(1).write.
      format("com.databricks.spark.csv").
      option("header", "true").
      mode("overwrite").
      save("/media/sf_data/sage_data/results/grid_filters")*/

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
      save("/media/sf_data/sage_data/results/filter_joins_10percent1")

    sparkSession.sparkContext.stop()


  }
}
