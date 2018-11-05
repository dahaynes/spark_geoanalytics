package spatial_workflow

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum
import org.datasyslab.geospark.formatMapper.shapefileParser.{ShapefileReader, ShapefileRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, LineStringRDD, SpatialRDD}
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.apache.spark.sql.functions._

object sage_screening {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //import sparkSession.implicits._

    val sparkSession: SparkSession = SparkSession.builder().
      config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.local.dir", "/media/sf_data").
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[4]").appName("AdaptiveFilter").getOrCreate() //.set("spark.driver.memory", "2g").set("spark.executor.memory", "1g")

    sparkSession.sparkContext.setLogLevel("ERROR")

    //adds Spatial Functions (ST_*)
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    //Allows you to write geom to text
    sparkSession.udf.register("ST_SaveAsWKT", (geometry: Geometry) => (geometry.toText))
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    var spatialRDD = new SpatialRDD[Geometry]

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, "/media/sf_data/sage_data/grid_5000meters/")
    var gridDF = Adapter.toDf(spatialRDD, sparkSession)
    gridDF.createOrReplaceTempView("load")
    gridDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape), 'epsg:4326', 'epsg:26915') as geom, _c1 as id FROM load """)
    gridDF.createOrReplaceTempView("grid")


    // Synthetic Households
    val syntheticPopulationCSV = "/media/sf_data/sage_data/synthetic_population/2010_ver1_27_synth_households.txt"
    var syntheticPopulation = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(syntheticPopulationCSV)
    syntheticPopulation.createOrReplaceTempView("load")
    syntheticPopulation = sparkSession.sql(""" SELECT ST_Point( cast(longitude as Decimal(24,20)), cast(latitude as Decimal(24,20)) ) as geom, sp_id, hh_income, hh_size FROM load """)
    syntheticPopulation.createOrReplaceTempView("households")

    val syntheticPeopleCSV = "/media/sf_data/sage_data/synthetic_population/2010_ver1_27_synth_people.txt"
    var syntheticPeople = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(syntheticPeopleCSV)
    syntheticPeople.createOrReplaceTempView("people")

    val eligiblePopulation = sparkSession.sql(
      """SELECT h.sp_id, ST_Transform(geom, 'epsg:4326', 'epsg:26915') as geom, p.sex, p.age, (h.hh_income - 30350 + (10800*h.hh_size)) as income, 1 as value, p.race
        |FROM households h INNER JOIN people p ON h.sp_id = p.sp_hh_id
        |WHERE (h.hh_income - 30350 + (10800*h.hh_size) < 0 AND p.sex = 2 AND p.age >= 40)
        | OR
        |(p.sex = 2 AND p.age >= 40 AND p.race IN (3,4,5)
        |)
        |ORDER BY 5 DESC""".stripMargin)
    eligiblePopulation.createOrReplaceTempView("eligible_women")

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, "/media/sf_data/sage_data/sage_breast_clients")
    var clientsDF = Adapter.toDf(spatialRDD, sparkSession)
    clientsDF.createOrReplaceTempView("load")
    clientsDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id FROM load """)
    clientsDF.createOrReplaceTempView("all_clients")

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, "/media/sf_data/sage_data/mn_boundary")
    var boundaryDF = Adapter.toDf(spatialRDD, sparkSession)
    boundaryDF.createOrReplaceTempView("load")
    boundaryDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c6 as name FROM load WHERE _c6 = 'Minnesota' """)
    boundaryDF.createOrReplaceTempView("state_boundary")

    var stateClientsDF = sparkSession.sql(""" SELECT ST_Transform(c.geom, 'epsg:4326', 'epsg:26915') as geom, c.id FROM all_clients c INNER JOIN state_boundary b on ST_Intersects(c.geom, b.geom) """)
    stateClientsDF.createOrReplaceTempView("clients")
    clientsDF.unpersist()

    var eligible_grid_join = sparkSession.sql("""
                                            SELECT g.id, p.sp_id, ST_Distance(p.geom, g.geom) as distance, 1 as people
                                            FROM eligible_women p cross join grid g ORDER BY 1,3
                                             """.stripMargin)
    eligible_grid_join.persist().createOrReplaceTempView("grid_distance_eligible")
    eligible_grid_join.show(101)

    var clients_grid_join = sparkSession.sql("""
                                            SELECT g.id, c.id as client_id, ST_Distance(c.geom, g.geom) as distance, 1 as client_value
                                            FROM clients c cross join grid g ORDER BY 1,3
                                             """.stripMargin)
    clients_grid_join.persist().createOrReplaceTempView("grid_distance_clients") //clients_grid_join.show(101)


    // http://xinhstechblog.blogspot.com/2016/04/spark-window-functions-for-dataframes.html
    val distance_ordering = Window.partitionBy("id").orderBy("distance").rowsBetween(Long.MinValue, 0)
    //Running a cummulative of n over distance using the window
    val ordered_clients = eligible_grid_join.withColumn("number_of_people", sum(eligible_grid_join("people")).over(distance_ordering))
    ordered_clients.createOrReplaceTempView("ordered_eligible_women")


    //Calculate the Filter or population size X
    ordered_clients.filter("number_of_people = 100").
      groupBy("id").
      agg(min("distance")).
      withColumnRenamed("min(distance)", "min_distance").
      orderBy("id").
      createOrReplaceTempView("ordered_min_distance_base_population")
    ordered_clients.persist()

    var filterJoins = sparkSession.sql(
      """
      SELECT g.id, ST_SaveAsWKT(g.geom) as geom, b.min_distance
      FROM ordered_min_distance_base_population b
      INNER JOIN grid g on (g.id = b.id)
      """.stripMargin)
    filterJoins.createOrReplaceTempView("filters")

    var basePopulation = sparkSession.sql(
      """
        |SELECT id, geom, sum(d.people)*5 as total_people
        |FROM
        |(
        |SELECT g.id, g.geom, b.people
        |FROM filters g INNER JOIN grid_distance_eligible b ON (g.id = b.id)
        |WHERE b.distance <= g.min_distance
        |)d
        |GROUP BY id, geom""".stripMargin)

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

    filterAnalysis.coalesce(1).write.
      format("com.databricks.spark.csv").
      option("header", "true").
      mode("overwrite").
      save("/media/sf_data/sage_data/results/financial_AI_26915_100")
      
  }
}
