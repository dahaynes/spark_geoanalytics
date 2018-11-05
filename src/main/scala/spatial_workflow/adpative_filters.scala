package spatial_workflow

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import geotrellis.vector.{Point, PointFeature}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.geosparksql.expressions.ST_GeomFromWKT
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.formatMapper.shapefileParser.{ShapefileRDD, ShapefileReader}
//import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, LineStringRDD, SpatialRDD}
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.apache.spark.sql.SQLContext
import scala.io.StdIn
import java.io.File
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import geotrellis.vector.io._



object adpative_filters {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkSession:SparkSession = SparkSession.builder().
      config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.local.dir", "/media/sf_data").
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[4]").appName("AdaptiveFilter").getOrCreate()

    //sparkSession.conf.set("spark.executor.memory", "30g") //set("spark.driver.memory", "2g")
    sparkSession.sparkContext.setLogLevel("ERROR")

    //adds Spatial Functions (ST_*)
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    sparkSession.udf.register("ST_SaveAsWKT", (geometry: Geometry) => (geometry.toText))
    //val sc = new SparkContext(sparkSession:sparkContext)
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)

    var spatialRDD = new SpatialRDD[Geometry]




    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/media/sf_data/sage_data/grid_5000meters/")
    var gridDF = Adapter.toDf(spatialRDD,sparkSession)
    gridDF.createOrReplaceTempView("load")
    gridDF = sparkSession.sql(""" SELECT ST_Transform(ST_GeomFromWKT(rddshape), 'epsg:4326', 'epsg:26915') as geom, _c1 as id FROM load""")
    gridDF.createOrReplaceTempView("grid")


    // Synthetic Households
    val syntheticPopulationCSV = "/media/sf_data/sage_data/synthetic_population/2010_ver1_27_synth_households.txt"
    var syntheticPopulation = sparkSession.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPopulationCSV)
    syntheticPopulation.createOrReplaceTempView("load")
    syntheticPopulation =sparkSession.sql(""" SELECT ST_Point( cast(longitude as Decimal(24,20)), cast(latitude as Decimal(24,20)) ) as geom, sp_id, hh_income, hh_size FROM load """)
    syntheticPopulation.createOrReplaceTempView("households")


    val syntheticPeopleCSV = "/media/sf_data/sage_data/synthetic_population/2010_ver1_27_synth_people.txt"
    var syntheticPeople = sparkSession.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPeopleCSV)
    syntheticPeople.createOrReplaceTempView("people")
    //syntheticPeople.show(20)

    val eligiblePopulation = sparkSession.sql(
      """SELECT h.sp_id, ST_Transform(geom, 'epsg:4326', 'epsg:26915') as geom, p.sex, p.age, (h.hh_income - 30350 + (10800*h.hh_size)) as income
        |FROM households h INNER JOIN people p ON h.sp_id = p.sp_hh_id
        |WHERE (h.hh_income - 30350 + (10800*h.hh_size) < 0 AND p.sex = 2 AND p.age >= 40)
        | OR
        |(p.sex = 2 AND p.age >= 40 AND p.race IN (3,4,5)
        |)
        |ORDER BY 5 DESC""".stripMargin)
    eligiblePopulation.createOrReplaceTempView("eligible_women")
    // eligiblePopulation.show(200)

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/media/sf_data/sage_data/sage_breast_clients")
    var clientsDF = Adapter.toDf(spatialRDD,sparkSession)
    clientsDF.createOrReplaceTempView("load")
    //clientsDF.show(10)
    clientsDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id FROM load """)
    clientsDF.createOrReplaceTempView("all_clients")

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/media/sf_data/sage_data/mn_boundary")
    var boundaryDF = Adapter.toDf(spatialRDD,sparkSession)
    boundaryDF.createOrReplaceTempView("load")
    boundaryDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c6 as name FROM load WHERE _c6 = 'Minnesota' """)
    boundaryDF.createOrReplaceTempView("state_boundary")

    var stateClientsDF = sparkSession.sql(""" SELECT ST_Transform(c.geom, 'epsg:4326', 'epsg:26915') as geom, c.id FROM all_clients c INNER JOIN state_boundary b on ST_Intersects(c.geom, b.geom) """)
    stateClientsDF.createOrReplaceTempView("clients")

    val clients_grid_join = sparkSession.sql(
      """ SELECT g.id, p.sp_id, ST_Distance(p.geom, g.geom) as distance, 1 as people
        |FROM eligible_women p cross join grid g ORDER BY 1,2 """.stripMargin)
    // clients_tracts_join.show(10)
    // http://xinhstechblog.blogspot.com/2016/04/spark-window-functions-for-dataframes.html
    val distance_ordered_clients = Window.partitionBy("id").orderBy("distance").rowsBetween(Long.MinValue, 0)
    val ordered_clients = clients_grid_join.withColumn("number_of_people", sum(clients_grid_join("people")).over(distance_ordered_clients))
    ordered_clients.createOrReplaceTempView("ordered_eligible_women")
    //ordered_clients.show(30)


    //, count(c.client_id) as number_of_clients
    //ST_Distance(f.geom, c.geom) as distance_calc, f.distance

    val filterJoins = sparkSession.sql("""
        |SELECT id, ST_SaveAsWKT(geom) as geom, number_of_clients, number_of_people, number_of_clients/cast(number_of_people as float) as ratio
        |FROM
        | (
            |SELECT g.id, g.geom, g.number_of_people*5 as number_of_people, count(c.id) as number_of_clients
            |FROM
            | (
                |SELECT DISTINCT w.id, w.number_of_people, w.distance, g.geom
                |FROM ordered_eligible_women w
                |INNER JOIN grid g on (g.id = w.id)
                |WHERE number_of_people = 100
            |) g CROSS JOIN clients c
            |WHERE ST_Distance(g.geom, c.geom) <= g.distance
            |GROUP BY g.id, g.geom, g.number_of_people
        |) results""".stripMargin)
    filterJoins.show(200)

    // First SQL defines the size of the adaptive filter
    // Second SQL gets all points that are within the size of the adaptive filter
    // Last SQL returns the results

    //var spatialRDD = new SpatialRDD[Geometry]
    //spatialRDD.rawSpatialRDD = Adapter.toRdd(filterJoins)
    //
    filterJoins.coalesce(1).write.
      format("com.databricks.spark.csv").
      option("header", "true").
      mode("overwrite").
      save("/media/sf_data/sage_data/results/financial_AI_26195")



    /*
    val datapoints = filterJoins.select("geom","ratio")
    //datapoints.foreach()

    val p4 = PointFeature(Point(2.1,3.6),3)
    val p5 = PointFeature(Point(4.1,1.6),5)

    val points = filterJoins.toJSON
    */

    sparkSession.sparkContext.stop()

  }

}
