package spatial_workflow

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.geosparksql.expressions.ST_GeomFromWKT
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

//import geotrellis.vector.io._



object adpative_filters {

  def main(args: Array[String]): Unit = {


    val sparkSession:SparkSession = SparkSession.builder().
      config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[2]").appName("AdaptiveFilter").getOrCreate()

    //adds Spatial Functions (ST_*)
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

    //val sc = new SparkContext(sparkSession:sparkContext)
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)

    var spatialRDD = new SpatialRDD[Geometry]


    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/home/david/SAGE/mn_tracts")
    // ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, nycArealandmarkShapefileLocation)
    var tractsDF = Adapter.toDf(spatialRDD,sparkSession)
    tractsDF.createOrReplaceTempView("load")
    tractsDF = sparkSession.sql("""SELECT ST_Centroid(ST_GeomFromWKT(rddshape)) as geom, _c4 as tract_id, _c6 as tract_name FROM load Limit 20""")
    tractsDF.createOrReplaceTempView("tracts")
    //tractsDF.show(10)

    // Synthetic Households
    val syntheticPopulationCSV = "/home/david/SAGE/households/households.csv"
    var syntheticPopulation = sparkSession.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPopulationCSV)
    syntheticPopulation.createOrReplaceTempView("load")
    // syntheticPopulation.show(20)
    syntheticPopulation =sparkSession.sql(""" SELECT ST_Point( cast(longitude as Decimal(24,20)), cast(latitude as Decimal(24,20)) ) as geom, sp_id, hh_income, hh_size FROM load """)
    //syntheticPopulation.show(20)
    syntheticPopulation.createOrReplaceTempView("households")


    val syntheticPeopleCSV = "/home/david/SAGE/households/people.csv"
    var syntheticPeople = sparkSession.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPeopleCSV)
    syntheticPeople.createOrReplaceTempView("people")
    //syntheticPeople.show(20)

    val eligiblePopulation = sparkSession.sql("""SELECT h.sp_id, geom, p.sex, p.age FROM households h INNER JOIN people p ON h.sp_id = p.sp_hh_id WHERE h.hh_income - 30350 + (10800*h.hh_size) < 0 AND p.sex = 2 AND p.age >= 40 """)
    eligiblePopulation.createOrReplaceTempView("eligible_women")
    // eligiblePopulation.show(20)


    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/home/david/SAGE/clients")
    var clientsDF = Adapter.toDf(spatialRDD,sparkSession)
    clientsDF.createOrReplaceTempView("load")
    //clientsDF.show(10)
    clientsDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as client_id, _c4 as type, _c7 as race FROM load """)
    clientsDF.createOrReplaceTempView("clients")


    val clients_tracts_join = sparkSession.sql(""" SELECT t.tract_id, p.sp_id, ST_Distance(p.geom, t.geom) as distance, 1 as people FROM eligible_women p cross join tracts t ORDER BY 1,2 """)
    // clients_tracts_join.show(10)
    // http://xinhstechblog.blogspot.com/2016/04/spark-window-functions-for-dataframes.html
    val distance_ordered_clients = Window.partitionBy("tract_id").orderBy("distance").rowsBetween(Long.MinValue, 0)
    val ordered_clients = clients_tracts_join.withColumn("number_of_people", sum(clients_tracts_join("people")).over(distance_ordered_clients))
    ordered_clients.createOrReplaceTempView("ordered_clients")
    //ordered_clients.show(30)

    //tractsDF.show(20)



    //, count(c.client_id) as number_of_clients
    //ST_Distance(f.geom, c.geom) as distance_calc, f.distance

    val filterJoins = sparkSession.sql(
      """
        |SELECT tract_id, geom, number_of_clients, number_of_people, number_of_clients/cast(number_of_people as float) as ratio
        |FROM
        |(
          |SELECT f.tract_id, f.geom, f.number_of_people, count(c.client_id) as number_of_clients
          |FROM
            |	(
            |	SELECT DISTINCT c.tract_id, c.number_of_people, c.distance, t.geom
            |	FROM ordered_clients c
            |	INNER JOIN tracts t on (t.tract_id = c.tract_id)
            |	WHERE number_of_people = 100
            |	) f CROSS JOIN clients c
          | WHERE ST_Distance(f.geom, c.geom) < f.distance
          | GROUP BY f.tract_id, f.geom, f.number_of_people
        | ) results
      """.stripMargin)
    filterJoins.show(25)




    sparkSession.sparkContext.stop()
  }

}
