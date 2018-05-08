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



object adpative_filters {

  def main(args: Array[String]): Unit = {


    /*
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Tiler").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")//.set("spark.driver.memory", "2g").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    //more garabage
    var sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName).
      master("local[*]").appName("GeoSparkSQL-demo").getOrCreate()
    */

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
    tractsDF = sparkSession.sql("""SELECT ST_Centroid(ST_GeomFromWKT(rddshape)) as geom, _c4 as geoid, _c6 as tract_id FROM load """)
    //tractsDF.show(10)

    // Synthetic Households
    val syntheticPopulationCSV = "/home/david/SAGE/households/households.csv"
    var syntheticPopulation = sparkSession.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPopulationCSV)
    syntheticPopulation.createOrReplaceTempView("load")
    // syntheticPopulation.show(20)
    syntheticPopulation =sparkSession.sql(""" SELECT ST_Point( cast(latitude as Decimal(24,20)), cast(longitude as Decimal(24,20)) ) as geom, sp_id, hh_income, hh_size FROM load """)
    syntheticPopulation.show(20)
    syntheticPopulation.createOrReplaceTempView("households")


    val syntheticPeopleCSV = "/home/david/SAGE/households/people.csv"
    var syntheticPeople = sparkSession.read.format("csv").option("delimiter",",").option("header","true").load(syntheticPeopleCSV)
    syntheticPeople.createOrReplaceTempView("people")
    syntheticPeople.show(20)

    val eligiblePopulation = sparkSession.sql("""SELECT h.sp_id, geom, p.sex, p.age FROM households h INNER JOIN people p ON h.sp_id = p.sp_hh_id WHERE h.hh_income - 30350 + (10800*h.hh_size) < 0 AND p.sex = 2 AND p.age > 40 """)
    eligiblePopulation.createOrReplaceTempView("eligible_women")
    eligiblePopulation.show(20)



    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/home/david/SAGE/clients")
    var clientsDF = Adapter.toDf(spatialRDD,sparkSession)
    clientsDF.createOrReplaceTempView("load")
    //clientsDF.show(10)
    clientsDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as client_id, _c4 as type, _c7 as race FROM load """.stripMargin)
    clientsDF.createOrReplaceTempView("clients")


    // clientsDF = sparkSession.sql(""" SELECT c1.client_id, ST_Distance(c1.geom, c2.geom) FROM clients c1 cross join clients c2 ORDER BY 1,2 LIMIT 50 """)
    // clientsDF.show(10)
    sparkSession.sparkContext.stop()

  }

}
