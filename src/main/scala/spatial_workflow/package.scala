package object spatial_workflow {

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.{ShapefileReader, ShapefileRDD}
//import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, LineStringRDD, SpatialRDD}
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.apache.spark.sql.SQLContext
import scala.io.StdIn
import java.io.File

  object spatial_adaptive_filters{

    def LoadShapefile(theDirectory: String): Unit ={

      /*
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      var pointsDF = Adapter.toDf(spatialRDD,sparkSession)
      pointsDF.createOrReplaceTempView("random_points")
      var pointsDF = sparkSession.sql("SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id FROM random_points")
      pointsDF.createOrReplaceTempView("random_points")
      */


    }

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

      //val sc = new SparkContext(sparkSession:sparkContext)

      val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)

      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/home/david/git/GIS5577_lab4/points")


      // hapefileReader.readToPolygonRDD(sparkSession.sparkContext, nycArealandmarkShapefileLocation)
      var polygonDF = Adapter.toDf(spatialRDD,sparkSession)

      polygonDF.createOrReplaceTempView("load")
      polygonDF.show(10)
    }
  }


//GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)




/*var polygonRDD = new SpatialRDD[Geometry]
val shapefileInputLocation: String = "/home/david/git/GIS5577_lab4/"
spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
var polygonDF = Adapter.toDf(spatialRDD,sparkSession)
polygonDF.createOrReplaceTempView("load")
var polygonDF = sparkSession.sql("SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id, _c2 as gisjoin, _c3 as state_name, _c4 as FIPS, _c5 as county_name FROM load")
polygonDF.createOrReplaceTempView("mn_census_tracts_2010")

/*
rddshape|  _c1|           _c2|      _c3|_c4|             _c5|_c6|   _c7| _c8| _c9|_c10|
  +--------------------+-----+--------------+---------+---+----------------+---+------+----+----+----+
|POLYGON ((-92.984...|66566|G2701230040200|Minnesota| 27|   Ramsey County|123| 40200|1742| 908| 834|
*/
polygonDF.show(3)

var queryDF = sparkSession.sql("SELECT * FROM random_points, mn_census_tracts_2010 WHERE ST_Intersects(random_points.geom, mn_census_tracts_2010.geom)")
var queryDF = sparkSession.sql(" SELECT mn_census_tracts_2010.county_name, count(random_points.id) FROM random_points, mn_census_tracts_2010  WHERE ST_Intersects(random_points.geom, mn_census_tracts_2010.geom) GROUP BY mn_census_tracts_2010.county_name ")
queryDF.show(4)
sparkSession.sparkContext.stop()*/






}
