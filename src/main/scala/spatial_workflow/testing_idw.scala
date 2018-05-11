import com.vividsolutions.jts.geom.Geometry
import geotrellis.vector.io.json._
import geotrellis.vector.{Point, PointFeature}
import geotrellis.raster._
import spray.json._
import geotrellis.vector.io._
import geotrellis.raster.io.geotiff._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

//val fc = Polygon( (10.0, 10.0), (12.1, 15.1), (13.2, 12)).toGeoJSON
object testing_idw{

  def main(): Unit ={

    //val p1 = Point((2.0,4.1))
    val p2 = Point((1.3,4.1))

    val j = geotrellis.vector.io.json.GeoJson
    val p4 = PointFeature(Point(2.1,3.6),3)
    val p5 = PointFeature(Point(4.1,1.6),5)

    val path: String = "/home/david/SAGE/mn_races.tif"
    val geoTiff = SinglebandGeoTiff(path)
    println(geoTiff.rasterExtent)
    println(geoTiff.extent, geoTiff.crs)

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
    spatialRDD.boundaryEnvelope.getMaxX
    // ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, nycArealandmarkShapefileLocation)
    var tractsDF = Adapter.toDf(spatialRDD,sparkSession)

    tractsDF.createOrReplaceTempView("load")
    tractsDF = sparkSession.sql("""SELECT ST_Centroid(ST_GeomFromWKT(rddshape)) as geom, _c4 as tract_id, _c6 as tract_name FROM load Limit 20""")
    tractsDF.createOrReplaceTempView("tracts")
    //tractsDF = sparkSession.sql("""SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:5070' ) as geom  FROM tracts""")
    val tractsRDD = tractsDF.rdd
    val boundary = sparkSession.sql("""SELECT ST_Envelope_Aggr(ST_GeomFromWKT(rddshape)) FROM load""")
    val theboundary = boundary.take(1)

    //val b = boundary.rdd

    val datapoints = tractsDF.select("geom","tract_id")
    tractsDF.toJSON.show(5)

    spatialRDD.boundaryEnvelope.toString

    val thing = datapoints.take(1)
    thing.toString()
    //val p = geotrellis.vector.io.readFeatureJson(thing(0)(0).toString() )
    val x: Double = thing(0)(0).toString().split(" ")(1).replace("(","").toDouble
    val y: Double = thing(0)(0).toString().split(" ")(2).replace(")","").toDouble
    val z = thing(0)(1).toString().toDouble

    val p1 = Point((x,y))
    p1.toGeoJson()

    // p1.jtsGeom.toJson
    val newpoint = PointFeature(Point(x,y),z)
    val t = thing.getClass()
    val r = geotrellis.vector.PointFeature(p1, z)


      //.apply(p1,3)
    //val p = geotrellis.vector.io.readFeatureJson(p1.toGeoJson() )

    //thing(0)(0).split(" ")(1)

    thing(0)(0).toString()
  }
}

