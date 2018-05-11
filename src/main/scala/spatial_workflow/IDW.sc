import com.vividsolutions.jts.geom.Geometry
import geotrellis.vector.{Point, PointFeature}
import geotrellis.raster._
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
val p1 = Point((2.0,4.1))
val p2 = Point((1.3,4.1))

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
// ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, nycArealandmarkShapefileLocation)
var tractsDF = Adapter.toDf(spatialRDD,sparkSession)
tractsDF.createOrReplaceTempView("load")
tractsDF = sparkSession.sql("""SELECT ST_Centroid(ST_GeomFromWKT(rddshape)) as geom, _c4 as tract_id, _c6 as tract_name FROM load Limit 20""")
tractsDF.createOrReplaceTempView("tracts")

val datapoints = tractsDF.select("geom","tract_id")



val interpolationOptions = geotrellis.raster.interpolation.InverseDistanceWeighted.Options(
  radiusX = 13.1,
  radiusY = 15.4,
  rotation = 0.0,
  weightingPower = 2.0,
  smoothingFactor = 1.0,
  equalWeightRadius = 0.0,
  cellType = geoTiff.cellType
  )

val points = Traversable(p4,p5)
val dataset = geotrellis.raster.interpolation.InverseDistanceWeighted(points,geoTiff.rasterExtent, interpolationOptions)
// geotrellis.raster.io.geotiff.writer.GeoTiffWriter(dataset, "/home/david/SAGE/idw.tif")



//val theWriter = geotrellis.raster.io.geotiff.writer.GeoTiffWriter
//theWriter.write(dataset, "/home/david/SAGE/idw.tif")


