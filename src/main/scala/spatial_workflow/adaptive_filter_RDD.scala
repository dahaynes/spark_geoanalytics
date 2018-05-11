package spatial_workflow

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import geotrellis.vector.{Point, PointFeature}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.geosparksql.expressions.ST_GeomFromWKT
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType}
import org.datasyslab.geospark.formatMapper.shapefileParser.{ShapefileRDD, ShapefileReader}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{LineStringRDD, PointRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.spatialPartitioning
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.apache.spark.sql.SQLContext

import scala.io.StdIn
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import geotrellis.vector.io._
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery}
import org.datasyslab.geospark.spatialPartitioning.KDBTree



object adpative_filters_RDD {

  def main(args: Array[String]): Unit = {


    val sparkSession:SparkSession = SparkSession.builder().
      config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[2]").appName("AdaptiveFilter").getOrCreate()

    //adds Spatial Functions (ST_*)
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

    //val sc = new SparkContext(sparkSession:sparkContext)
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)




    /*
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/home/david/SAGE/mn_tracts")
    // ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, nycArealandmarkShapefileLocation)
    var tractsDF = Adapter.toDf(spatialRDD,sparkSession)
    tractsDF.createOrReplaceTempView("load")
    tractsDF = sparkSession.sql("""SELECT ST_Centroid(ST_GeomFromWKT(rddshape)) as geom, _c4 as tract_id, _c6 as tract_name FROM load""")
    tractsDF.createOrReplaceTempView("tracts")
    */

    val splitter = FileDataSplitter.WKT
    val theGridRDD = new PointRDD(sparkSession.sparkContext, "/home/david/SAGE/grid/grid.csv", 1, splitter, false)
    //theGridRDD.spatialPartitionedRDD
    // This doesn't work because it has to be converted, unless the x,y columns were split up.
    // http://datasystemslab.github.io/GeoSpark/api/sql/GeoSparkSQL-Constructor/
    // val point2 = theGridRDD.getRawSpatialRDD.rdd.first()


    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/home/david/SAGE/grid")
    //objectRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String]
    val point = spatialRDD.rawSpatialRDD.rdd.first()

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
    val basePopulationRDD = eligiblePopulation.rdd


    // eligiblePopulation.show(20)


    var clientsRDD = new SpatialRDD[Geometry]
    clientsRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/home/david/SAGE/clients")
    // var clientsDF = Adapter.toDf(spatialRDD,sparkSession)
    //val items = theGridRDD.getRawSpatialRDD.map(p => KNNQuery.SpatialKnnQuery(clientsRDD,p,5,false))


    /*
    var clientsRDD = spatialRDD.indexedRDD
    val clientsRDDAttributes = spatialRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])
    val items = theGridRDD.getRawSpatialRDD.map[List[Geometry]](p => KNNQuery.SpatialKnnQuery(clientsRDD,p,5,false))

    def nearest(p: Geometry) = KNNQuery.SpatialKnnQuery(clientsRDD,p,5,false)

    val items = theGridRDD.getRawSpatialRDD.foreach{p => KNNQuery.SpatialKnnQuery(clientsRDD,p,5,false) }
    spatialRDD.rawSpatialRDD.foreach(p => KNNQuery.SpatialKnnQuery(clientsRDD,p,5,false))
    val l1 = spatialRDD.rawSpatialRDD.toList()
    */




    /*
    //val result = JoinQuery.SpatialJoinQuery(theGridRDD.spatialPartitionedRDD,clientsRDD, true,false)
    //val resultDF = Adapter.toDf(result, sparkSession)
    val clientsRDD = spatialRDD.spatialPartitionedRDD(GridType.QUADTREE)
    //theGridRDD.spatialPartitioning()
    // clientsRDD.spatialPartitioning(GridType.KDBTREE  )
    theGridRDD.spatialPartitionedRDD(GridType.QUADTREE)

    val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
    val usingIndex = false

    val result = JoinQuery.DistanceJoinQueryFlat(clientsRDD, theGridRDD, usingIndex, considerBoundaryIntersection)
    */



    //val result = KNNQuery.SpatialKnnQuery(clientsRDD,point,15,false)

  /*
    for g in grid:
      syntheticPersonID = KNN(p, syntheticPeople, 100)

    for g in grid:
      numclients = DistanceJoin( clients, g, ST_Distance(g, synethic_id))
      */

    /*
    clientsDF.createOrReplaceTempView("load")
    //clientsDF.show(10)
    clientsDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as client_id, _c4 as type, _c7 as race FROM load """)
    clientsDF.createOrReplaceTempView("clients")
    */


    /*
    val clients_tracts_join = sparkSession.sql(""" SELECT t.tract_id, p.sp_id, ST_Distance(p.geom, t.geom) as distance, 1 as people FROM eligible_women p cross join tracts t ORDER BY 1,2 """)
    // clients_tracts_join.show(10)
    // http://xinhstechblog.blogspot.com/2016/04/spark-window-functions-for-dataframes.html
    val distance_ordered_clients = Window.partitionBy("tract_id").orderBy("distance").rowsBetween(Long.MinValue, 0)
    val ordered_clients = clients_tracts_join.withColumn("number_of_people", sum(clients_tracts_join("people")).over(distance_ordered_clients))
    ordered_clients.createOrReplaceTempView("ordered_clients")
    */
    //ordered_clients.show(30)

    //tractsDF.show(20)



    //, count(c.client_id) as number_of_clients
    //ST_Distance(f.geom, c.geom) as distance_calc, f.distance

    /*
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
    filterJoins.show(200)


    */

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
