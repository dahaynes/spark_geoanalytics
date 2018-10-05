package spatial_workflow

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

object uninsured_income {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //import sparkSession.implicits._

    val sparkSession: SparkSession = SparkSession.builder().
      config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[4]").appName("AdaptiveFilter").getOrCreate() //.set("spark.driver.memory", "2g").set("spark.executor.memory", "1g")

    //adds Spatial Functions (ST_*)
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    sparkSession.udf.register("ST_SaveAsWKT", (geometry: Geometry) => (geometry.toText))

    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    var spatialRDD = new SpatialRDD[Geometry]

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, "/media/sf_data/sage_data/regular_grid/")
    var gridDF = Adapter.toDf(spatialRDD, sparkSession)
    gridDF.createOrReplaceTempView("load")
    //gridDF.show(4)
    gridDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id FROM load LIMIT 20""")
    gridDF.createOrReplaceTempView("grid")


    // Synthetic Households
    val syntheticPopulationCSV = "/media/sf_data/sage_data/synthetic_population/2010_ver1_27_synth_households.txt"
    var syntheticPopulation = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(syntheticPopulationCSV)
    syntheticPopulation.createOrReplaceTempView("load")
    // syntheticPopulation.show(20)
    syntheticPopulation = sparkSession.sql(""" SELECT ST_Point( cast(longitude as Decimal(24,20)), cast(latitude as Decimal(24,20)) ) as geom, sp_id, hh_income, hh_size FROM load """)
    //syntheticPopulation.show(20)
    syntheticPopulation.createOrReplaceTempView("households")

    val syntheticPeopleCSV = "/media/sf_data/sage_data/synthetic_population/2010_ver1_27_synth_people.txt"
    var syntheticPeople = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(syntheticPeopleCSV)
    syntheticPeople.createOrReplaceTempView("people")
    //syntheticPeople.show(20)

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
    //spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext,"/home/david/SAGE/clients")
    var clientsDF = Adapter.toDf(spatialRDD, sparkSession)
    clientsDF.createOrReplaceTempView("load")
    //clientsDF.show(10)
    clientsDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c1 as id FROM load """)
    clientsDF.createOrReplaceTempView("all_clients")

    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, "/media/sf_data/sage_data/mn_boundary")
    var boundaryDF = Adapter.toDf(spatialRDD, sparkSession)
    boundaryDF.createOrReplaceTempView("load")
    //boundaryDF.show(10)
    boundaryDF = sparkSession.sql(""" SELECT ST_GeomFromWKT(rddshape) as geom, _c6 as name FROM load WHERE _c6 = 'Minnesota' """)
    boundaryDF.createOrReplaceTempView("state_boundary")

    var stateClientsDF = sparkSession.sql(""" SELECT ST_Transform(c.geom, 'epsg:4326', 'epsg:26915') as geom, c.id FROM all_clients c INNER JOIN state_boundary b on ST_Intersects(c.geom, b.geom) """)
    stateClientsDF.createOrReplaceTempView("clients")

    var numClientsDF = sparkSession.sql("""SELECT count(id) FROM clients""")
    //numClientsDF.show(2)
    //println(numClientsDF.count())
  }
}
