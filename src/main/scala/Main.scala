
import java.util.{Calendar, Date}

import scala.util.Properties

import com.datastax.spark.connector._

object Main {
  def main(args: Array[String]): Unit = {

    val context = Factory.createContext()
    val hdfsMaster = Properties.envOrElse("BATCH_HADOOP_NAMENODE", "hdfs://namenode:8020")
    val data = context.textFile(hdfsMaster + "/user/spark/vehiclelocation/*", 2)
    val serialized = data.map(value => Vehicle.create(value.toString))
      .filter(v => isToday(v.time))

    val traces = serialized.sortBy(v => (v.id, v.time))
      .groupBy(v => (v.id, v.company_id))

    val distances = traces.mapValues(points => {
      var dist = 0.0
      var last = points.head
      points.foreach(point => {
        dist += CalculateDistance(point.latitude, point.longitude, last.latitude, last.longitude)
        last = point
      })
      dist
    })

    val keyspace = Properties.envOrElse("BATCH_CASSANDRA_KEYSPACE", "streaming")
    val table = Properties.envOrElse("BATCH_CASSANDRA_DISTANCE_TABLE", "vehicles_covered_distance")
    val time = today()

    distances.map(item => VehicleCoveredDistance(item._1._1, item._1._2, item._2, time))
      .saveToCassandra(keyspace, table)
  }

  def today(): Date = {
    val now = Calendar.getInstance()
    now.set(Calendar.MINUTE, 0)
    now.set(Calendar.SECOND, 0)
    now.set(Calendar.MILLISECOND, 0)
    now.set(Calendar.HOUR_OF_DAY, 0)
    now.getTime()
  }

  def isToday(date: Date): Boolean = {
    date.after(today())
  }

  def CalculateDistance(lat: Double, lng: Double, lat2: Double, lng2: Double): Double = {
    val radius = 3958.75
    val dLat = Math.toRadians(lat2-lat)
    val dLon = Math.toRadians(lng2-lng)
    val a = Math.sin(dLat/2) * Math.sin(dLat/2) +
      Math.cos(Math.toRadians(lat)) * Math.cos(Math.toRadians(lat2)) *
        Math.sin(dLon/2) * Math.sin(dLon/2)
    val c = 2 * Math.asin(Math.sqrt(a))
    radius * c
  }
}
