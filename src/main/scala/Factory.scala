import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



import scala.util.Properties

object Factory {
  def createContext(): SparkContext = {
    val conf = new SparkConf()
      .setAppName(Properties.envOrElse("BATCH_APP_NAME", "DigestData"))
      .set("spark.cassandra.connection.host", Properties.envOrElse("BATCH_CASSANDRA_HOST", "cassandra:7000"))

    new SparkContext(conf)
  }
}
