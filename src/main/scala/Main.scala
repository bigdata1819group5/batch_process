import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.KeyValueTextInputFormat

import scala.util.Properties

object Main {
  def main(args: Array[String]): Unit = {

    val context = Factory.createContext()
    val hdfsMaster = Properties.envOrElse("BATCH_HADOOP_NAMENODE", "hdfs://namenode:8020")
    val data = context.textFile(hdfsMaster + "/user/spark/vehiclelocation/*", 2)
    val serialized = data.map(value => Vehicle.create(value.toString))
    val paired = serialized.map(v => (v.id, v))
    val count = paired.countByKey()
    count.foreach(println(_))
    context.stop()
  }
}
