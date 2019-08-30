import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.KeyValueTextInputFormat

import scala.util.Properties

object Main {
  def main(args: Array[String]): Unit = {

    val context = Factory.createContext()
    val hdfsMaster = Properties.envOrElse("BATCH_HADOOP_NAMENODE", "hdfs://namenode:8020")
    val data = context.hadoopFile(hdfsMaster + "/vehiclelocation/", classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text], 2)
    val serialized = data.mapValues(v => Vehicle.create(v.toString))
    val count = serialized.countByKey()
    count.foreach(println(_))
    context.stop()
  }
}
