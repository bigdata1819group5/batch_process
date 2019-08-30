
import java.util.{Date, Calendar}
import java.text.SimpleDateFormat

object Vehicle {
  val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def create(args: String): Vehicle = {
    val splited = args.stripPrefix("\"").stripSuffix("\"").split(",")
    Vehicle(splited(0), splited(1), timeFormat.parse(splited(2)), splited(3).toDouble, splited(4).toDouble)
  }

  def convertToTimeID(dateString: String): Date = {
    val time = timeFormat.parse(dateString)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.getTime()
  }
}


case class Vehicle(
  id: String,
  company_id: String,
  time: Date,
  latitude: Double,
  longitude: Double
)