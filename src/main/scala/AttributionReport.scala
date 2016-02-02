import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{HashMap, HashSet}

object AttributionReport {

  case class Event(timestamp: Int,
                   eventId: String,
                   advertiserId: Int,
                   userId: String,
                   eventType: String)

  case class Impression(timestamp: Int,
                        advertiserId: Int,
                        creativeId: Int,
                        userId: String)

  /**
   * Deserializes a string to an Event
   * @param input a comma separated string representing a single event
   * @return the corresponding event
   */
  def strToEvent(input: String): Event = {
    val s = input.split(",").map(_.trim)
    Event(s(0).toInt, s(1), s(2).toInt, s(3), s(4))
  }

  /**
   * Deserializes a string to an Impression
   * @param input a comma separated string representing a single impression
   * @return the corresponding impression
   */
  def strToImpression(input: String): Impression = {
    val s = input.split(",").map(_.trim)
    Impression(s(0).toInt, s(1).toInt, s(2).toInt, s(3))
  }

  /**
   * Deduplication is based only on timestamp and event type. Other values such as advertiserId and userId are not
   * considered as it is assumed that the Event sequence has already been grouped
   *
   * @param events a list of events
   * @param threshold events seen within {threshold} milliseconds from the previous event of this type will be discarded
   * @return the list of events, with duplicates pruned out
   */
  def deduplicate(events: Seq[Event], threshold: Int) = {

    val (_, l) = events.foldLeft((HashMap.empty[String, Int], Seq.empty[Event])) {
      case ((hm, iterable), event) => {
        hm.get(event.eventType) match {
          case None =>
            (hm += (event.eventType -> event.timestamp), iterable :+ event)
          case Some(previousEntry) => {
            if (event.timestamp - previousEntry < threshold) {
              (hm, iterable)
            } else {
              (hm += (event.eventType -> event.timestamp), iterable :+ event)
            }
          }
        }
      }
    }
    l
  }

  /**
   * Generates an attribution report given RDDs for events and impressions
   *
   * @param events potential attributable events
   * @param impressions the impressions to associate with events
   * @param deduplicationThreshold events seen within {threshold} milliseconds from the previous event of the same type
   *                               will be discarded
   * @return a Tuple2 of RDDs. The first corresponds to the count of events the second is the count of users
   */
  def genAttributionReport (events: RDD[Event], impressions: RDD[Impression], deduplicationThreshold:Int = 60000) = {
    //create kv pairs keyed by advertiserId, userId
    val eventsKv = events.map(event => ((event.advertiserId, event.userId), event))

    //create kv pairs keyed by advertiserId, userId
    val impressionsKv = impressions.map(imp => ((imp.advertiserId, imp.userId), imp))

    //get the earliest timestamp for each impression grouped by advertiserId, userId
    val earliestImps = impressionsKv.reduceByKey((a, b) => {
      if (a.timestamp < b.timestamp) a
      else b
    })

    //foreach group of advertiserId, userId pair in events find find out if there is an earlier impression
    val attributed = eventsKv.groupByKey
      .join(earliestImps)
      .flatMap({ case ((advId, usrId), (events, imp)) => {
        deduplicate(
          events.filter(ev => ev.timestamp > imp.timestamp).toSeq,
          deduplicationThreshold
        )
      }})
      .cache

    val countOfEvents = attributed.map(event => ((event.advertiserId, event.eventType), 1))
      .reduceByKey((a, b) => a + b)

    val countOfUsers = attributed.map(event => ((event.advertiserId, event.eventType), event.userId))
      .aggregateByKey(HashSet.empty[String]) (
        (a, userId) => a += userId,
        (a, b) => a ++= b
      ).map {case (a, set) => (a, set.size)}

    (countOfEvents, countOfUsers)
  }

  // writes a each string in {rows} as a line to {filename}
  def writeToLocal(rows: Seq[String], filename: String): Unit = {
    import java.io.{File, PrintWriter}
    val writer = new PrintWriter(new File(filename))
    rows.foreach(row => writer.println(row))
    writer.close()
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Attribution Report")
                              .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val eventsCsv = sc.textFile("events.csv")
    val impressionsCsv = sc.textFile("impressions.csv")

    val (countOfEvents, countOfUsers) = genAttributionReport(eventsCsv.map(strToEvent), impressionsCsv.map(strToImpression))

    //converts a ((Int, String), Int) to a csv string
    val countToString: (((Int, String), Int)) => String = {
      case ((advertiserId: Int, eventType: String), count: Int) =>
        Seq(advertiserId, eventType, count).mkString(",")
    }

    val eventsReportStr = countOfEvents.map(countToString).saveAsTextFile("count_of_events")
    val usersReportStr = countOfUsers.map(countToString).saveAsTextFile("count_of_users")

    sc.stop
  }
}
