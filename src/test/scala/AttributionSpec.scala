import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfter
import AttributionReport._

class AttributionSpec extends FunSpec with BeforeAndAfter {
  describe("String to Event/Impression") {
    it("should turn a correctly formed csv string into an Event") {
      val event = AttributionReport.strToEvent("1450583934,22efbdaf-efc6-4a65-bb50-2d33121b16d4,0,7fe40811-7d3b-42b9-83ff-58eee06859d9,click")
      assert(event === Event(1450583934, "22efbdaf-efc6-4a65-bb50-2d33121b16d4", 0, "7fe40811-7d3b-42b9-83ff-58eee06859d9", "click"))
    }
    it("should turn a correctly formed csv string into an Impression") {
      val impression = AttributionReport.strToImpression("1450585657,1,2,0adb6ada-6978-4d95-801f-b03289a6c12c")
      assert(impression === Impression(1450585657, 1, 2, "0adb6ada-6978-4d95-801f-b03289a6c12c"))
    }
  }

  describe("Deduplication tests") {
    it("should deduplicate when following events are within the threshold") {
      val threshold = 60
      val events = Seq(
        Event(5, "advertiser-1", 0, "user-1", "click"),
        Event(10, "advertiser-1", 0, "user-1", "click"),
        Event(15, "advertiser-1", 0, "user-1", "click")
      )
      val result = AttributionReport.deduplicate(events, threshold)
      assert(result.size === 1)
      assert(result.head === events(0))
    }
    it("should deduplicate only for the threshold window") {
      val threshold = 7
      val events = Seq(
        Event(5, "advertiser-1", 0, "user-1", "click"),
        Event(10, "advertiser-1", 0, "user-1", "click"),
        Event(15, "advertiser-1", 0, "user-1", "click")
      )
      val result = AttributionReport.deduplicate(events, threshold)
      assert(result.size === 2)
      assert(result(0) === events(0))
      assert(result(1) === events(2))
    }
    it("should only deduplicate for alike event types") {
      val threshold = 10
      val events = Seq(
        Event(0, "advertiser-1", 0, "user-1", "click"),
        Event(5, "advertiser-1", 0, "user-1", "click"),
        Event(5, "advertiser-1", 0, "user-1", "purchase"),
        Event(11, "advertiser-1", 0, "user-1", "click"),
        Event(11, "advertiser-1", 0, "user-1", "purchase")
      )
      val result = AttributionReport.deduplicate(events, threshold)
      assert(result === Seq (
        events(0),
        events(2),
        events(3)
      ))
    }
  }

  var sc: SparkContext = _

  describe("Attribution report") {

    before {
      val conf = new SparkConf().setAppName("Test Attribution Report")
        .setMaster("local[2]")
      sc = new SparkContext(conf)
    }

    it ("one each (trivial)") {
      val impressions = Seq(
        Impression(timestamp = 0, advertiserId = 1, creativeId = 0, userId = "1")
      )
      val events = Seq (
        Event(timestamp = 10, eventId = "event-1", advertiserId = 1, userId = "1", "click")
      )
      val (e, u) = AttributionReport.genAttributionReport(sc.parallelize(events), sc.parallelize(impressions))
      val eventCounts = e.collect
      val userCounts = u.collect

      assert(eventCounts === Seq(((1, "click"), 1)))
      assert(userCounts === Seq(((1, "click"), 1)))
    }

    it ("generalized test") {
      val impressions = Seq(
        Impression(timestamp = 0, advertiserId = 1, creativeId = 0, userId = "1"),
        Impression(timestamp = 0, advertiserId = 1, creativeId = 0, userId = "2"),
        Impression(timestamp = 0, advertiserId = 2, creativeId = 0, userId = "1")
      )
      val events = Seq (
        Event(timestamp = 10,    eventId = "event-1", advertiserId = 1, userId = "1", "click"),
        Event(timestamp = 10,    eventId = "event-2", advertiserId = 1, userId = "2", "click"),
        Event(timestamp = 15,    eventId = "event-3", advertiserId = 1, userId = "2", "click"), //should be filtered out
        Event(timestamp = 16,    eventId = "event-4", advertiserId = 1, userId = "2", "visit"),
        Event(timestamp = 17,    eventId = "event-5", advertiserId = 1, userId = "2", "purchase"),
        Event(timestamp = 70000, eventId = "event-6", advertiserId = 1, userId = "1", "click")
      )

      val (e, u) = AttributionReport.genAttributionReport(sc.parallelize(events), sc.parallelize(impressions))
      val eventCountSet = e.collect.toSet
      val userCountSet = u.collect.toSet

      assert(eventCountSet === Set(
        ((1, "click"), 3),
        ((1, "purchase"), 1),
        ((1, "visit"), 1)
      ))

      assert(userCountSet === Set(
        ((1, "click"), 2),
        ((1, "purchase"), 1),
        ((1, "visit"), 1)
      ))
    }

    after {
      sc.stop
    }
  }
}
