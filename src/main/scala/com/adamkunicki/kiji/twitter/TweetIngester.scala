package com.adamkunicki.kiji.twitter

import org.slf4j.LoggerFactory
import akka.actor.{ActorRef, Props, Actor, ActorSystem}
import com.adamkunicki.akka._
import com.adamkunicki.avro._
import akka.routing.RoundRobinRouter
import org.kiji.schema._
import java.io.IOException
import twitter4j._
import scala.collection.JavaConverters._
import org.kiji.schema.util.ToJson
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import scala.Long

object TweetIngester extends App {

  lazy val log = LoggerFactory.getLogger(getClass.getName)

  case class Config(
                     kijiUriString: String = "kiji://.env/default",
                     tweetTableName: String = "tweet",
                     colF: String = "info",
                     colQ: String = "tweet",
                     esIndexName: String = "twitter",
                     esDocType: String = "tweet",
                     esHostAddress: String = "localhost",
                     usersToFollow: Seq[Long] = Seq(),
                     numWriters: Int = 4,
                     numIndexers: Int = 4
                     )

  val parser = new scopt.OptionParser[Config]("TweetIngester") {
    head("TweetIngester", "0.0.1")
    opt[String]("kiji") action {
      (x, c) =>
        c.copy(kijiUriString = x)
    } text """Kiji Instance URI. Default: "kiji://.env/default""""
    opt[String]('t', "table") action {
      (x, c) =>
        c.copy(tweetTableName = x)
    } text """Kiji Table Name. Default: "tweet""""
    opt[String]('f', "family") action {
      (x, c) =>
        c.copy(colF = x)
    } text """Column Family. Default: "info""""
    opt[String]('q', "qualifier") action {
      (x, c) =>
        c.copy(colQ = x)
    } text """Column Qualifier. Default: "tweet""""
    opt[String]('i', "index") action {
      (x, c) =>
        c.copy(esIndexName = x)
    } text """Elastic Search index name. Default: "twitter""""
    opt[String]("doctype") action {
      (x, c) =>
        c.copy(esDocType = x)
    } text """Elastic Search document type. Default: "tweet""""
    opt[String]("eshost") action {
      (x, c) =>
        c.copy(esHostAddress = x)
    } text """Elastic Search host. Default: "localhost""""
    opt[Seq[Long]]('u', "users") action {
      (x, c) =>
        c.copy(usersToFollow = x)
    } text "User Ids to subscribe to."
    opt[Int]("nw") action {
      (x, c) =>
        c.copy(numWriters = x)
    } text "Number of write threads. Default: 4"
    opt[Int]("ni") action {
      (x, c) =>
        c.copy(numIndexers = x)
    } text "Number of index threads. Default: 4"
  }

  val parsed = parser.parse(args, Config())
  if (parsed.isEmpty) { System.exit(1) }
  val config = parsed.get

  val kijiUri = KijiURI.newBuilder(config.kijiUriString).build()

  // "amandabynes",  "rihanna", "katyperry", "jtimberlake", "ActuallyNPH", "wibidata"
  val usersToFollow = Array[Long](243442402, 79293791, 21447363, 26565946, 90420314, 377018652)

  // Try to open a Kiji instance.
  val kiji = Kiji.Factory.open(kijiUri)
  val tablePool = KijiTablePool.newBuilder(kiji)
    .withIdleTimeout(10)
    .withIdlePollPeriod(1)
    .build()

  val system = ActorSystem("TweetIngestSystem")
  val listener = system.actorOf(Props[Listener], name = "listener")
  val master = system.actorOf(Props(new Master(config.numWriters, config.numIndexers, listener)))

  master ! Start

  class KijiTweetWriter(tablePool: KijiTablePool, config: Config) extends Actor {

    var tweetCount: Int = _
    val table = tablePool.get(config.tweetTableName)
    val writer = table.openTableWriter()

    def receive = {
      case TweetReceived(status) =>
        try {
          makeTweet(status) match {
            case Some(tweet) =>
              writer.put(table.getEntityId(status.getUser.getId: java.lang.Long), config.colF, config.colQ, tweet.getCreatedAt, tweet)
              tweetCount += 1
              sender ! AvroTweet(tweet)
              if (tweetCount % 500 == 0) {
                writer.flush()
                sender ! Progress(500)
              }
            case None => log.warn("Unable to parse Tweet")
          }
        } catch {
          case e: IOException =>
            log.error("Error while writing tweet", e)
        }
    }

    override def postStop(): Unit = {
      writer.close()
      table.release()
    }
  }

  class Indexer extends Actor {
    val client = new TransportClient()
      .addTransportAddress(new InetSocketTransportAddress(esHostAddress, 9300))
    val bulkRequest = client.prepareBulk()

    var tweetCount: Int = _
    var errorCount: Int = _

    def receive = {
      case AvroTweet(tweet) =>
        bulkRequest.add(client.prepareIndex(esIndexName, esDocType, tweet.getId.toString)
          .setSource(ToJson.toJsonString(tweet, tweet.getSchema)))
        tweetCount += 1

        if (tweetCount % 500 == 0) {
          val response = bulkRequest.execute().actionGet()
          if (response.hasFailures) {
            log.error(response.buildFailureMessage())
            errorCount += 1
          }
        }
      case _ => log.warn("Indexer received unsupported message type.")
    }

    override def postStop(): Unit = {
      client.close()
    }
  }

  class Listener extends Actor {
    def receive = {
      case Finish =>
        log.info("Shutting down.")
        context.system.shutdown()
        tablePool.close()
        kiji.release()
    }
  }

  class Master(numWriters: Int, numIndexers: Int, listener: ActorRef) extends Actor {
    val writerRouter = context.actorOf(Props[KijiTweetWriter].withRouter(RoundRobinRouter(numWriters)), name = "writerRouter")
    val indexerRouter = context.actorOf(Props[Indexer].withRouter(RoundRobinRouter(numIndexers)), name = "indexerRouter")

    var totalTweets: Int = _

    def receive = {
      case Start =>
        // Connect to the Twitter API and start sending out tweets
        val tweetStream = new TwitterStreamFactory().getInstance()
        val statusListener = new StatusListener {
          def onStallWarning(warning: StallWarning) {
            log.info("Got stall warning: " + warning)
          }

          def onException(e: Exception) {
            log.error("Exception occurred", e)
          }

          def onDeletionNotice(deletionNotice: StatusDeletionNotice) {}

          def onScrubGeo(userId: Long, upToStatusId: Long) {}

          def onStatus(status: Status) {
            writerRouter ! TweetReceived(status)
          }

          def onTrackLimitationNotice(numLimitedStatuses: Int) {}
        }
        tweetStream.addListener(statusListener)
        tweetStream.filter(new FilterQuery(initialTweets, usersToFollow))
      case Progress(numTweets) =>
        totalTweets += numTweets
        log.info(f"Wrote $totalTweets%d tweets.")
      case AvroTweet(tweet) =>
        indexerRouter ! AvroTweet(tweet)
      case _ => log.warn("Dropping unrecognized message.")
    }
  }

  private def makeTweet(status: Status): Option[Tweet] = {
    if (status != null) {
      Some(Tweet.newBuilder()
        .setCreatedAt(status.getCreatedAt.getTime)
        .setId(status.getId)
        .setText(status.getText)
        .setSource(status.getSource)
        .setIsTruncated(status.isTruncated)
        .setInReplyToStatusId(status.getInReplyToStatusId)
        .setInReplyToUserId(status.getInReplyToUserId)
        .setInReplyToScreenName(status.getInReplyToScreenName)
        .setGeolocation(makeGeolocation(status.getGeoLocation).getOrElse(null))
        .setPlace(makePlace(status.getPlace).getOrElse(null))
        .setIsFavorited(status.isFavorited)
        .setIsRetweeted(status.isRetweeted)
        .setFavoriteCount(status.getFavoriteCount)
        .setUser(makeUser(status.getUser))
        .setRetweetedStatus(makeTweet(status.getRetweetedStatus).getOrElse(null))
        .setContributors(status.getContributors.toList.map(Long.box).asJava)
        .setRetweetCount(status.getRetweetCount)
        .setIsPossiblySensitive(status.isPossiblySensitive)
        .setIsoLanguageCode(status.getIsoLanguageCode)
        .build())
    } else {
      None
    }
  }

  private def makeUser(u: twitter4j.User): com.adamkunicki.avro.User = {
    com.adamkunicki.avro.User.newBuilder()
      .setId(u.getId)
      .setName(u.getName)
      .setScreenName(u.getScreenName)
      .setLocation(u.getLocation)
      .setDescription(u.getDescription)
      .setFollowersCount(u.getFollowersCount)
      .setStatusesCount(u.getStatusesCount)
      .build()
  }

  private def makeGeolocation(geo: GeoLocation): Option[Geolocation] = {
    if (geo != null) {
      Some(Geolocation.newBuilder()
        .setLongitude(geo.getLongitude)
        .setLatitude(geo.getLatitude)
        .build())
    } else {
      None
    }
  }

  private def makePlace(p: twitter4j.Place): Option[com.adamkunicki.avro.Place] = {
    if (p != null) {
      val place = com.adamkunicki.avro.Place.newBuilder()
        .setName(p.getName)
        .setStreetAddress(p.getStreetAddress)
        .setCountry(p.getCountry)
        .setCountryCode(p.getCountryCode)
        .setId(p.getId)
        .setPlaceType(p.getPlaceType)
        .setUrl(p.getURL)
        .setFullName(p.getFullName)
        .setBoundingBoxType(p.getBoundingBoxType)
        .setGeometryType(p.getGeometryType)
        .build()
      try {
        place.setBoundingBoxCoordinates(p.getBoundingBoxCoordinates
          .map(_.map(makeGeolocation(_).getOrElse(null)).toList.asJava).toList.asJava)
      } catch {
        case e: NullPointerException => place.setBoundingBoxCoordinates(null)
      }
      try {
        place.setGeometryCoordinates(p.getGeometryCoordinates
          .map(_.map(makeGeolocation(_).getOrElse(null)).toList.asJava).toList.asJava)
      } catch {
        case e: NullPointerException => place.setGeometryCoordinates(null)
      }
      try {
        place.setContainedWithin(p.getContainedWithIn.toList
          .map(makePlace(_).getOrElse(null)).asJava)
      } catch {
        case e: NullPointerException => place.setContainedWithin(null)
      }
      Some(place)
    } else {
      None
    }
  }

}
