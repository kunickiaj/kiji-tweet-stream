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

object TweetIngester extends App {

  lazy val log = LoggerFactory.getLogger(getClass.getName)

  val kijiUri = KijiURI.newBuilder(args(0)).build()
  val tweetTableName = "tweet"
  val COL_F = "info"
  val COL_Q = "tweet"

  val esIndexName = "twitter"
  val esDocType = "tweet"
  val esHostAddress = "xwing07.ul.wibidata.net"

  val numWriters = 2
  val numIndexers = 2

  // Try to open a Kiji instance.
  val kiji = Kiji.Factory.open(kijiUri)
  val tablePool = KijiTablePool.newBuilder(kiji)
    .withIdleTimeout(10)
    .withIdlePollPeriod(1)
    .build()

  val system = ActorSystem("TweetIngestSystem")
  val listener = system.actorOf(Props[Listener], name = "listener")
  val master = system.actorOf(Props(new Master(numWriters, numIndexers, listener)))

  master ! Start

  class KijiTweetWriter extends Actor {

    var tweetCount: Int = _

    def receive = {
      case TweetReceived(status) =>
        log.debug("Tweet! " + status.getText)
        val table = tablePool.get(tweetTableName)
        val writer = table.openTableWriter()
        try {
          makeTweet(status) match {
            case Some(tweet) =>
              writer.put(table.getEntityId(status.getUser.getId: java.lang.Long), COL_F, COL_Q, tweet.getCreatedAt, tweet)
              tweetCount += 1
              if (tweetCount % 500 == 0) {
                writer.flush()
                sender ! Progress(tweetCount)
              }
            case None => log.warn("Unable to parse Tweet")
          }
        } catch {
          case e: IOException =>
            log.error("Error while writing tweet", e)
        } finally {
          writer.close()
          table.release()
        }
    }
  }

  class Indexer extends Actor {
    val client = new TransportClient()
      .addTransportAddress(new InetSocketTransportAddress(esHostAddress, 9300))
    val bulkRequest = client.prepareBulk()

    var tweetCount: Int = _

    def receive = {
      case AvroTweet(tweet) =>
        bulkRequest.add(client.prepareIndex(esIndexName, esDocType, tweet.getId.toString)
          .setSource(ToJson.toJsonString(tweet, tweet.getSchema)))
        tweetCount += 1

        if (tweetCount % 500 == 0) {
          val response = bulkRequest.execute().actionGet()
          if (response.hasFailures) {
            log.error(response.buildFailureMessage())
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
            indexerRouter ! TweetReceived(status)
          }

          def onTrackLimitationNotice(numLimitedStatuses: Int) {}
        }
        tweetStream.addListener(statusListener)
        tweetStream.sample()
      case Progress(numTweets) =>
        totalTweets += numTweets
        log.info(f"Wrote $totalTweets%d tweets.")
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
