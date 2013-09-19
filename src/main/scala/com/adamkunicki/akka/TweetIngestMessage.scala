package com.adamkunicki.akka

import twitter4j.Status
import com.adamkunicki.avro.Tweet

sealed trait TweetIngestMessage

case class Start() extends TweetIngestMessage
case class TweetReceived(tweet: Status) extends TweetIngestMessage
case class AvroTweet(tweet: Tweet) extends TweetIngestMessage
case class Progress(numTweets: Int) extends TweetIngestMessage
case class Finish() extends TweetIngestMessage