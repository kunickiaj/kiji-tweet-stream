package com.adamkunicki.akka

import twitter4j.Status

sealed trait TweetIngestMessage

case class Start() extends TweetIngestMessage
case class TweetReceived(tweet: Status) extends TweetIngestMessage
case class Progress(numTweets: Int) extends TweetIngestMessage
case class Finish() extends TweetIngestMessage