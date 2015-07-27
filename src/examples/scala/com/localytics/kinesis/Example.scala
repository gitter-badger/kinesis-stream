package com.localytics.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.ExecutorService

import com.amazonaws.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.google.common.util.concurrent.MoreExecutors

import scalaz.\/
import scalaz.concurrent.Task
import scalaz.stream._

object E {
  // ExecutorService needed to handle success and failure callbacks.
  implicit val e: ExecutorService = MoreExecutors.newDirectExecutorService()
}

import com.localytics.kinesis.E._

object GettingStartedFast {
  def gettingStarted : Process[Task, Throwable \/ UserRecordResult] = {
    val kw = new KinesisWriter[String] {
      val kinesisProducer: KinesisProducer = new KinesisProducer()
      def toInputRecord(s: String) = KinesisInputRecord(
        "my-stream", partitionKey(s), ByteBuffer.wrap(s.getBytes)
      )
    }
    val data = Process("Hello", ", ", "World", "!!!")
    kw.process(data): Process[Task, Throwable \/ UserRecordResult]
  }
  def partitionKey(s: String): String = s + "tubular"
}

/**
 * This example will return a Channel that can
 *   - Write Strings to Kinesis
 *   - Emit KPL UserRecordResults
 **/
object DeepDive {

  def deepDive : Channel[Task, Throwable \/ String, Throwable \/ UserRecordResult] = {

    // Create a KinesisWriter that writes Strings to Kinesis
    // You can write anything that you can Serialize to bytes
    val kw = new KinesisWriter[String] {
      // The writer needs an actual AWS KinesisProducer object
      val kinesisProducer = new KinesisProducer()

      /**
       * Kinesis needs to know 3 things:
       *   - The stream to write the record to
       *   - The partition key (determines the shard written to)
       *   - And some bytes that actually contain the payload.
       *
       * This info is captured in a KinesisInputRecord
       * which you create in toInputRecord using your input.
       */
      def toInputRecord(s: String) = KinesisInputRecord(
        "my-stream", "shard-" + s, ByteBuffer.wrap(s.getBytes)
      )
    }

    //// Now that we have a KinesisWriter, we can put it to use. ////

    // Get a scalaz-stream Channel that accepts Strings
    // and outputs UserRecordResult returned from the KPL
    val channel = kw.channel

    // Prepare some data to put into kinesis
    val data = Process("Hello", ", ", "World", "!!!")

    // Get the process from the Writer.
    // The process feeds all the data to Kinesis and returns UserRecordResults
    val process: Process[Task, Throwable \/ UserRecordResult] = kw.process(data)

    // Now, run the process and obtain all the responses from Kinesis
    val results: IndexedSeq[Throwable \/ UserRecordResult] = process.runLog.run

    // All that could have been accomplished more simply:
    kw.write(Process("Hello", ", ", "World", "!!!"))

    // But, it's having the Channel and Process that are important.
    // At this point you can get all sorts of info from the
    // UserResultRecords, such as shard id, sequence number, and more

    // But for now, we'll just return the process
    channel
  }

  /**
   * The partition key determines which shard your record
   * gets written to. We called it while creating the Writer
   *
   * I don't recommend this particular strategy, but it sure is fun!
   */
  def partitionKey(s: String): String = s + "tubular"
}