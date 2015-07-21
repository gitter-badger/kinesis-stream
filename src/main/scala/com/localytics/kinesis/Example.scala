/**
 * Created by jcough on 7/19/15.
 */
package com.localytics.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.Executor

import com.amazonaws.kinesis.producer.{UserRecordResult, KinesisProducer}
import com.google.common.util.concurrent.MoreExecutors

import scalaz.\/
import scalaz.concurrent.Task
import scalaz.stream._

object E {
  // Executor needed to handle success and failure callbacks.
  implicit val e: Executor = MoreExecutors.directExecutor()
}

import E._

object GettingStartedFast {
  def gettingStarted : Channel[Task, String, Throwable \/ UserRecordResult] = {
    val kw = new KinesisWriter[String] {
      val kinesisProducer: KinesisProducer = new KinesisProducer()
      def toInputRecord(s: String) = KinesisInputRecord(
        "my-stream", partitionKey(s), ByteBuffer.wrap(s.getBytes)
      )
    }
    kw.write(List("Hello", ", ", "World", "!!!"))
    kw.channel : Channel[Task, String, Throwable \/ UserRecordResult]
  }
  def partitionKey(s: String): String = s + "tubular"
}

/**
 * This example will return a Channel that can
 *   - Write Strings to Kinesis
 *   - Emit KPL UserRecordResults
 **/
object DeepDive {

  def deepDive : Channel[Task, String, Throwable \/ UserRecordResult] = {

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
    val data = List("Hello", ", ", "World", "!!!")

    // Create a process by feeding all the data to the channel.
    // The result is a process that simply emits the results.
    val process_ : Process[Task, Throwable \/ UserRecordResult] =
      Process(data: _*).tee(channel)(tee.zipApply).eval

    // Or you can just get the process from the Writer :)
    val process: Process[Task, Throwable \/ UserRecordResult] = kw.process(data)

    // Now, run the process and obtain all the responses from Kinesis
    val results: IndexedSeq[Throwable \/ UserRecordResult] = process.runLog.run

    // All that could have been accomplished more simply:
    kw.write(List("Hello", ", ", "World", "!!!"))

    // But, it's having the Channel and Process that are important.
    // At this point you can get all sorts of info from the
    // UserResultRecords, such as shard id, sequence number, and more

    // But for now, we'll just return the channel
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