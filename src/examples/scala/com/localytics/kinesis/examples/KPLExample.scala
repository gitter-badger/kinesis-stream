package com.localytics.kinesis.examples

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.localytics.kinesis.{KinesisInputRecord, KinesisWriter}

import scalaz.concurrent.Task
import scalaz.stream._

object KPLExample {

  object GettingStartedFast {
    val writer = KinesisWriter(new KinesisProducer())
    def partitionKey(s: String): String = s + "tubular"
    def toInputRecord(s: String) =
      KinesisInputRecord("my-stream", partitionKey(s), s)
    val input = Process("Hello", ", ", "World", "!!!")
    writer.fireAndForget(input map toInputRecord)
  }

  /*
   * This example will return a Process that will
   *   - Write Strings to Kinesis
   *   - Emit KPL UserRecordResults
   **/
  object DeepDive {

    def deepDive : Process[Task, UserRecordResult] = {

      // The writer needs an actual AWS KinesisProducer object
      val kinesisProducer = new KinesisProducer()

      // Create a KinesisWriter that writes Strings to Kinesis
      // You can write anything that you can Serialize to bytes
      val kw = new KinesisWriter(kinesisProducer)

      /*
       * Kinesis needs to know 3 things:
       *   - The stream to write the record to
       *   - The partition key (determines the shard written to)
       *   - And some bytes that actually contain the payload.
       *
       * Here, we'll just convert some Strings to KinesisInputRecords.
       */
      def toInputRecord(s: String) =
        KinesisInputRecord("my-stream", "shard-" + s, s)

      //// Now that we have a KinesisWriter, we can put it to use. ////

      // Prepare some data to write to Kinesis
      val data: Process[Task, KinesisInputRecord] =
        Process("Hello", ", ", "World", "!!!").map(toInputRecord)

      // Create a sink that processes the UserRecordResults
      // as they come back from Kinesis
      val errorLoggerSink: Sink[Task,UserRecordResult] =
        sink lift (r => Task(println(r.isSuccessful)))

      // Create a Process that feeds pipes data to Kinesis
      // The process returns UserRecordResults
      // The errorLoggerSink is also attached as an observer,
      // logging any errors.
      val process: Process[Task, UserRecordResult] =
        kw.runner(data) observe errorLoggerSink

      // Now, run the process and obtain all the responses from Kinesis
      val results: Seq[UserRecordResult] = process.runLog.run

      // At this point you can get all sorts of info from the
      // UserResultRecords, such as shard id, sequence number, and more
      // But for now, we'll just return the process
      process
    }

    /*
     * The partition key determines which shard your record
     * gets written to. We called it while creating the Writer
     *
     * I don't recommend this particular strategy, but it sure is fun!
     */
    def partitionKey(s: String): String = s + "tubular"
  }
}
