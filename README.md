[![Build Status](https://travis-ci.org/localytics/kinesis-stream.png?branch=master)](https://travis-ci.org/joshcough/kinesis-stream)
[![Coverage Status](https://coveralls.io/repos/localytics/kinesis-stream/badge.svg?branch=master)](https://coveralls.io/r/joshcough/kinesis-stream?branch=master)
[![Join the chat at https://gitter.im/joshcough/kinesis-stream](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/localytics/kinesis-stream?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
`
# kinesis-stream

kinesis-stream is a scalaz-stream API for Amazon Kinesis.

kinesis-stream currently supports the KPL, but hopefully
will soon support the KCL, and maybe the original AWS API as well.

## Examples

All the examples that follow can be found in `src/main/scala/com/localytics/kinesis/Example.scala`

### Getting Started with KPL Streaming.

First we'll get started with a simple example with no comments,
and then we'll review it in detail, and dive even deeper.

This is all you need to get started:

```scala
object GettingStartedFast {
  val channel : Channel[Task, String, Throwable \/ UserRecordResult] =
    new KinesisWriter[String](new KinesisProducer()) {
      def partitionKey(s: String): String = s + "tubular"
      def toInputRecord(s: => String) = KinesisInputRecord(
        "my-stream", partitionKey(s), ByteBuffer.wrap(s.getBytes)
      )
    }.channel
  (Process("Hello", ", ", "World", "!!!") through channel).run.run
}
```

### Deep Dive

Here is a larger example that breaks things down a little more.

```scala
/**
 * This example will return a Channel that can
 *   - Write Strings to Kinesis
 *   - Emit KPL UserRecordResults
 **/
object DeepDive {

  def deepDive : Channel[Task, String, Throwable \/ UserRecordResult] = {

    // The writer needs an actual AWS KinesisProducer object
    val kinesisProducer = new KinesisProducer()

    // Create a KinesisWriter that writes Strings to Kinesis
    // You can write anything that you can Serialize to bytes
    val kw = new KinesisWriter[String](kinesisProducer) {
      /**
       * Kinesis needs to know 3 things:
       *   - The stream to write the record to
       *   - The partition key (determines the shard written to)
       *   - And some bytes that actually contain the payload.
       *
       * This info is captured in a KinesisInputRecord
       * which you create in toInputRecord using your input.
       */
      def toInputRecord(s: => String) = KinesisInputRecord(
        "my-stream", "shard-" + s, ByteBuffer.wrap(s.getBytes)
      )
    }

    //// Now that we have a KinesisWriter, we can put it to use. ////

    // Get a scalaz-stream Channel that accepts Strings
    // and outputs UserRecordResults returned from the KPL
    val channel = kw.channel

    // Prepare some data to write to Kinesis
    val data = Process("Hello", ", ", "World", "!!!")

    // Create a sink that processes the UserRecordResults
    // as they come back from Kinesis
    val errorLoggerSink: Sink[Task,Throwable \/ UserRecordResult] =
      sink lift (v => Task(if(v.isLeft) println(v)))

    // Create a Process that feeds pipes data through the channel
    // The channel returns UserRecordResults (or errors)
    // The errorLoggerSink is also attached as an observer,
    // logging any errors.
    val process: Process[Task, Throwable \/ UserRecordResult] =
      (data through channel).observe(errorLoggerSink)

    // Now, run the process and obtain all the responses from Kinesis
    val results: Seq[Throwable \/ UserRecordResult] = process.runLog.run

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
```
