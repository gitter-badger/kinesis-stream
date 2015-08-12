kinesis-stream
==============

[![Join the chat at https://gitter.im/localytics/kinesis-stream](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/localytics/kinesis-stream?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

kinesis-stream is a scalaz-stream API for Amazon Kinesis.

kinesis-stream currently supports the KPL, but hopefully
will soon support the KCL, and maybe the original AWS API as well.

[![Download](https://api.bintray.com/packages/localytics/maven/kinesis-stream/images/download.svg)](https://bintray.com/localytics/maven/kinesis-stream/_latestVersion)
[![Build Status](https://travis-ci.org/localytics/kinesis-stream.png?branch=master)](https://travis-ci.org/joshcough/kinesis-stream)
[![Coverage Status](https://coveralls.io/repos/localytics/kinesis-stream/badge.svg?branch=master&service=github)](https://coveralls.io/github/localytics/kinesis-stream?branch=master)
[![Join the chat at https://gitter.im/joshcough/kinesis-stream](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/localytics/kinesis-stream?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
`

### Where to get it ###

To get the latest version of the library, add the following to your SBT build:

```
// available for Scala 2.10.5, 2.11.7
libraryDependencies += "com.localytics" %% "kinesis-stream" % "0.2.0"
```

## Examples

All the examples that follow can be found in `src/main/scala/com/localytics/kinesis/examples`

### Getting Started with KPL Streaming.

First we'll get started with a simple example with no comments,
and then we'll review it in detail, and dive even deeper.

This is all you need to get started:

```scala
object GettingStartedFast {
  val writer = KinesisWriter(new KinesisProducer())
  def partitionKey(s: String): String = s + "tubular"
  def toInputRecord(s: String) = KinesisInputRecord(
    "my-stream", partitionKey(s), ByteBuffer.wrap(s.getBytes))
  val input = Process("Hello", ", ", "World", "!!!")
  writer.fireAndForget(input map toInputRecord)
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
    def toInputRecord(s: String) = KinesisInputRecord(
      "my-stream", "shard-" + s, ByteBuffer.wrap(s.getBytes)
    )

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
```
