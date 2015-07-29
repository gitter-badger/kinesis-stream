package com.localytics.kinesis

import scalaz.concurrent.Task
import scalaz.stream._
import scala.collection.JavaConverters._

object KCLExample {

  object GettingStartedFast {

    val queue = async.unboundedQueue[WorkerRecordsInput]

    // when records come from kinesis, they get passed into this sink.
    // all this sink does is add records to the queue above.
    val recordSink: Sink[Task, WorkerRecordsInput] =
      sink lift (pri => queue.enqueueOne(pri))

    // run the KCL, which puts WorkerRecordsInputs onto the queue
    KCL.go(recordSink, KCL.kinesisClientConfig("sampleApp", "sampleStream"))

    // get the records and do whatever you want with them
    // here we just print them
    def printRecords(wri: WorkerRecordsInput): Task[Unit] =
      Task(wri.processRecordsInput.getRecords.asScala.foreach(println(_)))

    // This kicks everything off and runs until the queue is closed.
    (queue.dequeue to (sink lift printRecords)).run.run
  }
}
