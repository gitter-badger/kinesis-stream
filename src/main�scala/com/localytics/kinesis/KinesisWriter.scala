package com.localytics.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.ExecutorService

import com.amazonaws.kinesis.producer.{UserRecordFailedException, KinesisProducer, UserRecordResult}
import com.google.common.util.concurrent.ListenableFuture
import org.apache.commons.io.IOUtils

import scalaz.concurrent.Task
import scalaz.stream.{Process, channel, sink}
import scalaz.\/

/**
 *
 */
object KinesisWriter {
  type StreamName   = String
  type PartitionKey = String
}

import KinesisWriter._

/**
 * Represents the 3 values Kinesis needs:
 * @param stream Stream name
 * @param partitionKey Partition Key
 * @param payload the payload, as bytes
 */
case class KinesisInputRecord(
  stream: StreamName,
  partitionKey: PartitionKey,
  payload: String)

object AbstractKinesisProducer {
  implicit def AWSProducer(kp: KinesisProducer): AbstractKinesisProducer =
    new AbstractKinesisProducer {
      def addUserRecord(stream: String, partitionKey: String, data: ByteBuffer): ListenableFuture[UserRecordResult] =
        kp.addUserRecord(stream, partitionKey, data)
      def getOutstandingRecordsCount: Int = kp.getOutstandingRecordsCount
    }
}

trait AbstractKinesisProducer {
  def addUserRecord(stream: String,
                    partitionKey: String,
                    data: ByteBuffer): ListenableFuture[UserRecordResult]
  def getOutstandingRecordsCount: Int
}

/**
 * A KinesisWriter gives a few nice functions for calling Kinesis
 * But also gives access to lower level processes for more
 * fine grained control.
 */
case class KinesisWriter(kinesisProducer: AbstractKinesisProducer)
  extends FutureRunner[KinesisInputRecord,UserRecordResult]{

  /**
   * Gets a ListenableFuture[UserRecordResult] by writing to the
   * given KinesisProducer.
   *
   * TODO: This currently assumes the most naive serialization
   *       possible, simply taking the bytes from the String.
   * @param r
   * @return
   */
  def makeFuture(r: KinesisInputRecord): ListenableFuture[UserRecordResult] = {
    // this while loop is suggested in this AWS blog post:
    // https://blogs.aws.amazon.com/bigdata/post/Tx3ET30EGDKUUI2/Implementing-Efficient-and-Reliable-Producers-with-the-Amazon-Kinesis-Producer-L
    while (kinesisProducer.getOutstandingRecordsCount > 1e4) { Thread.sleep(1) }
    kinesisProducer.addUserRecord(r.stream, r.partitionKey, ByteBuffer.wrap(r.payload.getBytes))
  }

  /**
   * TODO:
   * The buffer all here is extremely important for efficiency.
   * However, it could be a big problem if p never ends.
   * Maybe it would be better to buffer in batches.
   * For example, `buffer(1024)` instead of `bufferAll`
   * The Kinesis API and documentation should be read before making
   * a change here.
   *
   * @param p
   * @return
   */
  override def runner(p:Process[Task, KinesisInputRecord]): Process[Task,UserRecordResult] =
    super.runner(p).bufferAll

  /**
   * Sends the input to Kinesis, and invokes onSuccess or onFailure
   * for each record in the output.
   * @param input
   * @param onSuccess
   * @param onFailure
   * @param e
   * @return
   */
  def fireAndObserveWith(
    input: Process[Task, KinesisInputRecord],
    onSuccess: (KinesisInputRecord, UserRecordResult) => Unit,
    onFailure: (KinesisInputRecord, UserRecordFailedException) => Unit)
    (implicit e: ExecutorService): Process[Task,Unit] = {

    def handler(i: KinesisInputRecord, v: Throwable \/ UserRecordResult): Task[Unit] =
      v.fold({
        case u:UserRecordFailedException => Task(onFailure(i,u))
        // The blog above swallows exceptions...
        // Maybe we should do something smarter here.
        // But also, maybe Amazon has arranged it so this wont ever happen.
        case t: Throwable => throw t
      }, a => Task(onSuccess(i, a)))
    runnerProcessZipped(input).bufferAll.through(channel lift {
      case (a,b) => FutureRunner.asyncTask(b).map((a,_))
    }).to(sink lift (handler _).tupled)
  }
}
