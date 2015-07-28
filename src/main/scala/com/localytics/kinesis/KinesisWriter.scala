package com.localytics.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit, Executor, ExecutorService}

import com.amazonaws.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scalaz.concurrent.Task
import scalaz.{\/, Contravariant, Functor}
import scalaz.syntax.either._
import scalaz.syntax.functor._

/**
 *
 */
object KinesisWriter {

  type StreamName   = String
  type PartitionKey = String

  //
  implicit def KinesisWriterCovariant(implicit es: ExecutorService) =
    new Contravariant[KinesisWriter] {
      override def contramap[A, B](k: KinesisWriter[A])(f: B => A): KinesisWriter[B] =
        new KinesisWriter[B](k.kinesisProducer) {
          def toInputRecord(b: => B): KinesisInputRecord[ByteBuffer] =
            k.toInputRecord(f(b))
        }
    }

  // Functor instance for ListenableFuture
  implicit val ListenableFutureFunctor = new Functor[ListenableFuture] {
    def map[A, B](fa: ListenableFuture[A])(f: A => B): ListenableFuture[B] =
      new ListenableFuture[B] {
        def addListener(r: Runnable, e: Executor): Unit = fa.addListener(r,e)
        def isCancelled: Boolean = fa.isCancelled
        def get(): B = f(fa.get)
        def get(timeout: Long, unit: TimeUnit): B = f(fa.get(timeout, unit))
        def cancel(mayInterruptIfRunning: Boolean): Boolean = fa.cancel(mayInterruptIfRunning)
        def isDone: Boolean = fa.isDone
      }
  }

  /**
   * Create a writer.
   * @param k KinesisProducer
   * @param stream The Kinesis stream name
   * @param partitioner A function that uses the input data to choose a shard.
   * @param mkInput A function to turn the input data into bytes.
   * @tparam A The generic type of the input data.
   * @return A KinesisWriter for the input data.
   */
  def noopWriter[A](
      k: KinesisProducer, stream: String, partitioner: A => String)
     (mkInput: A => Array[Byte])(implicit es: ExecutorService): KinesisWriter[A] = {
    new KinesisWriter[A](k) {
      def toInputRecord(a: => A) =
        KinesisInputRecord(stream, partitioner(a), ByteBuffer.wrap(mkInput(a)))
    }
  }
}

import KinesisWriter._

/**
 * Represents the 3 values Kinesis needs:
 *   Stream name, Partition Key, and ByteBuffer (the payload)
 *
 * However, for flexibility, this let the payload be anything.
 * It just has to be converted to a ByteArray at write time.
 *
 * @param stream
 * @param partitionKey
 * @param payload
 * @tparam T
 */
case class KinesisInputRecord[T](
  stream: StreamName,
  partitionKey: PartitionKey,
  payload: T
)

/**
 *
 */
object KinesisInputRecord {

  implicit def fromBytes(k:KinesisInputRecord[Array[Byte]]): KinesisInputRecord[ByteBuffer] =
    k.copy(payload = ByteBuffer.wrap(k.payload))

  // functor instance
  implicit val KinesisInputRecordFunctor: Functor[KinesisInputRecord] =
    new Functor[KinesisInputRecord] {
      def map[A, B](k: KinesisInputRecord[A])(f: A => B): KinesisInputRecord[B] =
        new KinesisInputRecord[B](k.stream, k.partitionKey, f(k.payload))
  }
}

/**
 * @param e Executor to handle their success and failure callbacks.
 * @tparam I
 */
abstract class KinesisWriter[I](val kinesisProducer: KinesisProducer)
                               (implicit val e: ExecutorService)
  extends AsyncWriter[I, UserRecordResult] { self =>

  type Result = Throwable \/ UserRecordResult

  /**
   * Turn the input into the 3 values Kinesis needs:
   *   Stream name, Partition Key, and ByteBuffer (the payload)
   * @param i
   * @return
   */
  def toInputRecord(i: => I): KinesisInputRecord[ByteBuffer]

  /**
   * A scalaz.concurrent.Task that runs asynchronously
   * invoking the future to write to Kinesis.
   * @param i
   * @return
   */
  def asyncTask(i: => I): Task[Result] =
    Task.async { (cb: (Throwable \/ (Result)) => Unit) =>
      Futures.addCallback(writeToKinesis(i), new FutureCallback[Result]() {
        def onSuccess(result: Result) = cb(result.right)
        def onFailure(t: Throwable) = cb(t.left.right)
      }, e)
    }

  /**
   * Actually run the input on Kinesis by first converting it to
   * the 3 values that Kinesis needs, and then calling Kinesis.
   * @return
   */
  private def writeToKinesis(i: => I): ListenableFuture[Result] = {
    val r = toInputRecord(i)
    // Presumably, KPL catches all errors, and never allows an exception
    // to be thrown (assumed because UserResultRecord has a successful field).
    // The map(_.right) here is just a formality.
    kinesisProducer.addUserRecord(r.stream, r.partitionKey, r.payload).map(_.right)
  }
}
