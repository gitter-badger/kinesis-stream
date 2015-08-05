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
  payload: ByteBuffer)


object AbstractKinesisProducer {
  implicit def AWSProducer(kp: KinesisProducer): AbstractKinesisProducer =
    new AbstractKinesisProducer {
      def addUserRecord(stream: String, partitionKey: String, data: ByteBuffer): ListenableFuture[UserRecordResult] =
        kp.addUserRecord(stream, partitionKey, data)
    }
}

trait AbstractKinesisProducer {
  def addUserRecord(stream: String,
                    partitionKey: String,
                    data: ByteBuffer): ListenableFuture[UserRecordResult]
}

/**
 * A KinesisWriter gives a few nice functions for calling Kinesis
 * But also gives access to lower level processes for more
 * fine grained control.
 */
case class KinesisWriter(kinesisProducer: AbstractKinesisProducer)
  extends AsyncWriter[KinesisInputRecord,UserRecordResult]{

  def makeFuture(r: KinesisInputRecord): ListenableFuture[UserRecordResult] =
    kinesisProducer.addUserRecord(r.stream, r.partitionKey, r.payload)
}
