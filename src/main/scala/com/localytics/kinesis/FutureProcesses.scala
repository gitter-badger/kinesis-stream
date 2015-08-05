package com.localytics.kinesis

import java.util.concurrent.ExecutorService

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scalaz._
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.syntax.either._
import Process._

object FutureProcesses {
  /**
   * Creates a channel that accepts ListenableFuture[A] and returns A.
   * The channel does so simply by calling .get on the future,
   * thus blocking the current thread.
   * @return
   */
  def futureChannel[A]: Channel[Task, ListenableFuture[A], A] =
    channel lift (f => Task(f.get))

  def pure[A](a:A): ListenableFuture[A] = Futures.immediateFuture(a)

  /**
   * A scalaz.concurrent.Task that runs asynchronously
   * @param i
   * @return
   */
  def asyncTask[I,O](i: I)(f: I => ListenableFuture[O])
                    (implicit e: ExecutorService): Task[O] =
    Task.async { (cb: (Throwable \/ O) => Unit) =>
      Futures.addCallback(f(i), new FutureCallback[O]() {
        def onSuccess(result: O) = cb(result.right)
        def onFailure(t: Throwable) = cb(t.left)
      }, e)
    }
}