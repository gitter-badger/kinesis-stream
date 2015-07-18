package com.localytics.kinesis

import com.google.common.util.concurrent.{
  FutureCallback, Futures, MoreExecutors, ListenableFuture
}
import java.util.concurrent.{ExecutorService, Callable}
import scalaz.\/
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.syntax.either._

/**
 * scalaz-stream extension providing functionality for
 * handling operations that return ListenableFutures
 */
object Writer {

  /**
   * Build a future that will run the computation (f)
   * on the input (i) on the executor service (es)
   */
  def executorChannel[I,O](i: I)(f: I => O)
                          (implicit es: ExecutorService): ListenableFuture[O] =
    MoreExecutors.listeningDecorator(es).submit(new Callable[O] {
      def call: O = f(i)
    })

  /**
   * Run the input through the writers synchProcess
   * @param is
   * @param e
   */
  def writeSynch[I,O](is: Seq[I], writer: Writer[I,O])
                     (implicit e: ExecutorService): Unit =
    writer.synchProcess(is).run.run


  /**
   * Run the input through asynchProcess.
   * @param is
   */
  def writeAsynch[I,O](is: Seq[I], writer: Writer[I,O])
                      (implicit e: ExecutorService): Unit =
    writer.asynchProcess(is).run.run
}

/**
 * scalaz-stream extension providing functionality for
 * handling operations that return ListenableFutures
 * @tparam I
 * @tparam O
 */
trait Writer[I,O] { self =>

  /**
   * Given some i, produce an asynchronous computation that produces an O
   * @param i
   * @return
   */
  def eval(i:I): ListenableFuture[O]

  /**
   * If the evaluation fails, take care of it here.
   * @param t
   */
  def onFailure(t: Throwable): Unit

  /**
   * If the evaluation succeeds, do any finalization work.
   * @param result
   */
  def onSuccess(result: O): Unit

  /**
   * An Process running in Task, producing Os from Is.
   * The tasks always wait for operations to complete.
   * @param is
   * @return
   */
  def synchProcess(is: Seq[I])(implicit e: ExecutorService): Process[Task, O] =
    mkProcess(is, synchChannel)

  /**
   * An Channel running in Task, producing Os from Is.
   * The tasks always wait for operations to complete.
   * @return
   */
  def synchChannel(implicit e: ExecutorService): Channel[Task, I, O] =
    channel.lift(synchTask)

  /**
   * Given some Is, return an 'synchronous' Process producing Os.
   * See synchChannel and synchTask for details.
   * @param i
   * @return
   */
  def synchTask(i: I)(implicit e: ExecutorService): Task[O] = Task.suspend({
    val fo = eval(i)
    Futures.addCallback(fo, new FutureCallback[O]() {
      def onSuccess(result: O) = self.onSuccess(result)
      def onFailure(t: Throwable) = self.onFailure(t)
    }, e)
    Task(fo.get)
  })

  /**
   * Given some Is, return an 'asynchronous' Process producing Os.
   * See asynchChannel and asynchTask for details.
   * @param is
   * @return
   */
  def asynchProcess(is: Seq[I])(implicit e: ExecutorService): Process[Task, O] =
    mkProcess(is, asynchChannel)

  private def mkProcess(is:Seq[I], channel: Channel[Task, I, O])
                       (implicit e: ExecutorService): Process[Task, O] =
    Process(is:_*).tee(channel)(tee.zipApply).eval

  /**
   * An asynchronous Channel running in Task, producing Os from Is.
   * The tasks don't wait for operations to complete.
   * @return
   */
  def asynchChannel(implicit e: ExecutorService): Channel[Task, I, O] =
    channel.lift(asynchTask)

  /**
   * A scalaz.concurrent.Task that runs asynchronously
   * invoking futures that turns `I`s into `O`s.
   * The Task does not wait for the future to complete execution.
   * @param i
   * @return
   */
  def asynchTask(i: I)(implicit e: ExecutorService): Task[O] = Task.suspend({
    Task.async { (cb: (Throwable \/ O) => Unit) =>
      Futures.addCallback(eval(i), new FutureCallback[O]() {
        def onSuccess(result: O) = {
          cb(result.right)
          self.onSuccess(result)
        }
        def onFailure(t: Throwable) = {
          cb(t.left)
          self.onFailure(t)
        }
      }, e)
    }
  })
}
