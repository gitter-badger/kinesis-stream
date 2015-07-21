package com.localytics.kinesis

import com.google.common.util.concurrent.{MoreExecutors => M, FutureCallback, Futures, ListenableFuture}
import java.util.concurrent.{TimeUnit, Executor, ExecutorService, Callable}
import scalaz.effect.IO
import scalaz.{Contravariant, Functor, \/}
import scalaz.concurrent.Task
import scalaz.stream.{channel => _, _}
import scalaz.syntax.either._

/**
 * scalaz-stream extension providing functionality for
 * handling operations that return ListenableFutures
 */
object Writer {

  /**
   * The writer that simply returns its input.
   * @param e
   * @tparam A
   * @return
   */
  def idWriter[A](implicit e: ExecutorService) = new Writer[A, A] { self =>
    def eval(a:A) = executorChannel(a, e)(identity)
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

  // Covariant Functor instance for Writer
  // TODO: why isn't this implicit being resolved?
  implicit def WriterContravariant[O]: Contravariant[Writer[?,O]] =
    new Contravariant[Writer[?, O]] {
      def contramap[A, B](r: Writer[A, O])(f: B => A): Writer[B, O] =
        new Writer[B, O] {
          def eval(b: B): ListenableFuture[O] = r.eval(f(b))
        }
    }

  /**
   * Build a ListenableFuture that will run the computation (f)
   * on the input (i) on the executor service (es)
   */
  def executorChannel[I,O](i: I, es: ExecutorService)
                          (f: I => O): ListenableFuture[O] =
    M.listeningDecorator(es).submit(new Callable[O] { def call: O = f(i) })

  /**
   *
   * @param is
   * @param channel
   * @param e
   * @return
   */
  def mkProcess[I,O](is:Seq[I], channel: Channel[Task, I, O])
                    (implicit e: Executor): Process[Task, O] =
    Process(is:_*).tee(channel)(tee.zipApply).eval
}

/**
 * scalaz-stream extension providing functionality for
 * handling operations that return ListenableFutures
 * @tparam I
 * @tparam O
 */
trait Writer[-I,O] { self =>

  /**
   * Given some i, produce an asynchronous computation that produces an O
   * @param i
   * @return
   */
  def eval(i:I): ListenableFuture[O]

  /**
   * Run the input through the writers process,
   * collecting the results.
   * @param is
   * @param e
   * @return
   */
  def collect(is: Seq[I])(implicit e: Executor): Seq[Throwable \/ O] = process(is).runLog.run

  /**
   * Run the input through the writers asyncProcess,
   * discarding the results.
   * @param is
   * @param e
   * @return
   */
  def write(is: Seq[I])(implicit e: Executor): Unit = process(is).run.run

  /**
   * Given some Is, return an 'asynchronous' Process producing Os.
   * See asyncChannel and asyncTask for details.
   * @param is
   * @return
   */
  def process(is: Seq[I])(implicit e: Executor): Process[Task, Throwable \/ O] =
    Writer.mkProcess(is, channel)

  /**
   * An asynchronous Channel running in Task, producing Os from Is.
   * The tasks don't wait for operations to complete.
   * @return
   */
  def channel(implicit e: Executor): Channel[Task, I, Throwable \/ O] =
    scalaz.stream.channel.lift(asyncTask)

  /**
   * A scalaz.concurrent.Task that runs asynchronously
   * invoking futures that turns `I`s into `O`s.
   * The Task does not wait for the future to complete execution.
   * @param i
   * @return
   */
  def asyncTask(i: I)(implicit e: Executor): Task[Throwable \/ O] =
    Task.async { (cb: (Throwable \/ (Throwable \/ O)) => Unit) =>
      Futures.addCallback(eval(i), new FutureCallback[O]() {
        def onSuccess(result: O) = cb(result.right.right)
        def onFailure(t: Throwable) = cb(t.left)
      }, e)
    }

  def contramap[I2](f: I2 => I): Writer[I2, O] =
    Writer.WriterContravariant.contramap(self)(f)

//  Notes from Spiewak
//  def through[F2[x]>:F[x],O2](f: Channel[F2,O,O2], limit: Int):
//    Process[F2, Process[Task, O2]] =
//      merge.mergeN(self.zipWith(f)((o,f) => f(o)) map eval, limit)
//  //runLog.runAsync(cb)

}
