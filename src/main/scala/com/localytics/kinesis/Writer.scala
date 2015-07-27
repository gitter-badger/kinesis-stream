package com.localytics.kinesis

import com.google.common.util.concurrent.{ListeningExecutorService, FutureCallback, Futures, ListenableFuture}
import java.util.concurrent.{Executor, TimeUnit, ExecutorService, Callable}
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{channel => _, _}
import scalaz.syntax.either._
import scalaz.syntax.functor._
import Process._

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
  def idWriter[A](implicit e: ListeningExecutorService) =
    new Writer[A, A] { self =>
      def eval(a:A) = buildFuture(a)(identity)(e)
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
          def eval(b: B): ListenableFuture[Throwable \/ O] = r.eval(f(b))
        }
    }

  /**
   * Build a ListenableFuture that will run the computation (f)
   * on the input (i) on the executor service (es)
   */
  def buildFuture[I,O](i: => I)(f: I => O)
                      (implicit es: ListeningExecutorService):
    ListenableFuture[Throwable \/ O] =
      es.submit(new Callable[Throwable \/ O] {
        def call: Throwable \/ O =
          try f(i).right catch { case t: Throwable => t.left }
      })

  /**
   * This might already exist somewhere.
   * @param a
   * @tparam A
   * @return
   */
  def safely[A](a: => A): Throwable \/ A =
    try a.right
    catch { case t: Throwable => t.left }
}

/**
 * scalaz-stream extension providing functionality for
 * handling operations that return ListenableFutures
 * @tparam I
 * @tparam O
 */
trait Writer[-I,O] { self =>

  import Writer._

  /**
   * Given some i, produce an asynchronous computation that
   * produces a Throwable \/ O. It is the callers responsibility
   * to catch any exception during the execution of that computation,
   * and propagate it back as a -\/, because this cannot
   * be done reliably on a regular ExecutorService.
   * @param i
   * @return
   */
  def eval(i:I): ListenableFuture[Throwable \/ O]

  /**
   * Run the input through the writers process,
   * collecting the results.
   * @param p
   * @param e
   * @return
   */
  def collect(p: Process0[I])
             (implicit e: ExecutorService): Seq[Throwable \/ O] =
    process(p)(e).runLog.run

  /**
   *
   * @param p
   * @param e
   * @return
   */
  def collectV(p: Process0[Throwable \/ I])
              (implicit e: ExecutorService): Seq[Throwable \/ O] =
    processV(p)(e).runLog.run

  /**
   * Run the input through the writers asyncProcess,
   * discarding the results.
   * @param p
   * @param e
   * @return
   */
  def writeV(p: Process0[Throwable \/ I])(implicit e: ExecutorService): Unit =
    processV(p).run.run

  /**
   *
   * @param p
   * @param e
   * @return
   */
  def write(p: Process0[I])(implicit e: ExecutorService): Unit =
    process(p).run.run

  /**
   * Given some Is, return an 'asynchronous' Process producing Os.
   * See asyncChannel and asyncTask for details.
   * @param p
   * @return
   */
  def process(p: Process0[I])
             (implicit e: ExecutorService): Process[Task, Throwable \/ O] =
    processV(p.map(_.right))

  /**
   *
   * @param p
   * @param e
   * @return
   */
  def processV(p: Process0[Throwable \/ I])
             (implicit e: ExecutorService): Process[Task, Throwable \/ O] =
    p through channel

  /**
   * An asynchronous Channel running in Task, producing Os from Is.
   * The tasks don't wait for operations to complete.
   * @return
   */
  def channel(implicit e: ExecutorService): Channel[Task, Throwable \/ I, Throwable \/ O] = {
    def asyncV(v: Throwable \/ I): Task[Throwable \/ O] = Task(v) flatMap {
      case t@ -\/(_) => Task(t)
      case \/-(i) => asyncTask(i)
    }
    scalaz.stream.channel.lift(asyncV(_))
  }

  /**
   * A scalaz.concurrent.Task that runs asynchronously
   * invoking futures that turns `I`s into `O`s.
   * The Task does not wait for the future to complete execution.
   * @param i
   * @return
   */
  def asyncTask(i: => I)(implicit e: ExecutorService): Task[Throwable \/ O] =
    Task.async { (cb: (Throwable \/ (Throwable \/ O)) => Unit) =>
      Futures.addCallback(eval(i), new FutureCallback[Throwable \/ O]() {
        def onSuccess(result: Throwable \/ O) = cb(result.right)
        def onFailure(t: Throwable) = cb(t.left.right)
      }, e)
    }

  /**
   *
   * @param f
   * @tparam I2
   * @return
   */
  def contramap[I2](f: I2 => I): Writer[I2, O] =
    WriterContravariant.contramap(self)(f)

//  Notes from Spiewak
//  def through[F2[x]>:F[x],O2](f: Channel[F2,O,O2], limit: Int):
//    Process[F2, Process[Task, O2]] =
//      merge.mergeN(self.zipWith(f)((o,f) => f(o)) map eval, limit)
//  //runLog.runAsync(cb)
}
