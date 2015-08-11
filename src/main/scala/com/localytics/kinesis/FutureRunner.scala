package com.localytics.kinesis

import java.util.concurrent.{TimeUnit, Executor, ExecutorService}

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scalaz.{Functor, \/, Contravariant}
import scalaz.concurrent.Task
import scalaz.syntax.either._
import scalaz.stream.{Channel, channel, Process}

/**
 *
 */
object FutureRunner {
  /**
   * The writer that simply returns its input.
   * @tparam A
   * @return
   */
  def idWriter[A] = new FutureRunner[A, A] {
    def makeFuture(a:A) = FutureRunner.pure(a)
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
   * Covariant Functor instance for Writer
   * if you have trouble getting this implicit resolved, see:
   * https://github.com/scalaz/scalaz/issues/976
   */
  implicit def WriterContravariant[O]: Contravariant[({type l[A]=FutureRunner[A,O]})#l] =
    new Contravariant[({type l[A]=FutureRunner[A,O]})#l] {
      def contramap[A, B](r: FutureRunner[A, O])(f: B => A): FutureRunner[B, O] =
        new FutureRunner[B, O] {
          def makeFuture(b: B): ListenableFuture[O] = r.makeFuture(f(b))
        }
    }

  /**
   * Creates a channel that accepts ListenableFuture[A] and returns A.
   * The channel does so simply by calling .get on the future,
   * thus blocking the current thread.
   * @return
   */
  def futureChannel[A]: Channel[Task, ListenableFuture[A], A] =
    channel lift (f => Task(f.get))

  def pure[A](a:A): ListenableFuture[A] = Futures.immediateFuture(a)

  //  def asyncTask_[I,O](i: I)(f: I => ListenableFuture[O])
  //                    (implicit e: ExecutorService): Task[O] = asyncTask(f(i))
  /**
   * A scalaz.concurrent.Task that runs the given future
   * asynchronously, on the given ExecutorService
   * @param o
   * @return
   */
  def asyncTask[O](o: ListenableFuture[O])
                  (implicit e: ExecutorService): Task[Throwable \/ O] =
    Task.async { (cb: (Throwable \/ (Throwable \/ O)) => Unit) =>
      Futures.addCallback(o, new FutureCallback[O]() {
        def onSuccess(result: O) = cb(result.right.right)
        def onFailure(t: Throwable) = cb(t.left.right)
      }, e)
    }
}

/**
 *
 * @tparam A
 * @tparam B
 */
trait FutureRunner[A,B] extends ProcessRunner[A,B] {

  /**
   *
   * @param a
   * @return
   */
  def makeFuture(a:A): ListenableFuture[B]

  private val writerChannel: Channel[Task, A, ListenableFuture[B]] =
    channel lift (a => Task(makeFuture(a)))

  /**
   * TODO: the naming here is terrible, runner and runnerProcess
   * @param p
   * @return
   */
  def runner(p:Process[Task, A]): Process[Task, B] =
    runnerProcess(p) through FutureRunner.futureChannel

  /**
   * TODO:
   * The bufferAll here is extremely important for efficiency.
   * However, it could be a big problem if p never ends.
   * Maybe it would be better to buffer in batches.
   * For example, `buffer(1024)` instead of `bufferAll`
   * The Kinesis API and documentation should be read before making
   * a change here.
   *
   * @param p
   * @return
   */
  def runnerProcess(p:Process[Task, A]): Process[Task, ListenableFuture[B]] =
    (p through writerChannel).bufferAll

  /**
   *
   * @param p
   * @return
   */
  def runnerProcessZipped(p:Process[Task, A]): Process[Task, (A, ListenableFuture[B])] =
    (p observeThrough writerChannel).bufferAll

  // TODO: consider bringing this back.
  // This was the basis of the old AsyncWriter/KinesisWriter.
  //  /**
  //   * An Channel running producing Os from Is via asyncTask.
  //   * Any errors in the input are passed through the channel unchanged.
  //   */
  //  def channelV: Channel[Task, Throwable \/ I, Throwable \/ O] =
  //    scalaz.stream.channel.lift(v => Task(v) flatMap {
  //      case t@ -\/(_) => Task(t)
  //      case \/-(i) => asyncTask(i) // TODO: might be able to catch errors here!
  //    })
}
