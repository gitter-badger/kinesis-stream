package com.localytics.kinesis

import com.google.common.util.concurrent.ListenableFuture

import scalaz.Contravariant
import scalaz.concurrent.Task
import scalaz.stream.{Channel, channel, Process}

object AsyncWriter {
  /**
   * The writer that simply returns its input.
   * @tparam A
   * @return
   */
  def idWriter[A] = new AsyncWriter[A, A] {
    def makeFuture(a:A) = FutureProcesses.pure(a)
  }

  // Covariant Functor instance for Writer
  // if you have trouble getting this implicit resolved, see:
  // https://github.com/scalaz/scalaz/issues/976
  implicit def WriterContravariant[O]: Contravariant[({type l[A]=AsyncWriter[A,O]})#l] =
    new Contravariant[({type l[A]=AsyncWriter[A,O]})#l] {
      def contramap[A, B](r: AsyncWriter[A, O])(f: B => A): AsyncWriter[B, O] =
        new AsyncWriter[B, O] {
          def makeFuture(b: B): ListenableFuture[O] = r.makeFuture(f(b))
        }
    }
}

trait AsyncWriter[A,B] extends ProcessRunner[A,B] {

  def makeFuture(a:A): ListenableFuture[B]

  def runner(p:Process[Task, A]): Process[Task, B] =
    runnerProcess(p) through FutureProcesses.futureChannel

  def runnerProcess(p:Process[Task, A]): Process[Task, ListenableFuture[B]] = {
    val writerChannel: Channel[Task, A, ListenableFuture[B]] =
      channel lift (a => Task(makeFuture(a)))
    /*
     * TODO:
     * The buffer all here is extremely important for efficiency.
     * However, it could be a big problem if p never ends.
     * Maybe it would be better to buffer in batches.
     * For example, `buffer(1024)` instead of `bufferAll`
     * The Kinesis API and documentation should be read before making
     * a change here.
     */
    (p through writerChannel).bufferAll
  }

  // TODO: go through history and bring this all back.
  // This was the basis of the old AsyncWriter/KinesisWriter.
  // It would be useful to bring back.
  //
  //  /**
  //   * Given some i, produce a (potentially) asynchronous computation
  //   * that produces a Throwable \/ O. It is the callers responsibility
  //   * to catch any exception during the execution of that computation,
  //   * and propagate it back as a -\/
  //   */
  //  def asyncTask(i: I): Task[Throwable \/ O]
  //
  //  /**
  //   * A scalaz.concurrent.Task that runs asynchronously
  //   * invoking the future to write to Kinesis.
  //   * @param i
  //   * @return
  //   */
  //  def asyncTask(i: I): Task[Result] =
  //    Task.async { (cb: (Throwable \/ (Result)) => Unit) =>
  //      Futures.addCallback(writeToKinesis(i), new FutureCallback[Result]() {
  //        def onSuccess(result: Result) = cb(result.right)
  //        def onFailure(t: Throwable) = cb(t.left.right)
  //      }, e)
  //    }
  //
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
