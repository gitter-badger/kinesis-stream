package com.localytics.kinesis

import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{channel => _, _}
import scalaz.syntax.either._
import Process._

object Writer {

  /**
   * The writer that simply returns its input.
   * @tparam A
   * @return
   */
  def idWriter[A] = new Writer[A, A] { self =>
    def asyncTask(i: => A): Task[Throwable \/ A] = Task.delay(i.right)
  }

  // Covariant Functor instance for Writer
  // TODO: why isn't this implicit being resolved?
  implicit def WriterContravariant[O]: Contravariant[Writer[?,O]] =
    new Contravariant[Writer[?, O]] {
      def contramap[A, B](r: Writer[A, O])(f: B => A): Writer[B, O] =
        new Writer[B, O] {
          def asyncTask(b: => B): Task[Throwable \/ O] = r.asyncTask(f(b))
        }
    }
}

/**
 * Writer is now just a very thin wrapper over scalaz-stream
 * If you can produce a Task[O] from an I, then you get a few
 * nice little functions for building processes and channels.
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
  def asyncTask(i: => I): Task[Throwable \/ O]

  /**
   * Run the input through the writers process,
   * collecting the results.
   * @param p
   * @return
   */
  def run(p: Process0[I]): Seq[Throwable \/ O] = process(p).runLog.run

  /**
   *
   * @param p
   * @return
   */
  def runV(p: Process0[Throwable \/ I]): Seq[Throwable \/ O] =
    processV(p).runLog.run

  /**
   * Run the input through the writers asyncProcess,
   * discarding the results.
   * @param p
   * @return
   */
  def writeV(p: Process0[Throwable \/ I]): Unit = processV(p).run.run

  /**
   *
   * @param p
   * @return
   */
  def write(p: Process0[I]): Unit = process(p).run.run

  /**
   * Given some Is, return an 'asynchronous' Process producing Os.
   * See asyncChannel and asyncTask for details.
   * @param p
   * @return
   */
  def process(p: Process0[I]): Process[Task, Throwable \/ O] =
    processV(p.map(_.right))

  /**
   *
   * @param p
   * @return
   */
  def processV(p: Process0[Throwable \/ I]): Process[Task, Throwable \/ O] =
    p through channel

  /**
   * An asynchronous Channel running in Task, producing Os from Is.
   * The tasks don't wait for operations to complete.
   * @return
   */
  def channel: Channel[Task, Throwable \/ I, Throwable \/ O] =
    scalaz.stream.channel.lift(v => Task(v) flatMap {
      case t@ -\/(_) => Task(t)
      case \/-(i) => asyncTask(i) // TODO: might be able to catch errors here!
    })

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
