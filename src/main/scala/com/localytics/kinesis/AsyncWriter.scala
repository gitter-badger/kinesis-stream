package com.localytics.kinesis

import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{channel => _, _}
import scalaz.syntax.either._
import Process._

object AsyncWriter {

  /**
   * The writer that simply returns its input.
   * @tparam A
   * @return
   */
  def idWriter[A] = new AsyncWriter[A, A] { self =>
    def asyncTask(i: A): Task[Throwable \/ A] = Task.delay(i.right)
  }

  // Covariant Functor instance for Writer
  // if you have trouble getting this implicit resolved, see:
  // https://github.com/scalaz/scalaz/issues/976
  implicit def WriterContravariant[O]: Contravariant[({type l[A]=AsyncWriter[A,O]})#l] =
    new Contravariant[({type l[A]=AsyncWriter[A,O]})#l] {
      def contramap[A, B](r: AsyncWriter[A, O])(f: B => A): AsyncWriter[B, O] =
        new AsyncWriter[B, O] {
          def asyncTask(b: B): Task[Throwable \/ O] = r.asyncTask(f(b))
        }
    }
}

/**
 * Writer is now just a very thin wrapper over scalaz-stream
 * If you can produce a Task[O] from an I, then you get a few
 * nice little functions for building processes and channels.
 *
 * There are three categories of functions:
 *  - functions that run processes to completion, returning Unit
 *  - functions that run processes to completion, return the results
 *  - functions that return scalaz-stream Channels and Processes
 */
trait AsyncWriter[-I,O] { self =>

  /**
   * Given some i, produce a (potentially) asynchronous computation
   * that produces a Throwable \/ O. It is the callers responsibility
   * to catch any exception during the execution of that computation,
   * and propagate it back as a -\/
   */
  def asyncTask(i: I): Task[Throwable \/ O]

  /**
   * Run the input through this writers process, collecting the results.
   */
  def run(p: Process0[I]): Seq[Throwable \/ O] = process(p).runLog.run

  /**
   * Run the input, which could potentially contain errors,
   * through this writers process. Any errors in the input
   * are passed through to the output unchanged.
   */
  def runV(p: Process0[Throwable \/ I]): Seq[Throwable \/ O] =
    processV(p).runLog.run

  /**
   * Run the input through this writers process, discarding the results.
   */
  def write(p: Process0[I]): Unit = process(p).run.run

  /**
   * Run the input through this writers process, and then pump
   * the results into the given sink.
   */
  def writeToSink(p: Process0[I], s:Sink[Task, Throwable \/ O]): Unit =
    process(p).observe(s).run.run

  /**
   * Run the input through this writers process, and then pump
   * the results into the given sink.
   */
  def writeToSinkV(p: Process0[Throwable \/ I],
                   s:Sink[Task, Throwable \/ O]): Unit =
    processV(p).observe(s).run.run

  /**
   * Run the input through this writers process, discarding the results.
   * Any errors in the input are simply discarded.
   */
  def writeV(p: Process0[Throwable \/ I]): Unit = processV(p).run.run

  /**
   * Pipes the given Process through this writers channel.
   */
  def process(p: Process0[I]): Process[Task, Throwable \/ O] =
    (p: Process[Task, I]) through channel

  /**
   * Pipes the given Process through this writers channel.
   * Any errors in the input are passed through the channel unchanged.
   */
  def processV(p: Process0[Throwable \/ I]): Process[Task, Throwable \/ O] =
    (p: Process[Task, Throwable \/ I]) through channelV

  /**
   * An Channel running producing Os from Is via asyncTask
   */
  def channel: Channel[Task, I, Throwable \/ O] =
    scalaz.stream.channel.lift(asyncTask(_))

  /**
   * An Channel running producing Os from Is via asyncTask.
   * Any errors in the input are passed through the channel unchanged.
   */
  def channelV: Channel[Task, Throwable \/ I, Throwable \/ O] =
    scalaz.stream.channel.lift(v => Task(v) flatMap {
      case t@ -\/(_) => Task(t)
      case \/-(i) => asyncTask(i) // TODO: might be able to catch errors here!
    })
}
