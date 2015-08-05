package com.localytics.kinesis

import scalaz.concurrent.Task
import scalaz.stream._

/**
 * ProcessRunner gives a few nice utility functions for running Processes
 * fireAndGet, fireAndForget, and fireAndObserve all run to completion.
 * @tparam A The
 * @tparam B
 */
trait ProcessRunner[A,B] {

  /**
   * This is the only function you need to implement a ProcessRunner.
   * Given a Process[A], return a Process[B].
   * This could be as simple as calling map, but could do
   * a lot of other things too.
   * @param p
   * @return
   */
  def runner(p:Process[Task, A]): Process[Task, B]

  /**
   * Take the given process or run it through the runner,
   * collect all the results and return them.
   * @param p The input data.
   * @return The result of
   */
  final def fireAndGet(p:Process[Task, A]): Seq[B] = runner(p).runLog.run

  /**
   * Take the given process or run it through the runner,
   * and simply drop the results on the floor.
   * @param p
   */
  final def fireAndForget(p:Process[Task, A]): Unit = runner(p).run.run

  /**
   * Take the given process or run it through the runner,
   * then have the result observed by the given sink.
   * @param p
   * @return
   */
  final def fireAndObserve(p:Process[Task, A], s: Sink[Task, B]): Unit =
    (runner(p) observe s).run.run
}
