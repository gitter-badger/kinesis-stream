package com.localytics.kinesis

import org.scalacheck.Properties
import org.scalacheck.Prop._
import AsyncWriter._

import scalaz.concurrent.Task
import scalaz.stream.{sink, Process}
import scalaz.{-\/, \/, \/-}
import scalaz.syntax.either._
import scalaz.syntax.contravariant._

object WriterProps extends Properties("Writer") {

  // TODO: make generator for Process[A]
  implicit class RichList[A](l:List[A]) {
    def toProc: Process[Nothing, A] = Process(l:_*)
  }

  // the first few tests here are dead simple.
  // the remainder are well documented.

  property("simple example") = secure {
    val helloWorld = "Hello, world.".split(' ')
    val result = stringCharWriter.run(Process(helloWorld:_*))
    val expected = mkRight(helloWorld.map(_.toList))
    all(result.size == 2, result == expected)
  }

  property("gracefully handle writing empty logs") = secure {
    idWriter[Int].run(Process()).isEmpty
  }

  property("identity writer") = forAll { (strings: List[String]) =>
    val actual = idWriter.run(strings.toProc).filter(_.isRight)
    val expected = mkRight(strings)
    actual == expected
  }

  property("contramap writer") = forAll { (strings: List[String]) =>
    // if we dont explicitly declare this type,
    // the implicit for contravariant won't get picked up :(
    val w: AsyncWriter[String, String] = idWriter[String]
    val writer = w.contramap[List[Char]](_.mkString)
    val actual: Seq[Throwable \/ String] =
      writer.run(strings.map(_.toList).toProc)
    val expected = mkRight(strings)
    actual == expected
  }

  // this test demonstrates that you must catch your own
  // exceptions in any input that you provide to the writer.
  // this could change in the future.
  property("will not catch exceptions in async tasks") =
    forAll { (dataPoints: List[Int]) => dataPoints.nonEmpty ==> {
      // badWriter always throws exceptions, so if we get
      // beyond the call to run, something is wrong.
      // we should get an exception, and everything is good.
      try { badWriter[Int].run(dataPoints.toProc); false }
      catch { case e: StringException => true }
    }}

  property("handle streams with some good data and some errors") =
    forAll { (dataPoints: List[Int]) =>
      // each dataPoint will be randomly either even or odd.
      // go bombs on odd numbers, but returns evens unscathed.
      def go(i:Int) = if (even(i)) i else throw new IntException(i)
      // create a proc from the datapoints, explicitly
      // catching any errors and turning them into lefts
      val p = dataPoints.toProc.map(a => Task.Try(go(a)))
      // pump p into the idWriter.
      // roughly half of the results should be Throwables,
      // and the other half Ints
      val results: Seq[Throwable \/ Int] = idWriter[Int].runV(p)
      all(
        // make sure all the exceptions contain odd numbers,
        // and all the rest are even numbers.
        results.forall { _.fold({case IntException(i) => odd(i)}, even) }
        // check that all the datapoints really went through the process
       ,results.size == dataPoints.size
      )
    }

  property("test writer observers") = forAll { (dataPoints: List[Int]) =>
    def go(i:Int) = if (even(i)) i else throw new IntException(i)
    val p = dataPoints.toProc.map(a => Task.Try(go(a)))

    // here we create some Sinks to simulate effects.
    // in the real world, one of these sinks might write
    // to a file, or a database.
    var numErrors = 0
    var numInts   = 0
    def errorObserver  (y:Throwable \/ Int): Task[Unit] =
      Task(if(y.isLeft) numErrors += 1)
    def successObserver(y:Throwable \/ Int): Task[Unit] =
      Task(if(y.isRight) numInts += 1)
    val errorSink = sink lift errorObserver
    val successSink = sink lift successObserver

    // create the process, and have it observed
    // both of the syncs above.
    val results = (p through idWriter[Int].channelV).
      observe(errorSink).
      observe(successSink).runLog.run

    val (errs,ints) = results.partition(_.isLeft)
    all(
      numErrors    == errs.size,
      numInts      == ints.size,
      results.size == dataPoints.size
    )
  }

  case class IntException(i:Int) extends java.lang.Exception {
    override def getMessage = i.toString
  }

  case class StringException(s:String) extends java.lang.Exception {
    override def getMessage = s
  }

  def even(i: Int) = i % 2 == 0
  def odd (i: Int) = i % 2 != 0

  def all(l:Boolean*): Boolean = l.forall((b:Boolean) => b)
  def mkRight[A](as:Seq[A]) = as.map(\/-(_))
  def mkLeft [A](as:Seq[A]) = as.map(-\/(_))

  def badWriter[A] =
    new AsyncWriter[A, A] {
      def asyncTask(a: => A): Task[Throwable \/ A] =
        Task.delay(throw new StringException(a.toString))
    }

  def stringCharWriter =
    new AsyncWriter[String, List[Char]] { self =>
      def asyncTask(a: => String): Task[Throwable \/ List[Char]] =
        Task.delay(a.toList.right)
    }
}

