package com.localytics.kinesis

import com.google.common.util.concurrent.ListenableFuture
import org.scalacheck.Properties
import org.scalacheck.Prop._

import scalaz.concurrent.Task
import scalaz.stream.{sink, Process}
import scalaz.{-\/, \/, \/-}
import scalaz.syntax.either._
import scalaz.syntax.contravariant._
import AsyncWriter._

object WriterProps extends Properties("Writer") {

  def stringCharWriter =
    new AsyncWriter[String, List[Char]] { self =>
      def makeFuture(a: String): ListenableFuture[List[Char]] =
        FutureProcesses.pure(a.toList)
    }


  // TODO: make generator for Process[A]
  implicit class RichList[A](l:List[A]) {
    def toProc: Process[Nothing, A] = Process(l:_*)
  }

  // the first few tests here are dead simple.
  // the remainder are well documented.

  property("simple example") = secure {
    val helloWorld = "Hello, world.".split(' ')
    val result = stringCharWriter.fireAndGet(Process(helloWorld:_*))
    val expected = helloWorld.map(_.toList).toSeq
    all(result.size == 2, result == expected)
  }

  property("gracefully handle writing empty logs") = secure {
    idWriter[Int].fireAndGet(Process()).isEmpty
  }

  property("identity writer") = forAll { (strings: List[String]) =>
    val actual = idWriter.fireAndGet(strings.toProc)
    val expected = strings
    actual == expected
  }

  property("contramap writer") = forAll { (strings: List[String]) =>
    // if we dont explicitly declare this type,
    // the implicit for contravariant won't get picked up :(
    val w: AsyncWriter[String, String] = idWriter[String]
    val writer = w.contramap[List[Char]](_.mkString)
    val actual: Seq[String] = writer.fireAndGet(strings.map(_.toList).toProc)
    val expected = strings
    actual == expected
  }


  // TODO: bring runV back as fireAndGetV, which catches exceptions.
  /*
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
    val p: Process[Task, Throwable \/ Int] =
      dataPoints.toProc.map(a => Task.Try(go(a)))

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
    */

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
}

