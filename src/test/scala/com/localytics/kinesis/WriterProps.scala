package com.localytics.kinesis

import com.google.common.util.concurrent.{MoreExecutors => M, ListeningExecutorService, ListenableFuture}
import org.scalacheck.Properties
import org.scalacheck.Prop._
import Writer._

import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.{-\/, \/, \/-}
import scalaz.syntax.either._

object WriterProps extends Properties("Writer") {

  // TODO: make generator for Process[A]
  implicit class RichList[A](l:List[A]) {
    def toProc: Process[Nothing, A] = Process(l:_*)
  }

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
    val writer = idWriter.contramap[List[Char]](_.mkString)
    val actual: Seq[Throwable \/ String] =
      writer.run(strings.map(_.toList).toProc)
    val expected = mkRight(strings)
    actual == expected
  }

  property("will not catch exceptions in async tasks") =
    forAll { (dataPoints: List[Int]) => dataPoints.nonEmpty ==> {
      try { badWriter[Int].run(dataPoints.toProc); false }
      catch { case e: StringException => true }
    }}

  property("handle streams with some good data and some errors") =
    forAll { (dataPoints: List[Int]) =>
      def go(i:Int) = if (even(i)) i else throw new IntException(i)
      val p = Process(dataPoints:_*).map(a => Task.Try(go(a)))
      val results = idWriter[Int].runV(p)
      all(
        results.forall { _.fold({case IntException(i) => odd(i)}, even) }
       ,results.size == dataPoints.size
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
    new Writer[A, A] {
      def asyncTask(a: => A): Task[Throwable \/ A] =
        Task.delay(throw new StringException(a.toString))
    }

  def stringCharWriter =
    new Writer[String, List[Char]] { self =>
      def asyncTask(a: => String): Task[Throwable \/ List[Char]] =
        Task.delay(a.toList.right)
    }
}

