package com.localytics.kinesis

import java.util.concurrent.ExecutorService

import com.google.common.util.concurrent.MoreExecutors
import org.scalacheck.Properties
import org.scalacheck.Prop._
import Writer._
import scalaz.{\/-, \/}

/**
 * Created by jcough on 7/19/15.
 */
object WriterProps extends Properties("Writer") {

  implicit val e: ExecutorService = MoreExecutors.newDirectExecutorService()

  property("identity writer") = forAll { (strings: List[String]) =>
    val actual = idWriter.collect(strings).filter(_.isRight)
    val result = strings.map(\/-(_))
    actual == result
  }

  property("contramap writer") = forAll { (strings: List[String]) =>
    val chars  = strings.map(_.toList)
    val writer = idWriter.contramap[List[Char]](_.mkString)
    val actual: Seq[Throwable \/ String] = writer.collect(strings.map(_.toList))
    val result = strings.map(\/-(_))
    actual == result
  }
}