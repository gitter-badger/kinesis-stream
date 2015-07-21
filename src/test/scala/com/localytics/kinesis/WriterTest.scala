package com.localytics.kinesis

import java.util.concurrent.Executors
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import scalaz.{\/, \/-}

class WriterTest extends FlatSpec with MockitoSugar with Matchers {

  import Writer._

  implicit val executorService = Executors.newSingleThreadExecutor()

  def writer = new Writer[String, List[Char]] { self =>
    def eval(s:String) = executorChannel(s, executorService)(_.toList)
  }

  behavior of "A Writer"

  it should "should write records normally, asynchronously" in {
    val hello = "Hello, world.".split(' ').toList
    val l = writer.process(hello).runLog.run
    l.size should be(2)
    val expected: Seq[Throwable \/ List[Char]] =
      Seq(
        \/-(List('H', 'e', 'l', 'l', 'o', ',')),
        \/-(List('w', 'o', 'r', 'l', 'd', '.'))
      )
    l should be(expected)
  }

  it should "gracefully handle writing empty logs" in {
    writer.process(List()).runLog.run should be(Seq())
  }
}
