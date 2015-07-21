package com.localytics.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.Executor

import com.amazonaws.kinesis.producer.{UserRecordResult, KinesisProducer}
import com.google.common.util.concurrent.MoreExecutors
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class KinesisWriterTest extends FlatSpec with MockitoSugar with Matchers {

  implicit val e: Executor = MoreExecutors.directExecutor

  def writer(k:KinesisProducer) = new KinesisWriter[String] { self =>
    val kinesisProducer = k
    def toInputRecord(s:String) = KinesisInputRecord(s,s,ByteBuffer.wrap(s.getBytes))
  }

  behavior of "A Kinesis Writer"

/*  it should "should write records normally, asynchronously" in {
    val k = mock[KinesisProducer]
    val w = writer(k)
    val hello = "Hello, world.".split(' ').toList
    val l = w.process(hello).runLog.run
    l.size should be(2)
    l should be(Seq(
      List('H', 'e', 'l', 'l', 'o', ','),
      List('w', 'o', 'r', 'l', 'd', '.')
    ))
  }*/

  it should "gracefully handle writing empty logs" in {
    val k = mock[KinesisProducer]
    val w = writer(k)
    w.process(List()).runLog.run should be(Seq())
  }
}
