package com.localytics.kinesis.testing

import java.nio.ByteBuffer
import com.amazonaws.kinesis.producer.UserRecordResult
import com.google.common.util.concurrent.{FutureCallback, Futures}

import Common._

/**
 * Writes 100k records to Common.kplTestStream
 */
object JavaStyleKPLWriter {
  def main(args: Array[String]): Unit = withProducer { p =>
    val prefix = args.headOption.getOrElse("test")
    val words = (1 to 100000).map(i => s"$prefix$i")
    words.foreach { w =>
      val f = p.addUserRecord(kplTestStream, w, ByteBuffer.wrap(w.getBytes))
      Futures.addCallback(f, new FutureCallback[UserRecordResult]() {
        def onSuccess(result: UserRecordResult) = {}
        def onFailure(t: Throwable) = println(t.getMessage)
      }, e)
    }
  }
}

