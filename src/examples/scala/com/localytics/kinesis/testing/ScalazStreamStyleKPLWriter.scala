package com.localytics.kinesis.testing

import com.localytics.kinesis.{KinesisInputRecord, KinesisWriter}
import scalaz.stream.Process
import Common._

/**
 * Writes 100k records to Common.kplTestStream
 */
object ScalazStreamStyleKPLWriter {
  def main(args: Array[String]): Unit = withProducer { p =>
    val prefix = args.headOption.getOrElse("test")
    val words = (1 to 100000).map(i => s"$prefix$i")
    new KinesisWriter(p).fireAndObserveWith(
      Process(words.map(s => KinesisInputRecord(kplTestStream, s, s)):_*),
      (_,_) => (), (i,e) =>  println("error: " + i + " " + e)
    )
  }
}
