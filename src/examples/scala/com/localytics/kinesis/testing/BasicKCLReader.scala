package com.localytics.kinesis.testing

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.Executors

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, Worker, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import com.amazonaws.services.kinesis.model.Record
import scala.collection.JavaConverters._
import Common._

/**
 * Reads from Common.kplTestStream
 */
object BasicKCLReader {

  def main(args: Array[String]): Unit = {
    println("running KCL")

    val appName = args.headOption.getOrElse("sample-app")

    java.security.Security.setProperty("networkaddress.cache.ttl", "60")
    val workerId = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID

    new Worker.Builder()
      .recordProcessorFactory(new IRecordProcessorFactory{
        def createProcessor: IRecordProcessor = new IRecordProcessor {
          def initialize (init: InitializationInput): Unit = {}
          def processRecords (pri: ProcessRecordsInput): Unit = {
            def deserialize(r:Record) = new String(r.getData.array())
            pri.getRecords.asScala.foreach(x => println(deserialize(x)))
          }
          def shutdown(shutdownInput: ShutdownInput): Unit = {}
        }
      })
      .config(new KinesisClientLibConfiguration(
        appName, kplTestStream, credentialsProvider, workerId
      )
      .withInitialPositionInStream(InitialPositionInStream.LATEST))
      .execService(Executors.newCachedThreadPool).build()
      .run()
  }
}
