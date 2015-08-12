package com.localytics.kinesis.testing

import java.util.concurrent.Executors

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.producer.{KinesisProducerConfiguration, KinesisProducer}

object Common {

  val kplTestStream = "kpl-testing"

  implicit val e = Executors.newFixedThreadPool(1)

  val credentialsProvider = new ProfileCredentialsProvider()

  val kinesisProducer: KinesisProducer = {
    val conf = new KinesisProducerConfiguration
    conf.setCredentialsProvider(credentialsProvider)
    conf.setRegion("us-east-1")
    conf.setAggregationEnabled(true)
    new KinesisProducer(conf)
  }

  def withProducer(f: KinesisProducer => Unit): Unit = {
    println("writing")
    f(kinesisProducer)
    kinesisProducer.flushSync()
    println("done writing")
  }

}
