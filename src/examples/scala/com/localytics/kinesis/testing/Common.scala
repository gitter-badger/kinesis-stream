package com.localytics.kinesis.testing

import java.util.concurrent.Executors

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.kinesis.producer.{Configuration, KinesisProducer}

object Common {

  val kplTestStream = "kpl-testing"

  implicit val e = Executors.newFixedThreadPool(1)

  val credentialsProvider = new ProfileCredentialsProvider()

  val kinesisProducer: KinesisProducer = {
    val conf = new Configuration
    conf.setAwsSecretKey(credentialsProvider.getCredentials.getAWSSecretKey)
    conf.setAwsAccessKeyId(credentialsProvider.getCredentials.getAWSAccessKeyId)
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
