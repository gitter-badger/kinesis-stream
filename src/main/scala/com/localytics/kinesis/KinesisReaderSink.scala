package com.localytics.kinesis

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.{ScheduledExecutorService, Executors}

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ThrottlingException, ShutdownException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessorFactory, IRecordProcessor}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{Worker, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.clientlibrary.types._

import scala.concurrent.duration
import scala.concurrent.duration.Duration
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.syntax.either._

/**
 * This data is passed to the Sink when data becomes avaiable.
 * @param startingSequenceNum
 * @param shardId
 * @param processRecordsInput
 */
case class WorkerRecordsInput(
  startingSequenceNum: Option[ExtendedSequenceNumber],
  shardId: Option[String],
  processRecordsInput: ProcessRecordsInput)

/**
 * Run the KCL, and when any records come in, feed them into the given Sink.
 */
object KCL {

  /**
   * Run the KCL
   * @param s the sink to pass records to when they become available.
   * @param config
   */
  def go(s:Sink[Task,WorkerRecordsInput],
             config: KinesisClientLibConfiguration): Unit = {
    // Ensure the JVM will refresh the cached IP values of AWS resources
    // (e.g. service endpoints).
    java.security.Security.setProperty("networkaddress.cache.ttl", "60")
    val worker = new Worker.Builder()
      .recordProcessorFactory(new IRecordProcessorFactory{
      def createProcessor: IRecordProcessor = new KinesisStreamReader(s)
    }).config(config)
      .execService(Executors.newCachedThreadPool)
      .build()
      .run()
  }

  /**
   * This creates a very simple config similar to the one in Amazon's example:
   * https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonKinesis/AmazonKinesisApplicationSample.java
   * @param appName The name of your application - TODO: what exactly does that mean?
   * @param streamName The Kinesis stream to read data from.
   * @return
   */
  def kinesisClientConfig(appName: String, streamName: String): KinesisClientLibConfiguration = {
    def getCredentials: ProfileCredentialsProvider = {
      val credentialsProvider = new ProfileCredentialsProvider
      // side effecting code makes sure you got cred.
      try credentialsProvider.getCredentials
      catch { case e: Exception =>
        throw new AmazonClientException(
          "Cannot load the credentials from the credential profiles file. "
            + "Please make sure that your credentials file is at the correct "
            + "location (~/.aws/credentials), and is in valid format.", e);
      }
      credentialsProvider
    }
    val workerId =
      InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID
    new KinesisClientLibConfiguration(appName, streamName, getCredentials, workerId)
  }
}

/**
 * KinesisStreamReader gets fed records from Kinesis, and passes them to a Sink.
 * @param s the sink to pass records to when they become available.
 */
case class KinesisStreamReader(s:Sink[Task,WorkerRecordsInput]) extends IRecordProcessor {

  private var startingSequenceNum: Option[ExtendedSequenceNumber] = None
  private var shardId: Option[String] = None

  def initialize (init: InitializationInput): Unit = {
    startingSequenceNum = Some(init.getExtendedSequenceNumber)
    //TODO: this actually seems to be missing from Amazons API...
    //val pendingCheckpointSequenceNumber = 42 //???
    shardId = Some(init.getShardId)
  }

  // just pass everything to the sink, and run it, executing any side effects.
  def processRecords (pri: ProcessRecordsInput): Unit = {
    val p: Process[Task, WorkerRecordsInput] =
      Process(WorkerRecordsInput(startingSequenceNum, shardId, pri))
    (p to s).run.run
  }

  // TODO: back backoff time and number of retries configurable.
  def shutdown(shutdownInput: ShutdownInput): Unit = {
    val BACKOFF_TIME_IN_MILLIS: Long = 3000L
    val NUM_RETRIES: Int = 10
    // stuff from Amazons example:
    // LOG.info("Shutting down record processor for shard: " + kinesisShardId);
    // Important to checkpoint after reaching end of shard,
    // so we can start processing data from child shards.
    if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE)
      new CheckPointer(NUM_RETRIES, BACKOFF_TIME_IN_MILLIS).
        checkpoint(shutdownInput.getCheckpointer)
  }
}

/**
 * @param maxTries
 * @param backoffTimeMillis
 */
class CheckPointer(maxTries: Int, backoffTimeMillis: Long) {

  trait CheckPointStatus
  case object CheckPointSuccess extends CheckPointStatus
  case class  CheckPointFailure(e:Exception) extends CheckPointStatus
  case class  CheckPointThrottled(attemptNumber: Int) extends CheckPointStatus

  def checkpoint(checkpointer: IRecordProcessorCheckpointer): Unit = {

    /**
     *
     * @param checkpointer
     * @param attemptNumber
     * @return
     */
    def attemptCheckpoint(checkpointer: IRecordProcessorCheckpointer,
                          attemptNumber: Int): CheckPointStatus =
      try {
        checkpointer.checkpoint()
        CheckPointSuccess
      } catch {
        case e:ShutdownException     => CheckPointFailure(e)
        case e:InvalidStateException => CheckPointFailure(e)
        case e:ThrottlingException   => CheckPointThrottled(attemptNumber)
      }

    //LOG.info("Checkpointing shard " + kinesisShardId);
    val d = Duration.apply(backoffTimeMillis, duration.MILLISECONDS)
    implicit val e = Executors.newScheduledThreadPool(2)
    val proc = time.awakeEvery(d)
      .zipWithIndex.map(_._2)
      .take(maxTries) // don't retry more than nrRetries times.
      .map(attemptCheckpoint(checkpointer, _))
      .flatMap {
        case CheckPointSuccess => Process.halt
        case CheckPointFailure(e: ShutdownException) =>
          // Ignore checkpoint if the processor instance has been shutdown (fail over).
          //LOG.info("Caught shutdown exception, skipping checkpoint.", se);
          Process.halt
        case CheckPointFailure(e: InvalidStateException) =>
          // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
          //LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
          Process.halt
        case CheckPointThrottled(n) =>
          if (n == maxTries)
            //LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
            Process.halt
          else
            //LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES, e);
            Process.emit(())
      }
    proc.run.run
  }
}

