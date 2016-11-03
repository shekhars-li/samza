/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.samza.system

import java.util
import java.util.{Collections, Random}

import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.config.MapConfig
import org.apache.samza.container.SamzaContainerMetrics
import org.apache.samza.container.TaskInstance
import org.apache.samza.container.TaskInstanceMetrics
import org.apache.samza.container.TaskName
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.chooser.DefaultChooser
import org.apache.samza.task.AsyncRunLoop
import org.apache.samza.task.AsyncStreamTaskAdapter
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.TaskCoordinator.RequestScope
import org.junit.{Before, Test}
import org.apache.samza.config.MetricsConfig.Config2Metrics
import scala.collection.JavaConversions._
import org.apache.samza.util.Util.asScalaClock

class TestSystemConsumersPerformance {

  val PARTITION_COUNT: Int = 30
  val BATCH_SIZE: Int = 200
  val ssps = new util.HashSet[SystemStreamPartition]()
  val messages = new util.HashMap[SystemStreamPartition, util.List[IncomingMessageEnvelope]]

  class MockSystemConsumer extends SystemConsumer {
    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def register(systemStreamPartition: SystemStreamPartition, offset: String): Unit = {}

    override def poll(systemStreamPartitions: util.Set[SystemStreamPartition], timeout: Long): util.Map[SystemStreamPartition, util.List[IncomingMessageEnvelope]] = {
      val result = new util.HashMap[SystemStreamPartition, util.List[IncomingMessageEnvelope]]()
      systemStreamPartitions.foreach { ssp =>
        result.put(ssp, messages.get(ssp))
      }

      result
    }
  }

  class MockSystemFactory extends SystemFactory {

    override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry): SystemConsumer = new MockSystemConsumer

    override def getAdmin(systemName: String, config: Config): SystemAdmin = new SystemAdmin {

      override def createCoordinatorStream(streamName: String): Unit = {}

      override def getOffsetsAfter(offsets: util.Map[SystemStreamPartition, String]): util.Map[SystemStreamPartition, String] = offsets

      override def validateChangelogStream(streamName: String, numOfPartitions: Int): Unit = {}

      override def createChangelogStream(streamName: String, numOfPartitions: Int): Unit = {}

      override def getSystemStreamMetadata(streamNames: util.Set[String]): util.Map[String, SystemStreamMetadata] = {
        val partitionMetaData = List.range(1, PARTITION_COUNT + 1).map(
          new Partition(_) -> new SystemStreamPartitionMetadata("0", "0", "0")
        ).toMap

        Map("input" -> new SystemStreamMetadata("input", partitionMetaData) )
      }

      override def offsetComparator(offset1: String, offset2: String): Integer = offset1.compareTo(offset2)
    }

    override def getProducer(systemName: String, config: Config, registry: MetricsRegistry): SystemProducer = null
  }

  class CountingTask extends StreamTask {
    override def process(envelope: IncomingMessageEnvelope,
                         collector: MessageCollector,
                         coordinator: TaskCoordinator): Unit = {
    }
  }

  @Before
  def setup = {
    1 to PARTITION_COUNT foreach { i =>
      ssps.add(new SystemStreamPartition("input", "stream", new Partition(i)))
    }

    ssps.foreach{ ssp =>
      val msgs = new util.LinkedList[IncomingMessageEnvelope]
      messages.put(ssp, msgs)
      val rand = new Random
      1 to BATCH_SIZE foreach { i =>
        val content = rand.nextLong() + ":" + rand.nextLong()
        msgs.add(new IncomingMessageEnvelope(ssp, i.toString, null, content));
      }
    }
  }

  //@Test
  def testChoose: Unit = {
    System.out.print("partitions " + ssps.size())

    val map: Map[String, String] = Map(
      "metrics.timer.enabled" -> "false"
    )
    val config = new MapConfig(map)
    val chooser = new DefaultChooser
    val consumer = new MockSystemConsumer
    val multiplexer = new SystemConsumers(chooser = chooser,
                                          consumers = Map("input" -> consumer),
                                          clock = config.getMetricsTimerClock)

    ssps.foreach {ssp =>
      multiplexer.register(ssp, "0")
    }

    multiplexer.start

    val times = 10000000
    val start = System.nanoTime()
    1 to times foreach { i =>
      val m = multiplexer.choose(false)
      if(m != null) {
        multiplexer.tryUpdate(m.getSystemStreamPartition)
      } else {
        //System.out.println("null message: " + i)
      }
    }
    val end = System.nanoTime()
    System.out.print("avg choose ns " + (end - start).toDouble / times)
  }

  @Test
  def testRun: Unit = {
    val map: Map[String, String] = Map(
      "metrics.timer.enabled" -> "false"
    )
    val config = new MapConfig(map)
    val chooser = new DefaultChooser
    val systemFactory = new MockSystemFactory
    val multiplexer = new SystemConsumers(chooser = chooser,
                                          consumers = Map("input" -> systemFactory.getConsumer("input", config, null)),
                                          clock = config.getMetricsTimerClock)
    ssps.foreach {ssp =>
      multiplexer.register(ssp, "0")
    }

    multiplexer.start


    val tasks = ssps.map{ ssp =>
      val taskName = new TaskName(ssp.getPartition.toString)
      val taskInstance = new TaskInstance(
        task = new AsyncStreamTaskAdapter(new CountingTask, null),
        taskName = new TaskName(ssp.getPartition.toString),
        config = config,
        metrics = new TaskInstanceMetrics(),
        systemAdmins = null,//Map("input" -> systemFactory.getAdmin("input", config)),
        consumerMultiplexer = multiplexer,
        collector = null,
        containerContext = null,
        systemStreamPartitions = Set(ssp))
      (taskName -> taskInstance)
    }.toMap

    val runLoop = new AsyncRunLoop(
      tasks,
      null,
      multiplexer,
      1,
      -1L,
      -1L,
      -1L,
      -1L,
      new SamzaContainerMetrics(),
      config.getMetricsTimerClock
    )

    val times = 10000000
    val rand = new Random

    val messages = ssps.map { ssp =>
      val id = ssp.getPartition.getPartitionId
      val content = rand.nextLong() + ":" + rand.nextLong()
      (id -> new IncomingMessageEnvelope(ssp, "0", null, content))
    }.toMap

    val start = System.nanoTime()
    1 to times foreach{ i =>
      var message = messages(i % 30 + 1)
      runLoop.runTasks(message)

      //runLoop.blockIfBusy(message)

    }
    val end = System.nanoTime()
    System.out.print("avg run ns " + (end - start).toDouble / times)
  }

  //@Test
  def testArrayDeque = {
    val times = 10000000
    val q = new util.ArrayDeque[Int]()

    val start = System.nanoTime()
    1 to times foreach{ i =>
      add(q, i)
      remove(q)
    }

    val end = System.nanoTime()
    System.out.print("avg arraydeque ns " + (end - start).toDouble / times)
  }

  def add(q: util.ArrayDeque[Int], i: Int): Unit = {
    q.add(i)
  }

  def remove(q: util.ArrayDeque[Int]) : Int = {
    if(!q.isEmpty) q.remove
    else -1
  }

}
