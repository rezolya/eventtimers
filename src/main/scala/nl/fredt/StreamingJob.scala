package nl.fredt

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import java.util.Properties

import grizzled.slf4j.Logging
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand



object StreamingJob extends Logging with AnsiColors {
  def main(args: Array[String]) {
    val ts = (System.currentTimeMillis() /10000) * 10000

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setMaxParallelism(10000)
    env.setParallelism(2)
    env.getConfig.disableSysoutLogging()

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "musasabi-kafka:9092")
    kafkaProps.put("group.id", "test-group")
    kafkaProps.put("flink.poll-timeout", "10")
    kafkaProps.put("max.poll.records", "50")

    import scala.collection.JavaConverters._
    val inStream = env.addSource(new FlinkKafkaConsumer010[ClickEvent](Seq("TEST", "fraudtestinputtopic").asJava, new TestSerializer, kafkaProps))

    val timedInStream: DataStream[ClickEvent] = inStream.assignTimestampsAndWatermarks(new PunctAssigner).name("TIMED")

    val filteredStream: DataStream[ClickEvent] = timedInStream.flatMap(new FilterClickEvents).name("FILTERED")

    val keyedStream = filteredStream.keyBy(ce => ce.customer)

    val outStream = keyedStream.process(new ProcessClickEvents).name("PROCESS")

    outStream.print()
    // execute program
    info(s"$MAGENTA$env.getExecutionPlan $RESET")

    env.execute("Flink Streaming Scala API Skeleton")
  }
}






