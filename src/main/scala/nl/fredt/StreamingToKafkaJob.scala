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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}

import scala.reflect.ClassTag

/**
  * Skeleton for a Flink Streaming Job.
  *
  * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
  * file in the same package/directory or have a look at the website.
  *
  * You can also generate a .jar file that you can submit on your Flink
  * cluster. Just type
  * {{{
  *   mvn clean package
  * }}}
  * in the projects root directory. You will find the jar in
  * target/eventtimers-1.0-SNAPSHOT.jar
  * From the CLI you can then run
  * {{{
  *    ./bin/flink run -c nl.fredt.StreamingJob target/eventtimers-1.0-SNAPSHOT.jar
  * }}}
  *
  * For more information on the CLI see:
  *
  * http://flink.apache.org/docs/latest/apis/cli.html
  */

object StreamingToKafkaJob extends Logging with AnsiColors {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.disableSysoutLogging()

    val ts = (System.currentTimeMillis() /10000) * 10000

    val inStream: DataStream[ClickEvent] = env.fromCollection(ClickEventInputSource.getAsSeq(ts)).name("INPUT")

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "musasabi-kafka:9092")
    kafkaProps.put("client.id", "test-client")

    val x = new FlinkKafkaProducer010[ClickEvent]("TEST", new TestSerializer, kafkaProps)
    inStream.addSink(x)
    // execute program
    info(s"$MAGENTA$env.getExecutionPlan $RESET")

    env.execute("Flink Streaming Scala API Skeleton")
  }
}

class TestSerializer extends KeyedSerializationSchema[ClickEvent] with KeyedDeserializationSchema[ClickEvent] {
  override def serializeKey(t: ClickEvent): Array[Byte] = {
    t.customer.getBytes("utf-8")
  }

  override def getTargetTopic(t: ClickEvent): String = {
    "TEST"
  }

  override def serializeValue(t: ClickEvent): Array[Byte] = {
    println(s"Serializing : $t")
    val testData = s"${t.customer}|${t.clickedItem}|${t.eventtime}"
    testData.getBytes("utf-8")
  }

  override def isEndOfStream(t: ClickEvent): Boolean = false

  override def deserialize(key: Array[Byte], message: Array[Byte], topicName: String, partition: Int, offset: Long): ClickEvent = {
    if (topicName == "TEST") {
      val x = new String(message, "utf-8")
      val splits = x.split('|')
      ClickEvent(splits(0), splits(1), splits(2).toLong)
    } else {
      null
    }
  }

  override def getProducedType: TypeInformation[ClickEvent] = TypeExtractor.getForClass(implicitly[ClassTag[ClickEvent]].runtimeClass.asInstanceOf[Class[ClickEvent]])
}

