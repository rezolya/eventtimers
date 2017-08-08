package nl.fredt

import grizzled.slf4j.Logging
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class ProcessClickEvents extends ProcessFunction[ClickEvent, ClickEvent] with Logging with AnsiColors {
  var prevClickEvents : ListState[ClickEvent] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val prevClickEventsStateDescriptor = new ListStateDescriptor[ClickEvent]("prevClickEvents", classOf[ClickEvent])
    prevClickEvents = getRuntimeContext.getListState[ClickEvent](prevClickEventsStateDescriptor)
  }
  override def processElement(i: ClickEvent, context: ProcessFunction[ClickEvent, ClickEvent]#Context, collector: Collector[ClickEvent]): Unit = {
    import scala.collection.JavaConverters._
    val eventTimerAt = context.timestamp() + ClickEventInputSource.getDelayInMs
    info(s"@${getRuntimeContext.getTaskNameWithSubtasks}\t${RED}processElement $i registerEventTimeTimer($BLUE${eventTimerAt}$RED) $RED ${prevClickEvents.get().asScala.mkString(",")}$RESET")
    prevClickEvents.add(i)
    context.timerService().registerEventTimeTimer(eventTimerAt)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[ClickEvent, ClickEvent]#OnTimerContext, out: Collector[ClickEvent]): Unit = {
    import scala.collection.JavaConverters._
    info(s"@${getRuntimeContext.getTaskNameWithSubtasks}\t${MAGENTA}onTimer $BLUE$timestamp$MAGENTA Collecting ${prevClickEvents.get().asScala.mkString(",")} $RESET")

    prevClickEvents.get().asScala.foreach{clickEvent =>
      out.collect(clickEvent)
    }
    prevClickEvents.clear()
  }
}
