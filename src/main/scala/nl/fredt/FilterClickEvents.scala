package nl.fredt

import grizzled.slf4j.Logging
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class FilterClickEvents extends RichFlatMapFunction[ClickEvent, ClickEvent] with Logging with AnsiColors {
  override def flatMap(value: ClickEvent, out: Collector[ClickEvent]): Unit = {

    value.clickedItem match {
      case "A" =>
        info(s"@${getRuntimeContext.getTaskNameWithSubtasks}\t${CYAN}Collecting $value$RESET")
        out.collect(value)

      case "B" =>
        info(s"@${getRuntimeContext.getTaskNameWithSubtasks}\t${CYAN}Collecting $value$RESET")
        out.collect(value)

      case "C" =>
        info(s"@${getRuntimeContext.getTaskNameWithSubtasks}\t${YELLOW}Dropping $value$RESET")
    }
  }
}
