package nl.fredt

import grizzled.slf4j.Logging
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class PunctAssigner extends AssignerWithPunctuatedWatermarks[ClickEvent] with Logging with AnsiColors {
  override def checkAndGetNextWatermark(t: ClickEvent, l: Long): Watermark = {
    info(s"@checkAndGetNextWatermark($t, $l)\t${GREEN}Create WM ${t.eventtime}$RESET")
    new Watermark(t.eventtime)
  }

  override def extractTimestamp(t: ClickEvent, l: Long): Long = {
    info(s"@extractTimestamp($t, $l)\t${YELLOW}extracting eventtime ${t.eventtime}$RESET")
    t.eventtime
  }
}
