package nl.fredt

case class ClickEvent(customer: String, clickedItem: String, eventtime: Long)

object ClickEventInputSource {
  def getDelayInMs : Long = 25
  def getAsSeq(ts:Long): Seq[ClickEvent] = Seq(
    //                                // eventTimer @ | Triggered After Event         | Collected Events
    ClickEvent("a", "A", ts + 0),     //     25       | ClickEvent("r", "A", ts + 30) |
    ClickEvent("b", "A", ts + 10),    //     35       | ClickEvent("q", "B", ts + 40) |
    ClickEvent("c", "A", ts + 20),    //     45       | ClickEvent("c", "B", ts + 50) |
    ClickEvent("r", "A", ts + 30),    //     55       | ClickEvent("c", "B", ts + 60) |
    ClickEvent("q", "B", ts + 40),    //     65       | ClickEvent("r", "B", ts + 70) |
    ClickEvent("c", "B", ts + 50),    //     75       | ClickEvent("q", "C", ts + 80) |
    ClickEvent("c", "B", ts + 60),    //     85       | ClickEvent("c", "C", ts + 90) |
    ClickEvent("r", "B", ts + 70),    //     95       | ClickEvent("c", "C", ts + 100)|
    ClickEvent("q", "C", ts + 80),    //              | ClickEvent("r", "C", ts + 110)|
    ClickEvent("c", "C", ts + 90),    //              | |
    ClickEvent("c", "C", ts + 100),   //              | |
    ClickEvent("c", "C", ts + 110)//,   //              | |
//    ClickEvent("c", "C", ts + 120),   //              | |
//    ClickEvent("c", "C", ts + 130),   //              | |
//    ClickEvent("c", "C", ts + 140),   //              | |
//    ClickEvent("c", "C", ts + 150),   //              | |
//    ClickEvent("r", "C", ts + 160)    //              | |
  )
}
