package nl.fredt

import grizzled.slf4j.Logging

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
trait AnsiColors extends Logging {
  def BLACK = "\u001B[30m"
  def RED = "\u001B[31m"
  def GREEN = "\u001B[32m"
  def YELLOW = "\u001B[33m"
  def BLUE = "\u001B[34m"
  def MAGENTA = "\u001B[35m"
  def CYAN = "\u001B[36m"
  def WHITE = "\u001B[37m"
  def RESET = "\u001B[0m"
}
