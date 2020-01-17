package com.oreilly.learningsparkexamples.mini.scala.chapter10

/**
  * information container,like JavaBean
  */
case class ApacheAccessLog(
                            ipAddress: String,
                            clientIdentd: String,
                            userId: String,
                            dateTime: String,
                            method: String,
                            endpoint: String,
                            protocol: String,
                            responseCode: Int,
                            contentSize: Long) {

}

/**
  * Retrieve information from log line using Regular Expression
  */
object ApacheAccessLog extends Serializable {
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  def parseLogLine(log: String): ApacheAccessLog = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      return null
    }
    val m = res.get
    ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
      m.group(5), m.group(6), m.group(7), m.group(8).toInt, m.group(9).toLong)
  }

  def main(args: Array[String]) {
    val line = """192.13.212.25 - - [04/Aug/2014:15:18:27 +0800] "GET /abc/ HTTP/1.1" 200 280"""
    val log = ApacheAccessLog.parseLogLine(line);
    println(log.ipAddress)
    println(log.clientIdentd)
    println(log.userId)
    println(log.dateTime)
    println(log.method)
    println(log.endpoint)
    println(log.protocol)
    println(log.responseCode)
    println(log.contentSize)

  }
}
