package src.main.scala.logging

/**
  * Object: <code>Logging</code>
  *
  * Handles the system default logging threshold, and the logging of messages
  * to a log destination with its method logMsg(...)
  *
  * (Further on, this logging needs to be made more fine-grained as in a
  * system with many different sub-systems: see SLF4J)
  */

object Logging extends Enumeration {
  type Logging = Value

  val EMERGENCY = Value(0)
  val ALERT = Value(1)
  val CRITICAL = Value(2)
  val ERROR = Value(3)
  val WARNING = Value(4)
  val NOTICE = Value(5)
  val INFO = Value(6)
  val DEBUG = Value(7)

  var loggingThreshold: Logging = ERROR

  def logMsg(level: Logging, errMsg: String)
  {
    if (level <= loggingThreshold) {
      System.err.println(level.toString + ": " + errMsg)
    }
  }
}

