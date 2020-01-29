package org.broadinstitute.monster.common.msg

import upack.Msg

/** Exception thrown when we try to convert a Msg subclass to an incompatible type. */
class ConversionMismatchException(
  expectedType: String,
  msg: Msg,
  cause: Option[Throwable] = None
) extends Exception {

  override def getMessage: String =
    s"Cannot convert message to type '$expectedType': $msg"

  override def getCause: Throwable = cause.orNull
}
