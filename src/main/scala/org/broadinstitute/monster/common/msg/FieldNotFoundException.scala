package org.broadinstitute.monster.common.msg

import upack.Msg

/** Exception thrown when a required field is missing from a message payload. */
class FieldNotFoundException(fieldChain: Seq[String], msg: Msg) extends Exception {

  override def getMessage: String =
    s"No value found for chain '${fieldChain.mkString(",")}' in message: $msg"
}
