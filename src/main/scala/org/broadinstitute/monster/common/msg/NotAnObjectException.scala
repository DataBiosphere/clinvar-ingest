package org.broadinstitute.monster.common.msg

import upack.Msg

/** Exception thrown when we attempt to extract fields from a non-object. */
class NotAnObjectException(fieldChain: Seq[String], msg: Msg) extends Exception {

  override def getMessage: String =
    s"Encountered non-object when trying to extract field chain '${fieldChain.mkString(",")}' from: $msg"
}
