package org.broadinstitute.monster.clinvar

import ujson.StringRenderer
import upack.Msg

object Content {

  /**
    * Encode unmodeled data so it can be included in a string column.
    *
    * If the data is empty, it will be dropped.
    */
  def encode(msg: Msg): Option[String] =
    if (msg.obj.isEmpty) {
      None
    } else {
      Some(upack.transform(msg, StringRenderer()).toString)
    }
}
