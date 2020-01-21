package org.broadinstitute.monster.common

import upack._

package object msg {

  /**
    * Extension methods for pulling fields out of upack Msgs.
    *
    * These should all be bridges to methods defined in `MsgTransformations`.
    */
  implicit class MsgOps(val msg: Msg) extends AnyVal {

    def extract[V: MsgParser](fieldChain: String*): V =
      MsgExtractors.extract(msg, fieldChain)

    def tryExtract[V: MsgParser](fieldChain: String*): Option[V] =
      MsgExtractors.tryExtract(msg, fieldChain)
  }
}
