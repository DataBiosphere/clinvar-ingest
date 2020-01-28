package org.broadinstitute.monster.clinvar.parsers

import org.broadinstitute.monster.clinvar.Constants
import upack.Msg

/**
  * TODO
  *
  * @param childIds TODO
  * @param descendantIds TODO
  */
case class VariationDescendants(
  childIds: List[String],
  descendantIds: List[String]
)

object VariationDescendants {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * TODO
    *
    * @param variationWrapper TODO
    * @param processChild TODO
    */
  def fromVariationWrapper(variationWrapper: Msg)(
    processChild: (String, Msg) => (String, List[String])
  ): VariationDescendants = {
    val zero = VariationDescendants(Nil, Nil)
    Constants.VariationTypes.foldLeft(zero) {
      case (VariationDescendants(childAcc, descendantsAcc), subtype) =>
        val descendants = variationWrapper.tryExtract[Array[Msg]](subtype).fold(zero) {
          _.foldLeft(zero) {
            case (VariationDescendants(childAcc, descendantsAcc), child) =>
              val (childId, descendantIds) = processChild(subtype, child)
              VariationDescendants(
                childId :: childAcc,
                descendantIds ::: descendantsAcc
              )
          }
        }
        VariationDescendants(
          descendants.childIds ::: childAcc,
          descendants.descendantIds ::: descendantsAcc
        )
    }
  }
}
