package org.broadinstitute.monster.clinvar.parsers

import org.broadinstitute.monster.clinvar.Constants
import upack.Msg

/**
  * Wrapper for our representation of variation ancestry.
  *
  * @param childIds IDs of the immediate children of a variation
  * @param descendantIds IDs for every descendant (including children) of a variation
  */
case class VariationDescendants(
  childIds: List[String],
  descendantIds: List[String]
)

object VariationDescendants {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Extract child and descendant IDs of the variation contained in a wrapper payload.
    *
    * @param variationWrapper raw payload containing at least one variation element
    * @param processChild function to extract the (ID, child IDs) from any raw variations
    *                     found while descending the tree
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
