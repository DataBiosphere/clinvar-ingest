package org.broadinstitute.monster.common.msg

import cats.data.NonEmptyList
import upack._

/** Helper methods for extracting values from upack Msgs. */
object MsgExtractors {

  /**
    * 'Drill' into a message, following a chain of fields, and return the value
    * at the end of the chain.
    *
    * An exception will be thrown if no value can be extracted. Use `tryExtract`
    * for extracting optional fields.
    */
  def extract[V: MsgParser](msg: Msg, fieldChain: Seq[String]): V =
    tryExtract(msg, fieldChain)
      .getOrElse(throw new FieldNotFoundException(fieldChain, msg))

  /**
    * 'Drill' into a message, following a chain of fields, and return the value
    * at the end of the chain.
    *
    * An exception will be thrown if any non-leaf along the field chain points to
    * a non-object value.
    */
  def tryExtract[V](msg: Msg, fieldChain: Seq[String])(
    implicit converter: MsgParser[V]
  ): Option[V] = {
    // Helper method that uses NonEmptyList for some extra guardrails.
    // NEL is convenient here but a pain to use elsewhere compared to the
    // var-args of the wrapping method.
    def drillDown(msg: Msg, chain: NonEmptyList[String]): Option[Msg] = msg match {
      case Obj(fields) =>
        val firstKey = Str(chain.head)
        NonEmptyList.fromList(chain.tail) match {
          case None =>
            fields.remove(firstKey)
          case Some(remainingFields) =>
            fields.get(firstKey).flatMap { nested =>
              val retVal = drillDown(nested, remainingFields)
              nested match {
                case Obj(nestedFields) if nestedFields.isEmpty =>
                  fields.remove(firstKey)
                  ()
                case _ => ()
              }
              retVal
            }
        }
      case _ => throw new NotAnObjectException(fieldChain.toList, msg)
    }

    NonEmptyList
      .fromList(fieldChain.toList)
      .flatMap(drillDown(msg, _))
      .map(converter.parse)
  }
}
