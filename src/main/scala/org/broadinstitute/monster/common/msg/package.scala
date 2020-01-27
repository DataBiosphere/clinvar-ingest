package org.broadinstitute.monster.common

import io.circe.{Json, JsonNumber}
import ujson.AstTransformer
import upack._
import upickle.core.Visitor

import scala.collection.mutable.ArrayBuffer

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

  /**
    * Utility class which can map circe's JSON representation to any type with
    * a upickle Visitor implementation.
    *
    * Mostly a copy-paste of ujson-circe's `CirceJson` object, but with the behavior
    * of numeric conversions tweaked to preserve the distinction between longs and doubles.
    *
    * @see https://github.com/lihaoyi/upickle/blob/master/ujson/circe/src/ujson/circe/CirceJson.scala
    */
  val circeVisitor: AstTransformer[Json] = new AstTransformer[Json] {

    override def transform[T](j: Json, f: Visitor[_, T]): T = j.fold(
      f.visitNull(-1),
      if (_) f.visitTrue(-1) else f.visitFalse(-1),
      n => n.toLong.map(f.visitInt64(_, -1)).getOrElse(f.visitFloat64(n.toDouble, -1)),
      f.visitString(_, -1),
      arr => transformArray(f, arr),
      obj => transformObject(f, obj.toList)
    )

    def visitArray(length: Int, index: Int) =
      new AstArrVisitor[Vector](x => Json.arr(x: _*))

    def visitObject(length: Int, index: Int) =
      new AstObjVisitor[ArrayBuffer[(String, Json)]](vs => Json.obj(vs: _*))

    def visitNull(index: Int): Json = Json.Null

    def visitFalse(index: Int): Json = Json.False

    def visitTrue(index: Int): Json = Json.True

    def visitFloat64StringParts(
      s: CharSequence,
      decIndex: Int,
      expIndex: Int,
      index: Int
    ): Json = Json.fromJsonNumber(
      if (decIndex == -1 && expIndex == -1)
        JsonNumber.fromIntegralStringUnsafe(s.toString)
      else JsonNumber.fromDecimalStringUnsafe(s.toString)
    )

    def visitString(s: CharSequence, index: Int): Json = Json.fromString(s.toString)
  }

  /**
    * Parse stringified JSON into a upack Msg.
    *
    * This method uses a custom visitor to better preserve distinctions
    * between numeric types.
    */
  def parseJsonString(json: String): Msg = {
    val maybeParsed = io.circe.parser.parse(json)
    maybeParsed.fold(
      err => throw new Exception(s"Failed to parse input line as JSON: $json", err),
      js => circeVisitor.transform(js, Msg)
    )
  }
}
