package org.broadinstitute.monster.common

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import io.circe.{Encoder, Json, JsonNumber, Printer}
import io.circe.syntax._
import ujson.{AstTransformer, StringRenderer}
import upack.Msg
import upickle.core.Visitor

import scala.collection.mutable.ArrayBuffer

/** Utilities for reading/writing messages from/to storage. */
object StorageIO {

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
    * JSON formatter used when writing outputs.
    *
    * Attempts to make our outputs as compact as possible by dropping whitespace and
    * null values.
    */
  val circePrinter: Printer = Printer.noSpaces.copy(dropNullValues = true)

  /**
    * Read JSON-list files matching a glob pattern into a scio collection
    * for processing.
    *
    * Each line of each input file must contain a full message object, or else
    * downstream processing will break.
    *
    * TODO: Can we enforce object-ness here, so things explode at the earliest
    * possible point?
    */
  def readJsonLists(
    context: ScioContext,
    description: String,
    filePattern: String
  )(implicit coder: Coder[Msg]): SCollection[Msg] =
    context
      .textFile(filePattern)
      .transform(s"Parse Messages: $description")(
        _.map { line =>
          val maybeParsed = io.circe.parser.parse(line)
          maybeParsed.fold(
            err => throw new Exception(s"Failed to parse input line as JSON: $line", err),
            js => circeVisitor.transform(js, Msg)
          )
        }
      )

  /**
    * Write unmodeled messages to storage for use by downstream components.
    *
    * Each message will be written to a single line in a part-file under the given
    * output prefix.
    */
  def writeJsonLists(
    messages: SCollection[Msg],
    description: String,
    outputPrefix: String
  ): ClosedTap[String] =
    messages
      .transform(s"Stringify Messages: $description")(
        _.map(upack.transform(_, StringRenderer()).toString)
      )
      .saveAsTextFile(outputPrefix, suffix = ".json")

  /**
    * Write modeled messages to storage for use by downstream components.
    *
    * Each message will be written to a single line in a part-file under the given
    * output prefix.
    */
  def writeJsonLists[M: Encoder](
    messages: SCollection[M],
    description: String,
    outputPrefix: String
  ): ClosedTap[String] =
    messages
      .transform(s"Stringify Messages: $description")(
        _.map(_.asJson.printWith(circePrinter))
      )
      .saveAsTextFile(outputPrefix, suffix = ".json")
}
