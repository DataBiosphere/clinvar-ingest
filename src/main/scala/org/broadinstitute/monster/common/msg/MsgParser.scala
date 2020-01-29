package org.broadinstitute.monster.common.msg

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, OffsetDateTime}

import upack._

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Typeclass which specifies how to parse upack messages into a target type.
  *
  * @tparam V type produced by the parser
  */
trait MsgParser[V] {
  /**
    * Unwrap a message and convert its contents into an instance of the target type.
    *
    * @throws ConversionMismatchException if the input is not compatible with the target
    */
  def parse(msg: Msg): V
}

object MsgParser {

  /**
    * Attempt to convert a upack string into another type by delegating to a parsing
    * function, converting any raised exceptions into a common form.
    *
    * @tparam V type produced by the parsing function
    * @param typ name of `V`, to show in error messages
    * @param str message to parse
    * @param parse parsing function to attempt running on `str`
    */
  private def parseString[V](typ: String, str: Str)(parse: String => V): V =
    try {
      parse(str.str)
    } catch {
      case NonFatal(e) => throw new ConversionMismatchException(typ, str, Some(e))
    }

  implicit val msgParser: MsgParser[Msg] = identity[Msg]

  implicit val stringParser: MsgParser[String] = {
    case Str(s) => s
    // TODO: Should we support converting all scalar Msgs to strings,
    // and just .toString them?
    case other => throw new ConversionMismatchException("String", other)
  }

  implicit val boolParser: MsgParser[Boolean] = {
    case Bool(b)  => b
    case str: Str => parseString("Boolean", str)(_.toBoolean)
    case other    => throw new ConversionMismatchException("Boolean", other)
  }

  implicit val longParser: MsgParser[Long] = {
    case Int64(l)  => l
    case UInt64(l) => l
    case Int32(i)  => i.toLong
    case str: Str =>
      parseString("Long", str) { s =>
        val decimalIdx = s.indexOf('.')
        if (decimalIdx >= 0) {
          /*
           * Some annoying datasets will have columns of values that
           * all end with some variation of '.0'
           * It breaks the implementation of .toLong, so we have to
           * strip it off, but only if it's actually all zeros.
           */
          val (whole, decimal) = (s.take(decimalIdx), s.drop(decimalIdx + 1))
          if (decimal.toSet[Char] == Set('0')) {
            whole.toLong
          } else {
            throw new NumberFormatException(s"Not a whole number: $s")
          }
        } else {
          s.toLong
        }
      }
    case msg @ Float64(d) =>
      if (d.isWhole()) {
        d.toLong
      } else {
        throw new ConversionMismatchException("Long", msg)
      }
    case msg @ Float32(f) =>
      if (f.isWhole()) {
        f.toLong
      } else {
        throw new ConversionMismatchException("Long", msg)
      }
    case other => throw new ConversionMismatchException("Long", other)
  }

  implicit val doubleParser: MsgParser[Double] = {
    case Float64(d) => d
    case Float32(f) => f.toDouble
    case Int64(l)   => l.toDouble
    case UInt64(l)  => l.toDouble
    case Int32(i)   => i.toDouble
    case str: Str   => parseString("Double", str)(_.toDouble)
    case other      => throw new ConversionMismatchException("Double", other)
  }

  implicit val dateParser: MsgParser[LocalDate] = new MsgParser[LocalDate] {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-M-d")

    override def parse(msg: Msg): LocalDate = msg match {
      case str: Str => parseString("LocalDate", str)(LocalDate.parse(_, formatter))
      case other    => throw new ConversionMismatchException("LocalDate", other)
    }
  }

  implicit val timestampParser: MsgParser[OffsetDateTime] = {
    case str: Str => parseString("OffsetDateTime", str)(OffsetDateTime.parse(_))
    case other    => throw new ConversionMismatchException("OffsetDateTime", other)
  }

  implicit def arrBufParser[V](
    implicit converter: MsgParser[V]
  ): MsgParser[mutable.ArrayBuffer[V]] = {
    case Arr(elts) => elts.map(converter.parse)
    case scalar    => mutable.ArrayBuffer(converter.parse(scalar))
  }

  implicit def arrayParser[V: MsgParser: ClassTag]: MsgParser[Array[V]] =
    arrBufParser[V].parse(_).toArray
}
