package org.broadinstitute.monster.common.msg

import java.time.{LocalDate, LocalDateTime, OffsetDateTime, ZoneOffset}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import upack._

import scala.collection.mutable.ArrayBuffer

class MsgParserSpec extends AnyFlatSpec with Matchers {
  behavior of "MsgParser"

  /** Testing utility to make sure implicit machinery is working. */
  private def parserFor[V](implicit parser: MsgParser[V]) = parser

  it should "parse strings from string messages" in {
    parserFor[String].parse(Str("a value")) shouldBe "a value"
  }
  it should "fail to parse strings from non-string messages" in {
    List(Int32(1), Int64(1L), Float32(1f), Float64(1d), Bool(true), Arr(), Obj()).foreach {
      msg =>
        a[ConversionMismatchException] shouldBe thrownBy {
          parserFor[String].parse(msg)
        }
    }
  }

  it should "parse bools from bool messages" in {
    parserFor[Boolean].parse(Bool(true)) shouldBe true
    parserFor[Boolean].parse(Bool(false)) shouldBe false
  }
  it should "parse bools from 'true' and 'false' string messages" in {
    parserFor[Boolean].parse(Str("true")) shouldBe true
    parserFor[Boolean].parse(Str("false")) shouldBe false
  }
  it should "fail to parse bools from random string messages" in {
    a[ConversionMismatchException] shouldBe thrownBy {
      parserFor[Boolean].parse(Str("maybe"))
    }
  }
  it should "fail to parse bools from other messages" in {
    List(Int32(1), Int64(1L), Float32(1f), Float64(1d), Arr(), Obj()).foreach { msg =>
      a[ConversionMismatchException] shouldBe thrownBy {
        parserFor[Boolean].parse(msg)
      }
    }
  }

  it should "parse longs from long messages" in {
    parserFor[Long].parse(Int64(1L)) shouldBe 1L
    parserFor[Long].parse(UInt64(1L)) shouldBe 1L
  }
  it should "parse longs from int messages" in {
    parserFor[Long].parse(Int32(1)) shouldBe 1L
  }
  it should "parse longs from whole double messages" in {
    parserFor[Long].parse(Float64(1.0d)) shouldBe 1L
  }
  it should "fail to parse longs from un-whole double messages" in {
    a[ConversionMismatchException] shouldBe thrownBy {
      parserFor[Long].parse(Float64(1.1d))
    }
  }
  it should "parse longs from whole float messages" in {
    parserFor[Long].parse(Float32(1.0f)) shouldBe 1L
  }
  it should "fail to parse longs from un-whole float messages" in {
    a[ConversionMismatchException] shouldBe thrownBy {
      parserFor[Long].parse(Float32(1.1f))
    }
  }
  it should "parse longs from stringified long messages" in {
    parserFor[Long].parse(Str("12345")) shouldBe 12345L
  }
  it should "parse longs from stringified whole double messages" in {
    parserFor[Long].parse(Str("98765.0")) shouldBe 98765L
    parserFor[Long].parse(Str("0000000.000000000")) shouldBe 0L
  }
  it should "fail to parse longs from stringified un-whole double messages" in {
    List("123.45", "0.00001", "999.090").foreach { s =>
      a[ConversionMismatchException] shouldBe thrownBy {
        parserFor[Long].parse(Str(s))
      }
    }
  }
  it should "fail to parse longs from random string messages" in {
    a[ConversionMismatchException] shouldBe thrownBy {
      parserFor[Long].parse(Str("numero uno"))
    }
  }
  it should "fail to parse longs from other messages" in {
    List(Bool(true), Arr(), Obj()).foreach { msg =>
      a[ConversionMismatchException] shouldBe thrownBy {
        parserFor[Long].parse(msg)
      }
    }
  }

  it should "parse doubles from double messages" in {
    parserFor[Double].parse(Float64(56.789d)) shouldBe 56.789d
  }
  it should "parse doubles from float messages" in {
    parserFor[Double].parse(Float32(56.321f)) shouldBe 56.321f
  }
  it should "parse doubles from long messages" in {
    parserFor[Double].parse(Int64(9990L)) shouldBe 9990d
  }
  it should "parse doubles from int messages" in {
    parserFor[Double].parse(Int32(-12345)) shouldBe -12345d
  }
  it should "parse doubles from stringified double messages" in {
    parserFor[Double].parse(Str("34.781")) shouldBe 34.781d
    parserFor[Double].parse(Str("-0.001")) shouldBe -0.001d
    parserFor[Double].parse(Str("6766")) shouldBe 6766d
  }
  it should "fail to parse doubles from random string messages" in {
    a[ConversionMismatchException] shouldBe thrownBy {
      parserFor[Double].parse(Str("woot"))
    }
  }
  it should "fail to parse doubles from other messages" in {
    List(Bool(true), Arr(), Obj()).foreach { msg =>
      a[ConversionMismatchException] shouldBe thrownBy {
        parserFor[Double].parse(msg)
      }
    }
  }

  it should "parse dates from string messages" in {
    parserFor[LocalDate].parse(Str("2020-01-06")) shouldBe LocalDate.of(2020, 1, 6)
    parserFor[LocalDate].parse(Str("2020-1-06")) shouldBe LocalDate.of(2020, 1, 6)
    parserFor[LocalDate].parse(Str("2020-01-6")) shouldBe LocalDate.of(2020, 1, 6)
    parserFor[LocalDate].parse(Str("2020-1-6")) shouldBe LocalDate.of(2020, 1, 6)
  }
  it should "fail to parse dates with an invalid value" in {
    List("2020-123-6", "2020-01-400", "100-5-6").foreach { s =>
      a[ConversionMismatchException] shouldBe thrownBy {
        parserFor[LocalDate].parse(Str(s))
      }
    }
  }
  it should "fail to parse dates from random string messages" in {
    a[ConversionMismatchException] shouldBe thrownBy {
      parserFor[LocalDate].parse(Str("some day"))
    }
  }
  it should "fail to parse dates from other messages" in {
    List(Int32(1), Int64(1L), Float32(1f), Float64(1d), Bool(true), Arr(), Obj()).foreach {
      msg =>
        a[ConversionMismatchException] shouldBe thrownBy {
          parserFor[LocalDate].parse(msg)
        }
    }
  }

  it should "parse timestamps from string messages" in {
    parserFor[OffsetDateTime].parse(Str("2020-05-09T12:00:50.000000123Z")) shouldBe OffsetDateTime
      .of(
        LocalDateTime.of(2020, 5, 9, 12, 0, 50, 123),
        ZoneOffset.UTC
      )
    parserFor[OffsetDateTime].parse(Str("2020-05-09T12:00:50Z")) shouldBe OffsetDateTime
      .of(
        LocalDateTime.of(2020, 5, 9, 12, 0, 50, 0),
        ZoneOffset.UTC
      )
  }
  it should "fail to parse timestamps with an invalid value" in {
    List("2020-05-09", "2020-01-400T12:00").foreach { s =>
      a[ConversionMismatchException] shouldBe thrownBy {
        parserFor[OffsetDateTime].parse(Str(s))
      }
    }
  }
  it should "fail to parse timestamps from random string messages" in {
    a[ConversionMismatchException] shouldBe thrownBy {
      parserFor[OffsetDateTime].parse(Str("some point in time"))
    }
  }
  it should "fail to parse timestamps from other messages" in {
    List(Int32(1), Int64(1L), Float32(1f), Float64(1d), Bool(true), Arr(), Obj()).foreach {
      msg =>
        a[ConversionMismatchException] shouldBe thrownBy {
          parserFor[LocalDate].parse(msg)
        }
    }
  }

  it should "parse buffers from array messages" in {
    parserFor[ArrayBuffer[String]]
      .parse(Arr(Str("foo"), Str("bar"), Str("baz"))) shouldBe
      ArrayBuffer("foo", "bar", "baz")

    parserFor[ArrayBuffer[Long]]
      .parse(Arr(Int64(1L), UInt64(-2L), Int32(5))) shouldBe
      ArrayBuffer(1L, -2L, 5L)

    parserFor[ArrayBuffer[Boolean]].parse(Arr()) shouldBe ArrayBuffer.empty[Boolean]
  }
  it should "parse buffers from singleton messages" in {
    parserFor[ArrayBuffer[Double]].parse(Float64(9.8765d)) shouldBe ArrayBuffer(9.8765d)
    parserFor[ArrayBuffer[Boolean]].parse(Bool(false)) shouldBe ArrayBuffer(false)
    parserFor[ArrayBuffer[Msg]].parse(Obj()) shouldBe ArrayBuffer(Obj())
  }

  it should "parse arrays from array messages" in {
    parserFor[Array[String]].parse(Arr(Str("foo"), Str("bar"), Str("baz"))) shouldBe
      Array("foo", "bar", "baz")

    parserFor[Array[Long]].parse(Arr(Int64(1L), UInt64(-2L), Int32(5))) shouldBe
      Array(1L, -2L, 5L)

    parserFor[Array[Boolean]].parse(Arr()) shouldBe Array.empty[Boolean]
  }
  it should "parse arrays from singleton messages" in {
    parserFor[Array[Double]].parse(Float64(9.8765d)) shouldBe Array(9.8765d)
    parserFor[Array[Boolean]].parse(Bool(false)) shouldBe Array(false)
  }
  it should "fail to parse arrays from objects" in {
    a[ConversionMismatchException] shouldBe thrownBy {
      parserFor[Array[String]].parse(Obj())
    }
  }
}
