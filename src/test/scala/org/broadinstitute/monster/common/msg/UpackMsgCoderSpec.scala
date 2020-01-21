package org.broadinstitute.monster.common.msg

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import upack._

class UpackMsgCoderSpec extends AnyFlatSpec with Matchers {
  behavior of "UpackMsgCoder"

  private val coder = new UpackMsgCoder

  it should "round-trip (de)serialize objects" in {
    val msg = Obj(
      Str("foo") -> Int32(1234),
      Str("bar") -> Str("baz"),
      Str("qux") -> Int64(1234567890000000000L),
      Str("bippy") -> Arr(
        Obj(
          Str("bar") -> Str("baz"),
          Str("qux") -> Float64(123.4567890)
        ),
        Str("nan")
      ),
      Str("done?") -> Bool(true)
    )

    val out = new ByteArrayOutputStream()
    coder.encode(msg, out)
    val decoded = coder.decode(new ByteArrayInputStream(out.toByteArray))

    decoded shouldBe msg
  }

  it should "drop null when encoding" in {
    val out = new ByteArrayOutputStream()
    coder.encode(null, out)
    out.toByteArray shouldBe empty
  }
}
