package org.broadinstitute.monster.common.msg

import java.time.LocalDate

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import upack._

class MsgExtractorsSpec extends AnyFlatSpec with Matchers {
  behavior of "MsgExtractors"

  it should "extract a field directly" in {
    val msg = Obj(Str("key") -> Int64(9000L), Str("key2") -> Str("other"))
    MsgExtractors.extract[Long](msg, List("key")) shouldBe 9000L
    msg shouldBe Obj(Str("key2") -> Str("other"))
  }
  it should "extract a field by following a chain of fields" in {
    val msg = Obj(
      Str("key1") -> Obj(Str("key2") -> Arr(Str("foo"), Str("bar"))),
      Str("key2") -> Bool(true)
    )
    MsgExtractors
      .extract[Array[String]](msg, List("key1", "key2")) shouldBe Array("foo", "bar")
    msg shouldBe Obj(Str("key2") -> Bool(true))
  }
  it should "fail to extract a missing field" in {
    a[FieldNotFoundException] shouldBe thrownBy {
      MsgExtractors.extract[Boolean](Obj(), List("key"))
    }
  }
  it should "fail to extract a missing field at the end of a chain" in {
    val msg = Obj(
      Str("key1") -> Obj(Str("key2") -> Obj(Str("key3") -> Arr(Str("foo"), Str("bar"))))
    )
    a[FieldNotFoundException] shouldBe thrownBy {
      MsgExtractors.extract[Long](msg, List("key1", "key2", "key4"))
    }
  }
  it should "fail to extract a missing field in the middle of a chain" in {
    val msg = Obj(
      Str("key1") -> Obj(Str("key2") -> Obj(Str("key3") -> Arr(Str("foo"), Str("bar"))))
    )
    a[FieldNotFoundException] shouldBe thrownBy {
      MsgExtractors.extract[Long](msg, List("key1", "key4", "key3"))
    }
  }

  it should "extract an optional field directly" in {
    val msg = Obj(Str("key") -> Int64(9000L), Str("key2") -> Str("other"))
    MsgExtractors.tryExtract[Long](msg, List("key")) shouldBe Some(9000L)
    msg shouldBe Obj(Str("key2") -> Str("other"))
  }
  it should "extract an optional field at the end of a chain" in {
    val msg = Obj(
      Str("key1") -> Obj(Str("key2") -> Str("2020-01-10")),
      Str("key2") -> Bool(true)
    )
    MsgExtractors.tryExtract[LocalDate](msg, List("key1", "key2")) shouldBe
      Some(LocalDate.of(2020, 1, 10))
    msg shouldBe Obj(Str("key2") -> Bool(true))
  }
  it should "handle a missing field in optional extraction" in {
    MsgExtractors.tryExtract[Boolean](Obj(), List("key")) shouldBe None
  }
  it should "handle a missing field at the end of a chain in optional extraction" in {
    val msg = Obj(
      Str("key1") -> Obj(Str("key2") -> Obj(Str("key3") -> Arr(Str("foo"), Str("bar"))))
    )
    MsgExtractors.tryExtract[Long](msg, List("key1", "key2", "key4")) shouldBe None
  }
  it should "handle a missing field in the middle of a chain in optional extraction" in {
    val msg = Obj(
      Str("key1") -> Obj(Str("key2") -> Obj(Str("key3") -> Arr(Str("foo"), Str("bar"))))
    )
    MsgExtractors.tryExtract[Long](msg, List("key1", "key4", "key3")) shouldBe None
  }
}
