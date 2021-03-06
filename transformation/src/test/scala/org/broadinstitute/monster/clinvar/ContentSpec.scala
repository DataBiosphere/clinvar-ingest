package org.broadinstitute.monster.clinvar

import org.broadinstitute.monster.common.msg.JsonParser
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import upack._

class ContentSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "Content"

  it should "drop empty objects" in {
    Content.encode(Obj()) shouldBe None
  }

  it should "canonicalize data during encoding" in {
    val unsorted = JsonParser.parseEncodedJson(
      s"""{
         |  "o-no-some-long-key": [10, 12, 8, 3],
         |  "eep": "asdfadsfdasfadsfdasdas",
         |  "nested": {
         |    "beepboop": [true, false],
         |    "wat": 1234.01
         |  },
         |  "many-objs": [
         |    { "object1": "should-go-second", "zzzzzz": "sleepy" },
         |    { "short": "object" },
         |    { "object2": "should-go-first", "aaaaaa": "awake" }
         |  ]
         |}""".stripMargin
    )
    val sorted =
      s"""{
         |  "eep": "asdfadsfdasfadsfdasdas",
         |  "many-objs": [
         |    { "short": "object" },
         |    { "aaaaaa": "awake", "object2": "should-go-first" },
         |    { "object1": "should-go-second", "zzzzzz": "sleepy" }
         |  ],
         |  "nested": {
         |    "beepboop": [false, true],
         |    "wat": 1234.01
         |  },
         |  "o-no-some-long-key": [3, 8, 10, 12]
         |}""".stripMargin

    Content.encode(unsorted).value shouldBe sorted.replaceAll("\\s", "")
  }
}
