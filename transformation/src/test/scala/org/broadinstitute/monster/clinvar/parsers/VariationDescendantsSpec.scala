package org.broadinstitute.monster.clinvar.parsers

import org.broadinstitute.monster.clinvar.Constants
import org.broadinstitute.monster.common.msg.JsonParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariationDescendantsSpec extends AnyFlatSpec with Matchers {
  import org.broadinstitute.monster.common.msg.MsgOps

  behavior of "VariationDescendants"

  it should "parse variation subtypes from a wrapper object" in {
    val examples = Constants.VariationTypes.map { subtype =>
      subtype -> JsonParser.parseEncodedJson(s"""{ "$subtype": { "id": "$subtype" } }""")
    }

    examples.foreach {
      case (subtype, example) =>
        val parsed = VariationDescendants.fromVariationWrapper(example) { (_, variation) =>
          variation.read[String]("id") -> Nil
        }
        parsed.childIds shouldBe List(subtype)
        parsed.descendantIds shouldBe Nil
    }
  }

  it should "parse all available subtypes from a wrapper object" in {
    val example = JsonParser.parseEncodedJson {
      val fields = Constants.VariationTypes.map { subtype =>
        s""""$subtype": { "id": "$subtype" }"""
      }
      s"""{ ${fields.mkString(",")} }"""
    }

    val parsed = VariationDescendants.fromVariationWrapper(example) { (_, variation) =>
      variation.read[String]("id") -> Nil
    }
    parsed.childIds.toSet shouldBe Constants.VariationTypes
    parsed.descendantIds shouldBe Nil
  }

  it should "collect deeper descendant IDs" in {
    def descendants(subtype: String) =
      subtype.take(5).permutations.toList

    val example = JsonParser.parseEncodedJson {
      val fields = Constants.VariationTypes.map { subtype =>
        s""""$subtype": { "id": "$subtype" }"""
      }
      s"""{ ${fields.mkString(",")} }"""
    }

    val parsed = VariationDescendants.fromVariationWrapper(example) { (subtype, variation) =>
      variation.read[String]("id") -> descendants(subtype)
    }
    parsed.childIds.toSet shouldBe Constants.VariationTypes
    parsed.descendantIds should contain theSameElementsAs
      Constants.VariationTypes.flatMap(descendants)
  }
}
