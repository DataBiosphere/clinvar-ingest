package org.broadinstitute.monster.clinvar.parsers

import java.time.LocalDate

import org.broadinstitute.monster.clinvar.jadeschema.struct.Xref
import org.broadinstitute.monster.clinvar.jadeschema.table.{
  ClinicalAssertionTrait,
  ClinicalAssertionTraitSet,
  Trait,
  TraitMapping
}
import org.broadinstitute.monster.common.msg.JsonParser
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SCVTraitSetSpec extends AnyFlatSpec with Matchers with OptionValues {
  import org.broadinstitute.monster.common.msg.MsgOps

  behavior of "SCVTraitSet.Parser"

  val date = LocalDate.now()
  val context = SCVTraitSet.ParsingContext(Nil, Nil)
  val setId = "imma-cool-set"

  it should "parse a minimal TraitSet from an SCV" in {
    val rawSet = JsonParser.parseEncodedJson(
      s"""{
         |  "Trait": [],
         |  "@Type": "foo"
         |}""".stripMargin
    )
    val parser = SCVTraitSet.parser(date, (_, _) => ???)

    parser.parse(context, setId, rawSet) shouldBe SCVTraitSet(
      ClinicalAssertionTraitSet(setId, date, Nil, Some("foo"), None),
      Nil
    )
  }

  it should "retain unmodeled content from raw TraitSets" in {
    val rawSet = JsonParser.parseEncodedJson(
      s"""{
         |  "Trait": [],
         |  "@Type": "foo",
         |  "Wot": { "Wat": "hello" }
         |}""".stripMargin
    )
    val parser = SCVTraitSet.parser(date, (_, _) => ???)

    parser.parse(context, setId, rawSet) shouldBe SCVTraitSet(
      ClinicalAssertionTraitSet(setId, date, Nil, Some("foo"), Some("""{"Wot":{"Wat":"hello"}}""")),
      Nil
    )
  }

  it should "use the set ID to assign IDs to nested traits" in {
    val rawSet = JsonParser.parseEncodedJson(
      s"""{
         |  "Trait": [{ "blarg": "3" }, { "blarg": "2" }, { "blarg": "1" }],
         |  "@Type": "foo"
         |}""".stripMargin
    )
    val parser = SCVTraitSet.parser(
      date,
      (id, raw) =>
        TraitMetadata(id, date, None, raw.tryExtract[String]("blarg"), Nil, None, Set.empty)
    )

    parser.parse(context, setId, rawSet) shouldBe SCVTraitSet(
      ClinicalAssertionTraitSet(
        setId,
        date,
        List.tabulate(3)(i => s"$setId.$i"),
        Some("foo"),
        None
      ),
      List.tabulate(3) { i =>
        ClinicalAssertionTrait(
          id = s"$setId.$i",
          releaseDate = date,
          `type` = None,
          name = Some((3 - i).toString),
          alternateNames = Nil,
          traitId = None,
          medgenId = None,
          content = None,
          xrefs = Nil
        )
      }
    )
  }

  it should "link SCV traits to RCV traits based on direct MedGen matches" in {
    val metadata = TraitMetadata("foo", date, None, None, Nil, Some("medgen!"), Set.empty)
    val traits = List(
      Trait.init("bar", date).copy(medgenId = Some("medgen?")),
      Trait.init("baz", date).copy(medgenId = Some("medgen!"))
    )

    SCVTraitSet.findMatchingTrait(metadata, traits, Nil).value shouldBe traits(1)
  }

  it should "link SCV traits to RCV traits based on direct XRef matches" in {
    val xref = Xref("foo", "bar", None, None, None)
    val metadata =
      TraitMetadata("foo", date, None, None, Nil, None, Set(xref, xref.copy(id = "foo2")))
    val traits = List(
      Trait.init("bar", date).copy(xrefs = List(xref.copy(id = "baz"), xref)),
      Trait.init("baz", date).copy(xrefs = List(xref.copy(id = "qux"), xref.copy(db = "ddddddd")))
    )

    SCVTraitSet.findMatchingTrait(metadata, traits, Nil).value shouldBe traits.head
  }

  it should "link SCV traits to RCV traits based on preferred-name mappings" in {
    val traitType = "type"
    val metadata =
      TraitMetadata("foo", date, Some(traitType), Some("medgen!"), Nil, None, Set.empty)
    val mappings = List(
      // Name value doesn't match.
      TraitMapping
        .init("id", traitType, "Name", "Preferred", "medgen?", date)
        .copy(medgenId = Some("idd")),
      // Mapping type doesn't match.
      TraitMapping
        .init("di", traitType, "Name", "Alternate", "medgen!", date)
        .copy(medgenId = Some("dii")),
      // Trait type doesn't match.
      TraitMapping
        .init("woo", "other-type", "Name", "Preferred", "medgen!", date)
        .copy(medgenId = Some("idd")),
      // Success!
      TraitMapping
        .init("woo", traitType, "Name", "Preferred", "medgen!", date)
        .copy(medgenId = Some("woooo"))
    )
    val traits = List(
      Trait.init("1", date).copy(medgenId = Some("dii")),
      Trait.init("2", date).copy(medgenId = Some("woooo")),
      Trait.init("3", date).copy(medgenId = Some("idd"))
    )

    SCVTraitSet.findMatchingTrait(metadata, traits, mappings).value shouldBe traits(1)
  }

  it should "link SCV traits to RCV traits based on alternate-name mappings" in {
    val traitType = "type"
    val metadata =
      TraitMetadata("foo", date, Some(traitType), Some("medgen!"), List("medgen?"), None, Set.empty)
    val mappings = List(
      // Mapping type doesn't match.
      TraitMapping
        .init("id", traitType, "Name", "Preferred", "medgen?", date)
        .copy(medgenId = Some("idd")),
      // Trait type doesn't match.
      TraitMapping
        .init("woo", "other-type", "Name", "Preferred", "medgen!", date)
        .copy(medgenId = Some("idd")),
      // Success!.
      TraitMapping
        .init("di", traitType, "Name", "Alternate", "medgen?", date)
        .copy(medgenId = Some("dii"))
    )
    val traits = List(
      Trait.init("1", date).copy(medgenId = Some("dii")),
      Trait.init("2", date).copy(medgenId = Some("woooo")),
      Trait.init("3", date).copy(medgenId = Some("idd"))
    )

    SCVTraitSet.findMatchingTrait(metadata, traits, mappings).value shouldBe traits.head
  }

  it should "link SCV traits to RCV traits based on XRef mappings" in {
    val traitType = "type"
    val xref = Xref("foo", "bar", None, None, None)
    val metadata = TraitMetadata(
      "foo",
      date,
      Some(traitType),
      None,
      Nil,
      None,
      Set(xref.copy(db = "other"), xref, xref.copy(id = "other"))
    )
    val mappings = List(
      TraitMapping
        .init("id", traitType, "XRef", xref.db, "nope", date)
        .copy(medgenId = Some("woooo")),
      TraitMapping
        .init("woo", "other-type", "XRef", "nope", xref.id, date)
        .copy(medgenId = Some("dii")),
      TraitMapping
        .init("di", traitType, "XRef", xref.db, xref.id, date)
        .copy(medgenId = Some("idd")),
      TraitMapping
        .init("di", "other-type", "XRef", xref.db, xref.id, date)
        .copy(medgenId = Some("dii"))
    )
    val traits = List(
      Trait.init("1", date).copy(medgenId = Some("dii")),
      Trait.init("2", date).copy(medgenId = Some("woooo")),
      Trait.init("3", date).copy(medgenId = Some("idd"))
    )

    SCVTraitSet.findMatchingTrait(metadata, traits, mappings).value shouldBe traits(2)
  }

  it should "link SCV traits to RCV traits by name if link has no medgen ID" in {
    val traitType = "type"
    val xref = Xref("foo", "bar", None, None, None)
    val metadata = TraitMetadata(
      "foo",
      date,
      Some(traitType),
      None,
      Nil,
      None,
      Set(xref.copy(db = "other"), xref, xref.copy(id = "other"))
    )
    val mappings = List(
      TraitMapping
        .init("id", traitType, "XRef", xref.db, "nope", date)
        .copy(medgenId = Some("woooo")),
      TraitMapping
        .init("woo", "other-type", "XRef", "nope", xref.id, date)
        .copy(medgenId = Some("dii")),
      TraitMapping
        .init("di", traitType, "XRef", xref.db, xref.id, date)
        .copy(medgenName = Some("fallback")),
      TraitMapping
        .init("di", "other-type", "XRef", xref.db, xref.id, date)
        .copy(medgenId = Some("dii"))
    )
    val traits = List(
      Trait.init("1", date).copy(medgenId = Some("dii")),
      Trait.init("2", date).copy(medgenId = Some("woooo")),
      Trait.init("3", date).copy(medgenId = Some("idd"), name = Some("fallback"))
    )

    SCVTraitSet.findMatchingTrait(metadata, traits, mappings).value shouldBe traits(2)
  }
}
