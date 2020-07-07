package org.broadinstitute.monster.clinvar.parsers

import java.time.LocalDate

import org.broadinstitute.monster.clinvar.jadeschema.struct.InterpretationComment
import org.broadinstitute.monster.clinvar.jadeschema.table._
import org.broadinstitute.monster.common.msg.JsonParser
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SCVSpec extends AnyFlatSpec with Matchers with OptionValues {
  import org.broadinstitute.monster.common.msg.MsgOps

  behavior of "SCV.Parser"

  val date = LocalDate.now()
  val interpretation = Interpretation(None, None, None, None, None, Nil, Nil)
  val context = SCV.ParsingContext("varId", "vcvId", Nil, interpretation, Map.empty)

  it should "parse top-level SCV fields" in {
    val raw = JsonParser.parseEncodedJson(
      """{
        |  "@ID": "123",
        |  "@SubmissionDate": "4321-09-20",
        |  "ClinVarAccession": {
        |    "@Type": "SCV",
        |    "@Accession": "SCV!",
        |    "@Version": "2",
        |    "@OrgID": "organization",
        |    "@SubmitterName": "name",
        |    "@OrgAbbreviation": "org",
        |    "@OrganizationCategory": "cat"
        |  },
        |  "ClinVarSubmissionID": {
        |    "@title": "ello",
        |    "@localKey": "key",
        |    "@submittedAssembly": "avengers assemble"
        |  },
        |  "Assertion": {
        |    "$": "dis is the best SCV ever"
        |  },
        |  "@DateCreated": "1234-01-01",
        |  "@DateLastUpdated": "9999-12-12",
        |  "RecordStatus": {
        |    "$": "It exists"
        |  },
        |  "ReviewStatus": {
        |    "$": "Not yet"
        |  },
        |  "Interpretation": {
        |    "Description": {
        |      "$": "Hmmmmmmmm"
        |    },
        |    "@DateLastEvaluated": "5678-07-20:11",
        |    "Comment": [
        |      {
        |        "$": "I like this"
        |      },
        |      {
        |        "$": "worst scv evr",
        |        "@Type": "trolling"
        |      }
        |    ]
        |  },
        |  "SubmissionNameList": {
        |    "SubmissionName": [
        |      { "$": "sub1" },
        |      { "$": "sub2" }
        |    ]
        |  }
        |}""".stripMargin
    )
    val parser = SCV.parser(date, (_, _, _) => ???)

    parser.parse(context, raw) shouldBe SCV(
      ClinicalAssertion(
        id = "SCV!",
        releaseDate = date,
        version = 2L,
        internalId = "123",
        variationArchiveId = context.vcvId,
        variationId = context.variationId,
        submitterId = "organization",
        submissionId = "organization.4321-09-20",
        rcvAccessionId = None,
        traitSetId = None,
        clinicalAssertionTraitSetId = None,
        clinicalAssertionObservationIds = Nil,
        title = Some("ello"),
        localKey = Some("key"),
        assertionType = Some("dis is the best SCV ever"),
        dateCreated = Some(LocalDate.of(1234, 1, 1)),
        dateLastUpdated = Some(LocalDate.of(9999, 12, 12)),
        submittedAssembly = Some("avengers assemble"),
        recordStatus = Some("It exists"),
        reviewStatus = Some("Not yet"),
        interpretationDescription = Some("Hmmmmmmmm"),
        interpretationDateLastEvaluated = Some(LocalDate.of(5678, 7, 20)),
        interpretationComments = List(
          InterpretationComment("I like this", None),
          InterpretationComment("worst scv evr", Some("trolling"))
        ),
        submissionNames = List("sub1", "sub2"),
        content = None
      ),
      List(
        Submitter(
          id = "organization",
          releaseDate = date,
          orgCategory = Some("cat"),
          currentName = Some("name"),
          allNames = List("name"),
          currentAbbrev = Some("org"),
          allAbbrevs = List("org")
        )
      ),
      Submission(
        id = "organization.4321-09-20",
        releaseDate = date,
        submitterId = "organization",
        additionalSubmitterIds = Nil,
        submissionDate = LocalDate.of(4321, 9, 20)
      ),
      variations = Nil,
      traitSets = Nil,
      traits = Nil,
      observations = Nil
    )
  }

  it should "parse an SCV with additional submitters" in {
    val raw = JsonParser.parseEncodedJson(
      """{
        |  "@ID": "123",
        |  "@SubmissionDate": "4321-09-20",
        |  "ClinVarAccession": {
        |    "@Type": "SCV",
        |    "@Accession": "SCV!",
        |    "@Version": "2",
        |    "@OrgID": "organization",
        |    "@SubmitterName": "name",
        |    "@OrgAbbreviation": "org",
        |    "@OrganizationCategory": "cat"
        |  },
        |  "AdditionalSubmitters": {
        |    "SubmitterDescription": [
        |      {
        |        "@OrgID": "other",
        |        "@SubmitterName": "me",
        |        "@OrgAbbreviation": "I",
        |        "@OrganizationCategory": "spy"
        |      },
        |      {
        |        "@OrgID": "who dis",
        |        "@SubmitterName": "you",
        |        "@OrgAbbreviation": "u",
        |        "@OrganizationCategory": "magic"
        |      }
        |    ]
        |  }
        |}""".stripMargin
    )
    val parser = SCV.parser(date, (_, _, _) => ???)

    parser.parse(context, raw) shouldBe SCV(
      ClinicalAssertion.init(
        id = "SCV!",
        releaseDate = date,
        version = 2L,
        internalId = "123",
        variationArchiveId = context.vcvId,
        variationId = context.variationId,
        submitterId = "organization",
        submissionId = "organization.4321-09-20"
      ),
      List(
        Submitter(
          id = "organization",
          releaseDate = date,
          orgCategory = Some("cat"),
          currentName = Some("name"),
          allNames = List("name"),
          currentAbbrev = Some("org"),
          allAbbrevs = List("org")
        ),
        Submitter(
          id = "other",
          releaseDate = date,
          orgCategory = Some("spy"),
          currentName = Some("me"),
          allNames = List("me"),
          currentAbbrev = Some("I"),
          allAbbrevs = List("I")
        ),
        Submitter(
          id = "who dis",
          releaseDate = date,
          orgCategory = Some("magic"),
          currentName = Some("you"),
          allNames = List("you"),
          currentAbbrev = Some("u"),
          allAbbrevs = List("u")
        )
      ),
      Submission(
        id = "organization.4321-09-20",
        releaseDate = date,
        submitterId = "organization",
        additionalSubmitterIds = List("other", "who dis"),
        submissionDate = LocalDate.of(4321, 9, 20)
      ),
      variations = Nil,
      traitSets = Nil,
      traits = Nil,
      observations = Nil
    )
  }

  it should "retain unmodeled SCV content" in {
    val raw = JsonParser.parseEncodedJson(
      """{
        |  "@ID": "123",
        |  "@SubmissionDate": "4321-09-20",
        |  "ClinVarAccession": {
        |    "@Type": "SCV",
        |    "@Accession": "SCV!",
        |    "@Version": "2",
        |    "@OrgID": "organization",
        |    "Unexpected!": [
        |      "O",
        |      "noe"
        |    ],
        |    "@SubmitterName": "name",
        |    "@OrgAbbreviation": "org",
        |    "@OrganizationCategory": "cat"
        |  },
        |  "Uh oh": 12345
        |}""".stripMargin
    )
    val parser = SCV.parser(date, (_, _, _) => ???)

    parser.parse(context, raw) shouldBe SCV(
      ClinicalAssertion
        .init(
          id = "SCV!",
          releaseDate = date,
          version = 2L,
          internalId = "123",
          variationArchiveId = context.vcvId,
          variationId = context.variationId,
          submitterId = "organization",
          submissionId = "organization.4321-09-20"
        )
        .copy(content = Some("""{"ClinVarAccession":{"Unexpected!":["O","noe"]},"Uh oh":12345}""")),
      List(
        Submitter(
          id = "organization",
          releaseDate = date,
          orgCategory = Some("cat"),
          currentName = Some("name"),
          allNames = List("name"),
          currentAbbrev = Some("org"),
          allAbbrevs = List("org")
        )
      ),
      Submission(
        id = "organization.4321-09-20",
        releaseDate = date,
        submitterId = "organization",
        additionalSubmitterIds = Nil,
        submissionDate = LocalDate.of(4321, 9, 20)
      ),
      variations = Nil,
      traitSets = Nil,
      traits = Nil,
      observations = Nil
    )
  }

  it should "parse an SCV with variations" in {
    val raw = JsonParser.parseEncodedJson(
      """{
        |  "@ID": "123",
        |  "@SubmissionDate": "4321-09-20",
        |  "ClinVarAccession": {
        |    "@Type": "SCV",
        |    "@Accession": "SCV!",
        |    "@Version": "2",
        |    "@OrgID": "organization",
        |    "@SubmitterName": "name",
        |    "@OrgAbbreviation": "org",
        |    "@OrganizationCategory": "cat"
        |  },
        |  "Genotype": {
        |    "Woo content": 9999,
        |    "VariantType": { "$": "geno" },
        |    "Haplotype": {
        |      "VariationType": { "$": "haplo" },
        |      "SimpleAllele": [
        |        { "VariationType": { "$": "simple" } },
        |        { "VariationType": { "$": "simple" } },
        |        { "VariantType": { "$": "simple" } }
        |      ]
        |    },
        |    "SimpleAllele": { "VariationType": { "$": "simple" } }
        |  },
        |  "Haplotype": [
        |    {
        |      "VariantType": { "$": "haplo" },
        |      "SimpleAllele": { "VariationType": { "$": "simple" } },
        |      "Extre": "stuff"
        |    },
        |    { "VariationType": { "$": "haplo" } }
        |  ]
        |}""".stripMargin
    )
    val parser = SCV.parser(date, (_, _, _) => ???)
    val parsed = parser.parse(context, raw)

    parsed.variations.sortBy(_.id) should contain theSameElementsAs List(
      ClinicalAssertionVariation(
        id = "SCV!.0",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = List("SCV!.1"),
        descendantIds = List("SCV!.1"),
        subclassType = "Haplotype",
        variationType = Some("haplo"),
        content = Some("""{"Extre":"stuff"}""")
      ),
      ClinicalAssertionVariation(
        id = "SCV!.1",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = Nil,
        descendantIds = Nil,
        subclassType = "SimpleAllele",
        variationType = Some("simple"),
        content = None
      ),
      ClinicalAssertionVariation(
        id = "SCV!.2",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = Nil,
        descendantIds = Nil,
        subclassType = "Haplotype",
        variationType = Some("haplo"),
        content = None
      ),
      ClinicalAssertionVariation(
        id = "SCV!.3",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = List("SCV!.4", "SCV!.5"),
        descendantIds = List("SCV!.4", "SCV!.5", "SCV!.6", "SCV!.7", "SCV!.8"),
        subclassType = "Genotype",
        variationType = Some("geno"),
        content = Some("""{"Woo content":9999}""")
      ),
      ClinicalAssertionVariation(
        id = "SCV!.4",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = Nil,
        descendantIds = Nil,
        subclassType = "SimpleAllele",
        variationType = Some("simple"),
        content = None
      ),
      ClinicalAssertionVariation(
        id = "SCV!.5",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = List("SCV!.6", "SCV!.7", "SCV!.8"),
        descendantIds = List("SCV!.6", "SCV!.7", "SCV!.8"),
        subclassType = "Haplotype",
        variationType = Some("haplo"),
        content = None
      ),
      ClinicalAssertionVariation(
        id = "SCV!.6",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = Nil,
        descendantIds = Nil,
        subclassType = "SimpleAllele",
        variationType = Some("simple"),
        content = None
      ),
      ClinicalAssertionVariation(
        id = "SCV!.7",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = Nil,
        descendantIds = Nil,
        subclassType = "SimpleAllele",
        variationType = Some("simple"),
        content = None
      ),
      ClinicalAssertionVariation(
        id = "SCV!.8",
        releaseDate = date,
        clinicalAssertionId = "SCV!",
        childIds = Nil,
        descendantIds = Nil,
        subclassType = "SimpleAllele",
        variationType = Some("simple"),
        content = None
      )
    )
  }

  it should "parse an SCV with a trait set" in {
    val raw = JsonParser.parseEncodedJson(
      """{
        |  "@ID": "123",
        |  "@SubmissionDate": "4321-09-20",
        |  "ClinVarAccession": {
        |    "@Type": "SCV",
        |    "@Accession": "SCV!",
        |    "@Version": "2",
        |    "@OrgID": "organization",
        |    "@SubmitterName": "name",
        |    "@OrgAbbreviation": "org",
        |    "@OrganizationCategory": "cat"
        |  },
        |  "TraitSet": {
        |    "ayy": "lmao"
        |  }
        |}""".stripMargin
    )
    val parser = SCV.parser(
      date,
      (_, id, raw) =>
        SCVTraitSet(
          ClinicalAssertionTraitSet.init(id, date).copy(`type` = raw.tryExtract[String]("ayy")),
          Nil
        )
    )
    val parsed = parser.parse(context, raw)

    parsed.traitSets shouldBe List(
      ClinicalAssertionTraitSet.init("SCV!", date).copy(`type` = Some("lmao"))
    )
  }

  it should "cross-link to RCVs when there's only one option" in {
    val linkableInterpretation = interpretation.copy(
      traitSets = List(TraitSet.init("set!", date).copy(traitIds = List("1", "2"))),
      traits = List.tabulate(2)(i => Trait.init((i + 1).toString, date))
    )
    val linkableContext = context.copy(
      interpretation = linkableInterpretation,
      referenceAccessions = List(
        RcvAccession
          .init("rcv!", date, 3L, context.variationId, context.vcvId)
          .copy(traitSetId = Some("set!"))
      )
    )

    val raw = JsonParser.parseEncodedJson(
      """{
        |  "@ID": "123",
        |  "@SubmissionDate": "4321-09-20",
        |  "ClinVarAccession": {
        |    "@Type": "SCV",
        |    "@Accession": "SCV!",
        |    "@Version": "2",
        |    "@OrgID": "organization",
        |    "@SubmitterName": "name",
        |    "@OrgAbbreviation": "org",
        |    "@OrganizationCategory": "cat"
        |  },
        |  "TraitSet": {}
        |}""".stripMargin
    )
    val parser = SCV.parser(
      date,
      (_, id, _) => SCVTraitSet(ClinicalAssertionTraitSet.init(id, date), Nil)
    )
    val parsed = parser.parse(linkableContext, raw)

    parsed.assertion.traitSetId.value shouldBe "set!"
    parsed.assertion.rcvAccessionId.value shouldBe "rcv!"
  }

  it should "cross-link to RCVs when there are multiple options" in {
    val linkableInterpretation = interpretation.copy(
      traitSets = List(
        TraitSet.init("set-odd", date).copy(traitIds = (1 to 10 by 2).map(_.toString).toList),
        TraitSet.init("set-even", date).copy(traitIds = (2 to 10 by 2).map(_.toString).toList)
      ),
      traits = List.tabulate(10)(i => Trait.init((i + 1).toString, date))
    )
    val linkableContext = context.copy(
      interpretation = linkableInterpretation,
      referenceAccessions = List(
        RcvAccession
          .init("rcv-even", date, 3L, context.variationId, context.vcvId)
          .copy(traitSetId = Some("set-even")),
        RcvAccession
          .init("rcv-odd", date, 2L, context.variationId, context.vcvId)
          .copy(traitSetId = Some("set-odd"))
      )
    )

    val raw = JsonParser.parseEncodedJson(
      """{
        |  "@ID": "123",
        |  "@SubmissionDate": "4321-09-20",
        |  "ClinVarAccession": {
        |    "@Type": "SCV",
        |    "@Accession": "SCV!",
        |    "@Version": "2",
        |    "@OrgID": "organization",
        |    "@SubmitterName": "name",
        |    "@OrgAbbreviation": "org",
        |    "@OrganizationCategory": "cat"
        |  },
        |  "TraitSet": {}
        |}""".stripMargin
    )
    val parser = SCV.parser(
      date,
      (_, id, _) =>
        SCVTraitSet(
          ClinicalAssertionTraitSet.init(id, date),
          (2 to 10 by 2).map { i =>
            ClinicalAssertionTrait.init(i.toString, date).copy(traitId = Some(i.toString))
          }.toList
        )
    )
    val parsed = parser.parse(linkableContext, raw)

    parsed.assertion.traitSetId.value shouldBe "set-even"
    parsed.assertion.rcvAccessionId.value shouldBe "rcv-even"
  }

  it should "parse an SCV with extra observations" in {
    val raw = JsonParser.parseEncodedJson(
      """{
        |  "@ID": "123",
        |  "@SubmissionDate": "4321-09-20",
        |  "ClinVarAccession": {
        |    "@Type": "SCV",
        |    "@Accession": "SCV!",
        |    "@Version": "2",
        |    "@OrgID": "organization",
        |    "@SubmitterName": "name",
        |    "@OrgAbbreviation": "org",
        |    "@OrganizationCategory": "cat"
        |  },
        |  "ObservedInList": {
        |    "ObservedIn": [
        |      {
        |        "TraitSet": {
        |          "start": 5,
        |          "end": 7
        |        },
        |        "Extra": { "cool": 2 }
        |      },
        |      {
        |        "TraitSet": {
        |          "start": 1,
        |          "end": 4
        |        }
        |      },
        |      {
        |        "More extra!": [],
        |        "TraitSet": {
        |          "start": 8,
        |          "end": 10
        |        }
        |      }
        |    ]
        |  }
        |}""".stripMargin
    )
    val parser = SCV.parser(
      date,
      (_, id, raw) => {
        val start = raw.extract[Long]("start")
        val end = raw.extract[Long]("end")
        SCVTraitSet(
          ClinicalAssertionTraitSet
            .init(id, date)
            .copy(clinicalAssertionTraitIds = (start to end).map(_.toString).toList),
          (start to end).map(i => ClinicalAssertionTrait.init(i.toString, date)).toList
        )
      }
    )
    val parsed = parser.parse(context, raw)

    parsed.observations should contain theSameElementsAs List(
      ClinicalAssertionObservation
        .init("SCV!.0", date)
        .copy(
          clinicalAssertionTraitSetId = Some("SCV!.0"),
          content = Some("""{"Extra":{"cool":2}}""")
        ),
      ClinicalAssertionObservation
        .init("SCV!.1", date)
        .copy(clinicalAssertionTraitSetId = Some("SCV!.1")),
      ClinicalAssertionObservation
        .init("SCV!.2", date)
        .copy(
          clinicalAssertionTraitSetId = Some("SCV!.2"),
          content = Some("""{"More extra!":[]}""")
        )
    )
    parsed.traitSets should contain theSameElementsAs List(
      ClinicalAssertionTraitSet
        .init("SCV!.0", date)
        .copy(clinicalAssertionTraitIds = List("5", "6", "7")),
      ClinicalAssertionTraitSet
        .init("SCV!.1", date)
        .copy(clinicalAssertionTraitIds = List("1", "2", "3", "4")),
      ClinicalAssertionTraitSet
        .init("SCV!.2", date)
        .copy(clinicalAssertionTraitIds = List("8", "9", "10"))
    )
    parsed.traits should contain theSameElementsAs List.tabulate(10) { i =>
      ClinicalAssertionTrait.init((i + 1).toString, date)
    }
  }
}
