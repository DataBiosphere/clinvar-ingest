package org.broadinstitute.monster.clinvar

import java.time.LocalDate

import com.spotify.scio.testing.PipelineSpec
import org.broadinstitute.monster.clinvar.jadeschema.table.{
  ClinicalAssertion,
  Gene,
  Submission,
  Submitter,
  Trait,
  TraitSet,
  Variation,
  VariationArchive
}
import org.broadinstitute.monster.clinvar.parsers.{ParsedArchive, ParsedScv, ParsedVariation}
import org.broadinstitute.monster.common.PipelineCoders
import upack._

class ArchiveBranchesSpec extends PipelineSpec with PipelineCoders {
  import org.broadinstitute.monster.common.msg.MsgOps

  behavior of "ArchiveBranches"

  it should "parse raw archives" in {
    def parse(id: String) = ParsedArchive(
      variation = ParsedVariation(
        variation = Variation.init(id, "type"),
        genes = Nil,
        associations = Nil
      ),
      vcv = None,
      rcvs = Nil,
      traitSets = Nil,
      traits = Nil,
      traitMappings = Nil,
      scvs = Nil
    )

    val fakeParser: ParsedArchive.Parser = rawArchive => {
      val id = rawArchive.read[String]("id")
      parse(id)
    }

    val archiveCount = 10

    val in = List.tabulate[Msg](archiveCount) { i =>
      Obj(ArchiveBranches.ArchiveKey -> Obj(Str("id") -> Str(i.toString)))
    }
    val expected = List.tabulate(archiveCount)(i => parse(i.toString).variation.variation)
    val out = runWithData(in)(ArchiveBranches.fromArchiveStream(fakeParser, _).variations)

    out should contain theSameElementsAs expected
  }

  it should "dedup genes by date" in {
    def parse(id: String) = ParsedArchive(
      variation = ParsedVariation(
        variation = Variation.init(id, "type"),
        genes = List(Gene.init(s"${id.toInt % 2}").copy(symbol = Some(id))),
        associations = Nil
      ),
      vcv = Some(
        VariationArchive
          .init(id, 1L, id)
          .copy(dateLastUpdated = Some(LocalDate.parse(f"2020-${id.toInt + 1}%02d-01")))
      ),
      rcvs = Nil,
      traitSets = Nil,
      traits = Nil,
      traitMappings = Nil,
      scvs = Nil
    )

    val fakeParser: ParsedArchive.Parser = rawArchive => {
      val id = rawArchive.read[String]("id")
      parse(id)
    }

    val archiveCount = 10

    val in = List.tabulate[Msg](archiveCount) { i =>
      Obj(ArchiveBranches.ArchiveKey -> Obj(Str("id") -> Str(i.toString)))
    }
    val expected = List(
      Gene.init("0").copy(symbol = Some(s"${archiveCount - 2}")),
      Gene.init("1").copy(symbol = Some(s"${archiveCount - 1}"))
    )
    val out = runWithData(in)(ArchiveBranches.fromArchiveStream(fakeParser, _).genes)

    out should contain theSameElementsAs expected
  }

  it should "dedup traits by date" in {
    def parse(id: String) = ParsedArchive(
      variation = ParsedVariation(
        variation = Variation.init(id, "type"),
        genes = Nil,
        associations = Nil
      ),
      vcv = Some(
        VariationArchive
          .init(id, 1L, id)
          .copy(dateLastUpdated = Some(LocalDate.parse(f"2020-${id.toInt + 1}%02d-01")))
      ),
      rcvs = Nil,
      traitSets = Nil,
      traits = List(Trait.init(id = s"${id.toInt % 2}").copy(medgenId = Some(id))),
      traitMappings = Nil,
      scvs = Nil
    )

    val fakeParser: ParsedArchive.Parser = rawArchive => {
      val id = rawArchive.read[String]("id")
      parse(id)
    }

    val archiveCount = 10

    val in = List.tabulate[Msg](archiveCount) { i =>
      Obj(ArchiveBranches.ArchiveKey -> Obj(Str("id") -> Str(i.toString)))
    }
    val expected = List(
      Trait.init("0").copy(medgenId = Some(s"${archiveCount - 2}")),
      Trait.init("1").copy(medgenId = Some(s"${archiveCount - 1}"))
    )
    val out = runWithData(in)(ArchiveBranches.fromArchiveStream(fakeParser, _).traits)

    out should contain theSameElementsAs expected
  }

  it should "dedup trait-sets by date" in {
    def parse(id: String) = ParsedArchive(
      variation = ParsedVariation(
        variation = Variation.init(id, "type"),
        genes = Nil,
        associations = Nil
      ),
      vcv = Some(
        VariationArchive
          .init(id, 1L, id)
          .copy(dateLastUpdated = Some(LocalDate.parse(f"2020-${id.toInt + 1}%02d-01")))
      ),
      rcvs = Nil,
      traitSets = List(TraitSet.init(id = s"${id.toInt % 2}").copy(`type` = Some(id))),
      traits = Nil,
      traitMappings = Nil,
      scvs = Nil
    )

    val fakeParser: ParsedArchive.Parser = rawArchive => {
      val id = rawArchive.read[String]("id")
      parse(id)
    }

    val archiveCount = 10

    val in = List.tabulate[Msg](archiveCount) { i =>
      Obj(ArchiveBranches.ArchiveKey -> Obj(Str("id") -> Str(i.toString)))
    }
    val expected = List(
      TraitSet.init("0").copy(`type` = Some(s"${archiveCount - 2}")),
      TraitSet.init("1").copy(`type` = Some(s"${archiveCount - 1}"))
    )
    val out = runWithData(in)(ArchiveBranches.fromArchiveStream(fakeParser, _).traitSets)

    out should contain theSameElementsAs expected
  }

  it should "dedup submissions by date" in {
    def parse(id: String) = ParsedArchive(
      variation = ParsedVariation(
        variation = Variation.init(id, "type"),
        genes = Nil,
        associations = Nil
      ),
      vcv = None,
      rcvs = Nil,
      traitSets = Nil,
      traits = Nil,
      traitMappings = Nil,
      scvs = List(
        ParsedScv(
          assertion = ClinicalAssertion
            .init(id, 1L, id, id, id, id, id)
            .copy(dateLastUpdated = Some(LocalDate.parse(f"2020-${id.toInt + 1}%02d-01"))),
          submitters = Nil,
          submission = Submission
            .init(s"${id.toInt % 2}", id, LocalDate.parse(f"2020-${id.toInt + 1}%02d-01")),
          variations = Nil,
          traitSets = Nil,
          traits = Nil,
          observations = Nil
        )
      )
    )

    val fakeParser: ParsedArchive.Parser = rawArchive => {
      val id = rawArchive.read[String]("id")
      parse(id)
    }

    val archiveCount = 10

    val in = List.tabulate[Msg](archiveCount) { i =>
      Obj(ArchiveBranches.ArchiveKey -> Obj(Str("id") -> Str(i.toString)))
    }
    val expected = List(
      Submission
        .init("0", s"${archiveCount - 2}", LocalDate.parse(f"2020-${archiveCount - 1}%02d-01")),
      Submission
        .init("1", s"${archiveCount - 1}", LocalDate.parse(f"2020-$archiveCount%02d-01"))
    )
    val out = runWithData(in)(ArchiveBranches.fromArchiveStream(fakeParser, _).submissions)

    out should contain theSameElementsAs expected
  }

  it should "aggregate names and abbreviations for submitters" in {
    def parse(id: String) = ParsedArchive(
      variation = ParsedVariation(
        variation = Variation.init(id, "type"),
        genes = Nil,
        associations = Nil
      ),
      vcv = None,
      rcvs = Nil,
      traitSets = Nil,
      traits = Nil,
      traitMappings = Nil,
      scvs = List(
        ParsedScv(
          assertion = ClinicalAssertion
            .init(id, 1L, id, id, id, id, id)
            .copy(dateLastUpdated = Some(LocalDate.parse(f"2020-${id.toInt + 1}%02d-01"))),
          submitters = List(
            Submitter
              .init(s"${id.toInt % 2}")
              .copy(
                currentName = Some(id),
                allNames = List(id),
                currentAbbrev = Some(id),
                allAbbrevs = List(id)
              )
          ),
          submission = Submission
            .init(s"${id.toInt % 2}", id, LocalDate.parse(f"2020-${id.toInt + 1}%02d-01")),
          variations = Nil,
          traitSets = Nil,
          traits = Nil,
          observations = Nil
        )
      )
    )

    val fakeParser: ParsedArchive.Parser = rawArchive => {
      val id = rawArchive.read[String]("id")
      parse(id)
    }

    val archiveCount = 10

    val in = List.tabulate[Msg](archiveCount) { i =>
      Obj(ArchiveBranches.ArchiveKey -> Obj(Str("id") -> Str(i.toString)))
    }
    val expected = List(
      Submitter
        .init("0")
        .copy(
          currentName = Some(s"${archiveCount - 2}"),
          allNames = (0 until archiveCount by 2).map(_.toString).toList,
          currentAbbrev = Some(s"${archiveCount - 2}"),
          allAbbrevs = (0 until archiveCount by 2).map(_.toString).toList
        ),
      Submitter
        .init("1")
        .copy(
          currentName = Some(s"${archiveCount - 1}"),
          allNames = (1 until archiveCount by 2).map(_.toString).toList,
          currentAbbrev = Some(s"${archiveCount - 1}"),
          allAbbrevs = (1 until archiveCount by 2).map(_.toString).toList
        )
    )
    val out = runWithData(in)(ArchiveBranches.fromArchiveStream(fakeParser, _).submitters)

    out should contain theSameElementsAs expected
  }

}
