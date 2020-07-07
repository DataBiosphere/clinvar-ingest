package org.broadinstitute.monster.clinvar

import java.time.LocalDate

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import org.broadinstitute.monster.clinvar.jadeschema.table._
import org.broadinstitute.monster.clinvar.parsers.VCV
import org.broadinstitute.monster.common.PipelineCoders
import upack.{Msg, Str}

/**
  * Collection of data streams produced by the initial splitting
  * operation performed on raw VariationArchive entries.
  */
case class ArchiveBranches(
  variations: SCollection[Variation],
  genes: SCollection[Gene],
  geneAssociations: SCollection[GeneAssociation],
  vcvs: SCollection[VariationArchive],
  rcvs: SCollection[RcvAccession],
  submitters: SCollection[Submitter],
  submissions: SCollection[Submission],
  scvs: SCollection[ClinicalAssertion],
  scvVariations: SCollection[ClinicalAssertionVariation],
  scvObservations: SCollection[ClinicalAssertionObservation],
  scvTraitSets: SCollection[ClinicalAssertionTraitSet],
  scvTraits: SCollection[ClinicalAssertionTrait],
  traitSets: SCollection[TraitSet],
  traits: SCollection[Trait],
  traitMappings: SCollection[TraitMapping]
)

object ArchiveBranches extends PipelineCoders {

  /** Object wrapper key expected for all archive entries. */
  val ArchiveKey: Msg = Str("VariationArchive")

  implicit val dateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

  /**
    * Split a stream of raw VariationArchive entries into multiple
    * streams of un-nested entities.
    *
    * Cross-linking between entities in the output streams occurs
    * prior to elements being pushed out of the split step.
    */
  def fromArchiveStream(
    parser: VCV.Parser,
    archiveStream: SCollection[Msg]
  ): ArchiveBranches = {
    val geneOut = SideOutput[(Gene, Option[LocalDate])]
    val geneAssociationOut = SideOutput[GeneAssociation]
    val vcvOut = SideOutput[VariationArchive]
    val rcvOut = SideOutput[RcvAccession]
    val submitterOut = SideOutput[(Submitter, Option[LocalDate])]
    val submissionOut = SideOutput[(Submission, Option[LocalDate])]
    val scvOut = SideOutput[ClinicalAssertion]
    val scvVariationOut = SideOutput[ClinicalAssertionVariation]
    val scvObservationOut = SideOutput[ClinicalAssertionObservation]
    val scvTraitSetOut = SideOutput[ClinicalAssertionTraitSet]
    val scvTraitOut = SideOutput[ClinicalAssertionTrait]
    val traitSetOut = SideOutput[(TraitSet, Option[LocalDate])]
    val traitOut = SideOutput[(Trait, Option[LocalDate])]
    val traitMappingOut = SideOutput[TraitMapping]

    val (variationStream, sideCtx) = archiveStream
      .withSideOutputs(
        geneOut,
        geneAssociationOut,
        vcvOut,
        rcvOut,
        submitterOut,
        submissionOut,
        scvOut,
        scvVariationOut,
        scvObservationOut,
        scvTraitSetOut,
        scvTraitOut,
        traitSetOut,
        traitOut,
        traitMappingOut
      )
      .withName("Split Variation Archives")
      .map { (rawArchive, ctx) =>
        // Beam prohibits mutating inputs, so we have to copy the archive before
        // processing it.
        val archiveCopy = upack.copy(rawArchive.obj(ArchiveKey))
        // Parse the raw archive into the structures we care about.
        val parsed = parser.parse(archiveCopy)
        // Output all the things!
        val updateDate = parsed.vcv.flatMap(_.dateLastUpdated)
        parsed.variation.genes.foreach(g => ctx.output(geneOut, g -> updateDate))
        parsed.variation.associations.foreach(ctx.output(geneAssociationOut, _))
        parsed.vcv.foreach(ctx.output(vcvOut, _))
        parsed.rcvs.foreach(ctx.output(rcvOut, _))
        parsed.scvs.foreach { aggregateScv =>
          val submissionDate = aggregateScv.assertion.dateLastUpdated
          aggregateScv.submitters.foreach(s => ctx.output(submitterOut, s -> submissionDate))
          ctx.output(submissionOut, aggregateScv.submission -> submissionDate)
          ctx.output(scvOut, aggregateScv.assertion)
          aggregateScv.variations.foreach(ctx.output(scvVariationOut, _))
          aggregateScv.observations.foreach(ctx.output(scvObservationOut, _))
          aggregateScv.traitSets.foreach(ctx.output(scvTraitSetOut, _))
          aggregateScv.traits.foreach(ctx.output(scvTraitOut, _))
        }
        parsed.traitSets.foreach(tSet => ctx.output(traitSetOut, tSet -> updateDate))
        parsed.traits.foreach(t => ctx.output(traitOut, t -> updateDate))
        parsed.traitMappings.foreach(ctx.output(traitMappingOut, _))
        // Use variation as the main output because each archive contains
        // exactly one of them.
        parsed.variation.variation
      }

    val latestGenes = dedupByDate(sideCtx(geneOut), "gene")(_.id)
    val latestTraitSets = dedupByDate(sideCtx(traitSetOut), "trait_set")(_.id)
    val latestTraits = dedupByDate(sideCtx(traitOut), "trait")(_.id)
    val aggregatedSubmitters = aggregateSubmitters(sideCtx(submitterOut))
    val latestSubmissions = dedupByDate(sideCtx(submissionOut), "submission")(_.id)

    ArchiveBranches(
      variations = variationStream,
      genes = latestGenes,
      geneAssociations = sideCtx(geneAssociationOut),
      vcvs = sideCtx(vcvOut),
      rcvs = sideCtx(rcvOut),
      submitters = aggregatedSubmitters,
      submissions = latestSubmissions,
      scvs = sideCtx(scvOut),
      scvVariations = sideCtx(scvVariationOut),
      scvObservations = sideCtx(scvObservationOut),
      scvTraitSets = sideCtx(scvTraitSetOut),
      scvTraits = sideCtx(scvTraitOut),
      traitSets = latestTraitSets,
      traits = latestTraits,
      traitMappings = sideCtx(traitMappingOut)
    )
  }

  def dedupByDate[T: Coder](
    in: SCollection[(T, Option[LocalDate])],
    description: String
  )(getId: T => String): SCollection[T] =
    in.transform(s"Deduplicate $description elements") {
      _.groupBy { case (item, _) => getId(item) }.values.flatMap { itemGroup =>
        val datedItems = itemGroup.flatMap {
          case (item, date) => date.map(item -> _)
        }
        if (datedItems.isEmpty) {
          // No way to tell what the "right" item is.
          itemGroup.headOption.map(_._1).toIterable
        } else {
          Iterable(datedItems.maxBy(_._2)._1)
        }
      }
    }

  def aggregateSubmitters(in: SCollection[(Submitter, Option[LocalDate])]): SCollection[Submitter] =
    in.transform(s"Aggregate submitter elements") {
      _.groupBy(_._1.id).values.map { allSubmitters =>
        val latestSubmitter = allSubmitters.maxBy(_._2)._1
        val allNames =
          latestSubmitter.allNames.toSet.union(allSubmitters.flatMap(_._1.currentName).toSet)
        val allAbbrevs =
          latestSubmitter.allAbbrevs.toSet.union(allSubmitters.flatMap(_._1.currentAbbrev).toSet)

        latestSubmitter.copy(
          allNames = allNames.toList.sorted,
          allAbbrevs = allAbbrevs.toList.sorted
        )
      }
    }
}
