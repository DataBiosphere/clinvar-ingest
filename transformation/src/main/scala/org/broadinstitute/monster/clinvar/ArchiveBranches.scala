package org.broadinstitute.monster.clinvar

import java.time.LocalDate

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import org.broadinstitute.monster.clinvar.jadeschema.table._
import org.broadinstitute.monster.clinvar.parsers.ParsedArchive
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

object ArchiveBranches {
  /** Object wrapper key expected for all archive entries. */
  val ArchiveKey: Msg = Str("VariationArchive")

  /** (De)serializer for dates parsed out of archives. */
  implicit val dateCoder: Coder[LocalDate] = Coder.xmap(Coder.stringCoder)(
    LocalDate.parse(_),
    _.toString
  )

  /**
    * Split a stream of raw VariationArchive entries into multiple
    * streams of un-nested entities.
    *
    * Cross-linking between entities in the output streams occurs
    * prior to elements being pushed out of the split step.
    */
  def fromArchiveStream(archiveStream: SCollection[Msg]): ArchiveBranches = {
    val geneOut = SideOutput[Gene]
    val geneAssociationOut = SideOutput[GeneAssociation]
    val vcvOut = SideOutput[VariationArchive]
    val rcvOut = SideOutput[RcvAccession]
    val submitterOut = SideOutput[Submitter]
    val submissionOut = SideOutput[Submission]
    val scvOut = SideOutput[ClinicalAssertion]
    val scvVariationOut = SideOutput[ClinicalAssertionVariation]
    val scvObservationOut = SideOutput[ClinicalAssertionObservation]
    val scvTraitSetOut = SideOutput[ClinicalAssertionTraitSet]
    val scvTraitOut = SideOutput[ClinicalAssertionTrait]
    val traitSetOut = SideOutput[TraitSet]
    val traitOut = SideOutput[Trait]
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
        val parsed = ParsedArchive.fromRawArchive(archiveCopy)
        // Output all the things!
        parsed.variation.genes.foreach(ctx.output(geneOut, _))
        parsed.variation.associations.foreach(ctx.output(geneAssociationOut, _))
        parsed.vcv.foreach(ctx.output(vcvOut, _))
        parsed.rcvs.foreach(ctx.output(rcvOut, _))
        parsed.scvs.foreach { aggregateScv =>
          ctx.output(submitterOut, aggregateScv.submitter)
          ctx.output(submissionOut, aggregateScv.submission)
          ctx.output(scvOut, aggregateScv.assertion)
          aggregateScv.variations.foreach(ctx.output(scvVariationOut, _))
          aggregateScv.observations.foreach(ctx.output(scvObservationOut, _))
          aggregateScv.traitSets.foreach(ctx.output(scvTraitSetOut, _))
          aggregateScv.traits.foreach(ctx.output(scvTraitOut, _))
        }
        parsed.traitSets.foreach(ctx.output(traitSetOut, _))
        parsed.traits.foreach(ctx.output(traitOut, _))
        parsed.traitMappings.foreach(ctx.output(traitMappingOut, _))
        // Use variation as the main output because each archive contains
        // exactly one of them.
        parsed.variation.variation
      }

    ArchiveBranches(
      variations = variationStream,
      genes = sideCtx(geneOut).distinctBy(_.id),
      geneAssociations = sideCtx(geneAssociationOut),
      vcvs = sideCtx(vcvOut),
      rcvs = sideCtx(rcvOut),
      submitters = sideCtx(submitterOut).distinctBy(_.id),
      submissions = sideCtx(submissionOut).distinctBy(_.id),
      scvs = sideCtx(scvOut),
      scvVariations = sideCtx(scvVariationOut),
      scvObservations = sideCtx(scvObservationOut),
      scvTraitSets = sideCtx(scvTraitSetOut),
      scvTraits = sideCtx(scvTraitOut),
      traitSets = sideCtx(traitSetOut).distinctBy(_.id),
      traits = sideCtx(traitOut).distinctBy(_.id),
      traitMappings = sideCtx(traitMappingOut)
    )
  }
}
