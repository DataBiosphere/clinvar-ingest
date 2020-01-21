package org.broadinstitute.monster.clinvar

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.coders.Coder
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.broadinstitute.monster.ClinvarIngestBuildInfo
import org.broadinstitute.monster.common.StorageIO
import org.broadinstitute.monster.common.msg.UpackMsgCoder
import upack.Msg

object ClinVarPipeline {

  @AppName("ClinVar transformation pipeline")
  @AppVersion(ClinvarIngestBuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.clinvar.ClinVarPipeline")
  case class Args(
    @HelpMessage("Path to the top-level directory where ClinVar XML was extracted")
    inputPrefix: String,
    @HelpMessage("Path where transformed ClinVar JSON should be written")
    outputPrefix: String
  )

  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)
    buildPipeline(pipelineContext, parsedArgs)
    pipelineContext.run()
    ()
  }

  /** (De)serializer for the upack messages we read from storage. */
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  /**
    * Schedule all the steps for the ClinVar pipeline in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  def buildPipeline(pipelineContext: ScioContext, args: Args): Unit = {
    // Read the nested archives from storage.
    val fullArchives = StorageIO
      .readJsonLists(
        pipelineContext,
        "VariationArchive",
        s"${args.inputPrefix}/VariationArchive/*.json"
      )

    // Split apart all of the entities that exist in the archives.
    // Since individual archives are self-contained, nearly all of the pipeline's
    // logic is done in this step.
    val archiveBranches = ArchiveBranches.fromArchiveStream(fullArchives)

    // Write everything back to storage.
    StorageIO.writeJsonLists(
      archiveBranches.variations,
      "Variations",
      s"${args.outputPrefix}/variation"
    )
    StorageIO.writeJsonLists(
      archiveBranches.genes,
      "Genes",
      s"${args.outputPrefix}/gene"
    )
    StorageIO.writeJsonLists(
      archiveBranches.geneAssociations,
      "Gene Associations",
      s"${args.outputPrefix}/gene_association"
    )
    StorageIO.writeJsonLists(
      archiveBranches.vcvs,
      "VCVs",
      s"${args.outputPrefix}/variation_archive"
    )
    StorageIO.writeJsonLists(
      archiveBranches.vcvReleases,
      "VCV Releases",
      s"${args.outputPrefix}/variation_archive_release"
    )
    StorageIO.writeJsonLists(
      archiveBranches.rcvs,
      "RCV Accessions",
      s"${args.outputPrefix}/rcv_accession"
    )
    StorageIO.writeJsonLists(
      archiveBranches.scvs,
      "SCVs",
      s"${args.outputPrefix}/clinical_assertion"
    )
    StorageIO.writeJsonLists(
      archiveBranches.submitters,
      "Submitters",
      s"${args.outputPrefix}/submitter"
    )
    StorageIO.writeJsonLists(
      archiveBranches.submissions,
      "Submissions",
      s"${args.outputPrefix}/submission"
    )
    StorageIO.writeJsonLists(
      archiveBranches.scvVariations,
      "SCV Variations",
      s"${args.outputPrefix}/clinical_assertion_variation"
    )
    StorageIO.writeJsonLists(
      archiveBranches.scvObservations,
      "SCV Observations",
      s"${args.outputPrefix}/clinical_assertion_observation"
    )
    StorageIO.writeJsonLists(
      archiveBranches.scvTraitSets,
      "SCV Trait Sets",
      s"${args.outputPrefix}/clinical_assertion_trait_set"
    )
    StorageIO.writeJsonLists(
      archiveBranches.scvTraits,
      "SCV Traits",
      s"${args.outputPrefix}/clinical_assertion_trait"
    )
    StorageIO.writeJsonLists(
      archiveBranches.traitSets,
      "Trait Sets",
      s"${args.outputPrefix}/trait_set"
    )
    StorageIO.writeJsonLists(
      archiveBranches.traits,
      "Traits",
      s"${args.outputPrefix}/trait"
    )
    StorageIO.writeJsonLists(
      archiveBranches.traitMappings,
      "Trait Mappings",
      s"${args.outputPrefix}/trait_mapping"
    )
    ()
  }
}
