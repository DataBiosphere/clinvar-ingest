package org.broadinstitute.monster.clinvar

import com.spotify.scio.ScioContext
import org.broadinstitute.monster.clinvar.parsers.ParsedArchive
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}

object ClinVarPipelineBuilder extends PipelineBuilder[Args] {

  /**
    * Schedule all the steps for the ClinVar pipeline in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    // Read the nested archives from storage.
    val fullArchives = StorageIO
      .readJsonLists(
        ctx,
        "VariationArchive",
        s"${args.inputPrefix}/VariationArchive/*.json"
      )

    // Split apart all of the entities that exist in the archives.
    // Since individual archives are self-contained, nearly all of the pipeline's
    // logic is done in this step.
    val archiveBranches =
      ArchiveBranches.fromArchiveStream(ParsedArchive.parser(args.releaseDate), fullArchives)

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
