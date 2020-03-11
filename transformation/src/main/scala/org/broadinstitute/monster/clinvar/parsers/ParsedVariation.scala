package org.broadinstitute.monster.clinvar.parsers

import org.broadinstitute.monster.clinvar.{Constants, Content}
import org.broadinstitute.monster.clinvar.jadeschema.table.{
  Gene,
  GeneAssociation,
  Variation
}
import upack.Msg

/**
  * Wrapper for fully-parsed contents of a variation modeled by ClinVar.
  *
  * @param variation info about the type and location of the variation in
  *                  various genome builds
  * @param genes info about genes associated with the variation
  * @param associations info about how the variation effects the genes
  */
case class ParsedVariation(
  variation: Variation,
  genes: Array[Gene],
  associations: Array[GeneAssociation]
)

object ParsedVariation {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Extract variation models from a raw InterpretedRecord or IncludedRecord. */
  def fromRawRecord(rawRecord: Msg): ParsedVariation = {
    // Get the top-level variation.
    val (rawVariation, variationType) = Constants.VariationTypes
      .foldLeft(Option.empty[(Msg, String)]) { (acc, subtype) =>
        acc.orElse(rawRecord.tryExtract[Msg](subtype).map(_ -> subtype))
      }
      .getOrElse {
        throw new IllegalStateException(
          s"Found a record with no variation: $rawRecord"
        )
      }
    val topId = rawVariation.extract[String]("@VariationID")

    // Extract gene info before parsing the rest of the variation so we can
    // bundle all the unmodeled content at the end.
    val (genes, geneAssociations) = rawVariation
      .tryExtract[Array[Msg]]("GeneList", "Gene")
      .getOrElse(Array.empty)
      .map { rawGene =>
        val gene = Gene(
          id = rawGene.extract[String]("@GeneID"),
          symbol = rawGene.tryExtract[String]("@Symbol"),
          hgncId = rawGene.tryExtract[String]("@HGNC_ID"),
          fullName = rawGene.tryExtract[String]("FullName", "$")
        )
        val geneAssociation = GeneAssociation(
          geneId = gene.id,
          variationId = topId,
          relationshipType = rawGene.tryExtract[String]("@RelationshipType"),
          source = rawGene.tryExtract[String]("@Source"),
          content = Content.encode(rawGene)
        )
        (gene, geneAssociation)
      }
      .unzip

    val variation = {
      val descendants = extractDescendantIds(rawVariation)

      Variation(
        id = topId,
        subclassType = variationType,
        childIds = descendants.childIds.toArray,
        descendantIds = (descendants.childIds ::: descendants.descendantIds).toArray,
        name = rawVariation.tryExtract[String]("Name", "$"),
        variationType = rawVariation
          .tryExtract[Msg]("VariantType")
          .orElse(rawVariation.tryExtract[Msg]("VariationType"))
          .map(_.extract[String]("$")),
        alleleId = rawVariation.tryExtract[String]("@AlleleID"),
        proteinChange = rawVariation
          .tryExtract[Array[Msg]]("ProteinChange")
          .getOrElse(Array.empty)
          .map(_.extract[String]("$")),
        numChromosomes = rawVariation.tryExtract[Long]("@NumberOfChromosomes"),
        numCopies = rawVariation.tryExtract[Long]("@NumberOfCopies"),
        content = Content.encode(rawVariation)
      )
    }

    ParsedVariation(variation, genes, geneAssociations)
  }

  /**
    * Descend the hierarchy of a variation payload to extract the IDs of its
    * immediate children and "deeper" descendants.
    *
    * @return a tuple where the first element contains the IDs of the variation's
    *         immediate children, and the second contains the IDs of all other
    *         descendants of the variation
    */
  def extractDescendantIds(rawVariation: Msg): VariationDescendants =
    VariationDescendants.fromVariationWrapper(rawVariation) { (_, variation) =>
      val id = variation.extract[String]("@VariationID")
      val descendants = extractDescendantIds(variation)
      (id, descendants.childIds ::: descendants.descendantIds)
    }
}
