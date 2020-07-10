package org.broadinstitute.monster.clinvar.parsers

import java.time.LocalDate

import org.broadinstitute.monster.clinvar.Content
import org.broadinstitute.monster.clinvar.jadeschema.struct.Xref
import org.broadinstitute.monster.clinvar.jadeschema.table.{Trait, TraitSet}
import upack.Msg

import scala.collection.mutable

/**
  * Wrapper for fully-parsed info about the interpreted clinical effects of a variation.
  *
  * @param dateLastEvaluated day the interpretation was last revised
  * @param `type` type of interpretation
  * @param description high-level description of the interpreted effects
  * @param explanation detailed explanation of the interpreted effects
  * @param content unmodeled fields from the interpretation
  * @param traitSets groups of conditions described by the interpretation
  * @param traits conditions described by the interpretation
  */
case class Interpretation(
  dateLastEvaluated: Option[LocalDate],
  `type`: Option[String],
  description: Option[String],
  explanation: Option[String],
  content: Option[String],
  traitSets: List[TraitSet],
  traits: List[Trait]
)

object Interpretation {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Interface for a utility which can convert raw Interpretations into
    * our target schema.
    */
  trait Parser extends Serializable {

    /** Convert a raw Interpretation payload into our parsed form. */
    def parse(rawInterpretation: Msg): Interpretation
  }

  /** Parser for "real" Interpretation payloads, to be used in production. */
  def parser(releaseDate: LocalDate, traitParser: TraitMetadata.Parser): Parser =
    rawInterpretation => {
      // Extract trait info first, so it doesn't get bundled into unmodeled content.
      val (traitSets, traits) = rawInterpretation
        .tryExtract[List[Msg]]("ConditionList", "TraitSet")
        .getOrElse(List.empty)
        .foldLeft((List.empty[TraitSet], List.empty[Trait])) {
          case ((setAcc, traitAcc), rawTraitSet) =>
            val traits =
              rawTraitSet.extract[List[Msg]]("Trait").map(parseRawTrait(releaseDate, traitParser))
            val traitSet = TraitSet(
              id = rawTraitSet.extract[String]("@ID"),
              releaseDate = releaseDate,
              `type` = rawTraitSet.tryExtract[String]("@Type"),
              traitIds = traits.map(_.id).sorted,
              content = Content.encode(rawTraitSet)
            )
            (traitSet +: setAcc, traits ++ traitAcc)
        }

      Interpretation(
        dateLastEvaluated = rawInterpretation.tryExtract[LocalDate]("@DateLastEvaluated"),
        `type` = rawInterpretation.tryExtract[String]("@Type"),
        description = rawInterpretation.tryExtract[String]("Description", "$"),
        explanation = rawInterpretation.tryExtract[String]("Explanation", "$"),
        content = Content.encode(rawInterpretation),
        traitSets = traitSets,
        traits = traits
      )
    }

  /** Convert a raw Trait payload into our model. */
  private[parsers] def parseRawTrait(
    releaseDate: LocalDate,
    traitParser: TraitMetadata.Parser
  )(rawTrait: Msg): Trait = {
    // Extract common metadata from the trait.
    val metadata = traitParser.parse(rawTrait.extract[String]("@ID"), rawTrait)

    // Process the trait's symbols.
    // NOTE: The structure of this code is identical to how we process trait names in `TraitMetadata`,
    // but I'm not sure it's worth abstracting into its own method. If we need this pattern one more
    // time, we should make it generic.
    val allSymbols = rawTrait.tryExtract[List[Msg]]("Symbol").getOrElse(List.empty)
    val (preferredSymbol, altSymbols, symbolXrefs) =
      allSymbols.foldLeft((Option.empty[String], List.empty[String], Set.empty[Xref])) {
        case ((prefAcc, altAcc, xrefAcc), symbol) =>
          val symValue = symbol.extract[Msg]("ElementValue")
          val symType = symValue.extract[String]("@Type")
          val symString = symValue.extract[String]("$")

          if (symType == "Preferred") {
            if (prefAcc.isDefined) {
              throw new IllegalStateException(
                s"Trait ${metadata.id} has multiple preferred symbols"
              )
            } else {
              val preferredRefs = TraitMetadata.extractXrefs(symbol, Some("symbol"), None).toSet
              (Some(symString), altAcc, preferredRefs.union(xrefAcc))
            }
          } else {
            val alternateRefs =
              TraitMetadata.extractXrefs(symbol, Some("alternate_symbols"), Some(symString)).toSet
            (prefAcc, symString :: altAcc, alternateRefs.union(xrefAcc))
          }
      }

    // Process some known attributes of traits.
    val attributes = rawTrait
      .tryExtract[mutable.ArrayBuffer[Msg]]("AttributeSet")
      .getOrElse(mutable.ArrayBuffer.empty)

    def popAttribute(attrType: String): Option[Msg] = {
      val index = attributes.indexWhere(_.read[String]("Attribute", "@Type") == attrType)
      if (index == -1) None else Some(attributes.remove(index))
    }
    def popRepeatedAttribute(attrType: String): List[Msg] = {
      val indices = attributes.zipWithIndex.flatMap {
        case (attr, i) =>
          if (attr.read[String]("Attribute", "@Type") == attrType) Some(i) else None
      }.sorted.zipWithIndex.map {
        // Every time we pop from the List, we need to deprecate all following indices by 1.
        // As long as the index-list is sorted, the amount we need to deprecate by should be
        // the original position in the list.
        case (originalIndex, adjustment) => originalIndex - adjustment
      }
      indices.map(attributes.remove).toList
    }

    val (definition, defXrefs) =
      popAttribute("public definition").fold((Option.empty[String], Set.empty[Xref])) { rawDef =>
        val defValue = rawDef.read[String]("Attribute", "$")
        val defRefs = TraitMetadata.extractXrefs(rawDef, Some("public_definition"), None)
        (Some(defValue), defRefs.toSet)
      }

    val (gardId, gardXrefs) = popAttribute("GARD id").fold((Option.empty[Long], Set.empty[Xref])) {
      rawId =>
        val idValue = rawId.read[Long]("Attribute", "@integerValue")
        val idRefs = TraitMetadata.extractXrefs(rawId, Some("gard_id"), None)
        (Some(idValue), idRefs.toSet)
    }

    val (keywords, keywordRefs) =
      popRepeatedAttribute("keyword").foldLeft((Set.empty[String], Set.empty[Xref])) {
        case ((kwAcc, refAcc), keyword) =>
          val kwValue = keyword.read[String]("Attribute", "$")
          val kwRefs = TraitMetadata.extractXrefs(keyword, Some("keywords"), Some(kwValue))
          (kwAcc + kwValue, refAcc.union(kwRefs.toSet))
      }

    val (mechanism, mechanismId, mechanismRefs) = popAttribute("disease mechanism").fold(
      (Option.empty[String], Option.empty[Long], Set.empty[Xref])
    ) { mechanism =>
      val mechanismValue = mechanism.read[String]("Attribute", "$")
      val mechanismId = mechanism.tryRead[Long]("Attribute", "@integerValue")
      val mechanismRefs = TraitMetadata.extractXrefs(mechanism, Some("disease_mechanism"), None)
      (Some(mechanismValue), mechanismId, mechanismRefs.toSet)
    }

    val (inheritanceMode, inheritanceRefs) =
      popAttribute("mode of inheritance").fold((Option.empty[String], Set.empty[Xref])) { rawMode =>
        val modeValue = rawMode.read[String]("Attribute", "$")
        val modeRefs = TraitMetadata.extractXrefs(rawMode, Some("mode_of_inheritance"), None)
        (Some(modeValue), modeRefs.toSet)
      }

    val (review, reviewRefs) =
      popAttribute("GeneReviews short").fold((Option.empty[String], Set.empty[Xref])) { rawReview =>
        val reviewValue = rawReview.read[String]("Attribute", "$")
        val reviewRefs = TraitMetadata.extractXrefs(rawReview, Some("gene_reviews_short"), None)
        (Some(reviewValue), reviewRefs.toSet)
      }

    val (ghr, ghrRefs) =
      popAttribute("Genetics Home Reference (GHR) links").fold(
        (Option.empty[String], Set.empty[Xref])
      ) { rawLinks =>
        val ghrValue = rawLinks.read[String]("Attribute", "$")
        val ghrRefs = TraitMetadata.extractXrefs(rawLinks, Some("ghr_links"), None)
        (Some(ghrValue), ghrRefs.toSet)
      }

    val allXrefs = metadata.xrefs
      .union(symbolXrefs)
      .union(defXrefs)
      .union(gardXrefs)
      .union(keywordRefs)
      .union(mechanismRefs)
      .union(inheritanceRefs)
      .union(reviewRefs)
      .union(ghrRefs)

    Trait(
      id = metadata.id,
      releaseDate = releaseDate,
      medgenId = metadata.medgenId,
      `type` = metadata.`type`,
      name = metadata.name,
      alternateNames = metadata.alternateNames,
      symbol = preferredSymbol,
      alternateSymbols = altSymbols.sorted,
      publicDefinition = definition,
      gardId = gardId,
      keywords = keywords.toList.sorted,
      diseaseMechanism = mechanism,
      diseaseMechanismId = mechanismId,
      modeOfInheritance = inheritanceMode,
      geneReviewsShort = review,
      ghrLinks = ghr,
      attributeContent = attributes.flatMap(Content.encode).toList,
      xrefs = allXrefs.toList
        .sortBy(xref => (xref.refField, xref.refFieldElement, xref.db, xref.id, xref.`type`)),
      content = Content.encode(rawTrait)
    )
  }
}
