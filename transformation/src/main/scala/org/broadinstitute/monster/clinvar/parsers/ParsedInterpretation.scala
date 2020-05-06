package org.broadinstitute.monster.clinvar.parsers

import java.time.LocalDate

import org.broadinstitute.monster.clinvar.Content
import org.broadinstitute.monster.clinvar.jadeschema.struct.Xref
import org.broadinstitute.monster.clinvar.jadeschema.table.{Trait, TraitSet}
import upack.{Arr, Msg, Str}

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
case class ParsedInterpretation(
  dateLastEvaluated: Option[LocalDate],
  `type`: Option[String],
  description: Option[String],
  explanation: Option[String],
  content: Option[String],
  traitSets: Array[TraitSet],
  traits: Array[Trait]
)

object ParsedInterpretation {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Convert a raw Interpretation payload into our model. */
  def fromRawInterpretation(rawInterpretation: Msg): ParsedInterpretation = {
    // Extract trait info first, so it doesn't get bundled into unmodeled content.
    val (traitSets, traits) = rawInterpretation
      .tryExtract[Array[Msg]]("ConditionList", "TraitSet")
      .getOrElse(Array.empty)
      .foldLeft((Array.empty[TraitSet], Array.empty[Trait])) {
        case ((setAcc, traitAcc), rawTraitSet) =>
          val traits = rawTraitSet.extract[Array[Msg]]("Trait").map(parseRawTrait)
          val traitSet = TraitSet(
            id = rawTraitSet.extract[String]("@ID"),
            `type` = rawTraitSet.tryExtract[String]("@Type"),
            traitIds = traits.map(_.id).sorted,
            content = Content.encode(rawTraitSet)
          )
          (traitSet +: setAcc, traits ++ traitAcc)
      }

    ParsedInterpretation(
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
  def parseRawTrait(rawTrait: Msg): Trait = {
    // Extract common metadata from the trait.
    val metadata = TraitMetadata.fromRawTrait(rawTrait)(_.extract[String]("@ID"))

    // Process the trait's symbols.
    // NOTE: The structure of this code is identical to how we process trait names in `TraitMetadata`,
    // but I'm not sure it's worth abstracting into its own method. If we need this pattern one more
    // time, we should make it generic.
    val allSymbols = rawTrait.tryExtract[Array[Msg]]("Symbol").getOrElse(Array.empty)
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
    val definitionIndex =
      attributes.indexWhere(_.read[String]("Attribute", "@Type") == "public definition")
    val rawDefinition = if (definitionIndex == -1) {
      None
    } else {
      Some(attributes.remove(definitionIndex))
    }
    val (definition, defXrefs) = rawDefinition.fold((Option.empty[String], Set.empty[Xref])) {
      rawDef =>
        val attribute = rawDef.read[Msg]("Attribute")
        val defValue = attribute.read[String]("$")
        val defRefs = TraitMetadata.extractXrefs(attribute, Some("public_definition"), None)
        (Some(defValue), defRefs.toSet)
    }

    val gardIndex =
      attributes.indexWhere(_.read[String]("Attribute", "@Type") == "GARD id")
    val rawGardId = if (gardIndex == -1) {
      None
    } else {
      Some(attributes.remove(gardIndex))
    }
    val (gardId, gardXrefs) = rawGardId.fold((Option.empty[Long], Set.empty[Xref])) { rawId =>
      val attribute = rawId.read[Msg]("Attribute")
      val idValue = attribute.read[Long]("@integerValue")
      val idRefs = TraitMetadata.extractXrefs(attribute, Some("gard_id"), None)
      (Some(idValue), idRefs.toSet)
    }

    // Inject any unused attributes back into the unmodeled content.
    if (attributes.nonEmpty) {
      rawTrait.obj.put(Str("AttributeSet"), Arr(attributes))
    }

    val allXrefs = metadata.xrefs
      .union(symbolXrefs)
      .union(defXrefs)
      .union(gardXrefs)

    Trait(
      id = metadata.id,
      medgenId = metadata.medgenId,
      `type` = metadata.`type`,
      name = metadata.name,
      alternateNames = metadata.alternateNames,
      symbol = preferredSymbol,
      alternateSymbols = altSymbols.toArray.sorted,
      publicDefinition = definition,
      gardId = gardId,
      xrefs = allXrefs.toArray
        .sortBy(xref => (xref.db, xref.id, xref.`type`, xref.sourceField, xref.sourceDetail)),
      content = Content.encode(rawTrait)
    )
  }
}
