package org.broadinstitute.monster.clinvar.parsers

import java.time.LocalDate

import org.broadinstitute.monster.clinvar.Content
import org.broadinstitute.monster.clinvar.jadeschema.table.{Trait, TraitSet}
import upack.Msg

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
            traitIds = traits.map(_.id),
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

    Trait(
      id = metadata.id,
      medgenId = metadata.medgenId,
      `type` = metadata.`type`,
      name = metadata.name,
      alternateNames = metadata.alternateNames,
      xrefs = metadata.xrefs,
      content = Content.encode(rawTrait)
    )
  }
}
