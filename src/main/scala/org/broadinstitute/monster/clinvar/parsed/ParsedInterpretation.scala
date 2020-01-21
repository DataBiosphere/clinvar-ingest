package org.broadinstitute.monster.clinvar.parsed

import java.time.LocalDate

import org.broadinstitute.monster.clinvar.{Constants, Content}
import org.broadinstitute.monster.clinvar.jadeschema.struct.Xref
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
    val allNames = rawTrait.extract[Array[Msg]]("Name")

    // "Unzip" name entries from their nested xrefs so we can store
    // each in a separate field.
    val (preferredName, alternateNames, nameXrefs) =
      allNames
        .foldLeft((Option.empty[String], List.empty[String], Set.empty[Xref])) {
          case ((prefAcc, altAcc, xrefAcc), name) =>
            val nameValue = name.extract[Msg]("ElementValue")
            val nameType = nameValue.extract[String]("@Type")
            val nameString = nameValue.extract[String]("$")

            val nameRefs = name
              .tryExtract[Array[Msg]]("XRef")
              .getOrElse(Array.empty)
              .map { nameRef =>
                Xref(
                  nameRef.extract[String]("@DB"),
                  nameRef.extract[String]("@ID"),
                  // Keep a link between xref and name.
                  Some(nameString)
                )
              }
              .toSet

            if (nameType == "Preferred") {
              if (prefAcc.isDefined) {
                throw new IllegalStateException(
                  s"Trait $rawTrait has multiple preferred names"
                )
              } else {
                (Some(nameString), altAcc, nameRefs.union(xrefAcc))
              }
            } else {
              (prefAcc, nameString :: altAcc, nameRefs.union(xrefAcc))
            }
        }

    // Process remaining xrefs in the payload, combining them with the
    // name-based refs.
    val topLevelRefs = rawTrait
      .tryExtract[Array[Msg]]("XRef")
      .getOrElse(Array.empty)
      .map { xref =>
        Xref(
          xref.extract[String]("@DB"),
          xref.extract[String]("@ID"),
          None
        )
      }
    val (medgenId, finalXrefs) =
      topLevelRefs.foldLeft((Option.empty[String], nameXrefs)) {
        case ((medgenAcc, xrefAcc), xref) =>
          if (xref.db.contains(Constants.MedGenKey)) {
            if (medgenAcc.isDefined) {
              throw new IllegalStateException(
                s"VCV Trait Set contains two MedGen references: $rawTrait"
              )
            } else {
              (Some(xref.id), xrefAcc)
            }
          } else {
            (medgenAcc, xrefAcc + xref)
          }
      }

    Trait(
      id = rawTrait.extract[String]("@ID"),
      medgenId = medgenId,
      `type` = rawTrait.tryExtract[String]("@Type"),
      name = preferredName,
      alternateNames = alternateNames.toArray,
      xrefs = finalXrefs.toArray,
      content = Content.encode(rawTrait)
    )
  }
}
